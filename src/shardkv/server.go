package shardkv

import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "labgob"

import (
	"bytes"
	"fmt"
	"log"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string
	Key   string
	Value string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister         *raft.Persister
	clientSeqs        map[int64]int64
	notices           map[int]*sync.Cond
	appliedCmds       map[int]*appliedResult
	lastIncludedIndex int
	lastIncludedTerm  int
	db                [shardmaster.NShards]map[string]string

	name          string
	id            int64
	seq           int64
	mck           *shardmaster.Clerk
	config        shardmaster.Config // persist state
	pendingConfig *PendingConfig     // new config
	delta         DeltaConfig
}

type appliedResult struct {
	Key    string
	Result interface{}
}

type PendingConfig struct {
	mu                 sync.Mutex
	Config             shardmaster.Config
	ShardsNeedToAdd    map[int]bool
	ShardsNeedToDelete map[int]bool
	IsDeleted          bool // for CAS
	IsSynced           bool
}

type DeltaConfig struct {
	mu      sync.Mutex
	addDiff map[int][]int // config num -> shards need to pull
	delDiff map[int][]int // config num -> shards need to delete
}

func (kv *ShardKV) getFmtKey(args *GetArgs) string {
	return fmt.Sprintf("%v_%v_%v", args.Key, args.ClientId, args.Seq)
}

func (kv *ShardKV) putAppendFmtKey(args *PutAppendArgs) string {
	return fmt.Sprintf("%v_%v_%v", args.Key, args.ClientId, args.Seq)
}

func (kv *ShardKV) migrateShardFmtKey(args *MigrateShardArgs) string {
	return fmt.Sprintf("%v_%v_%v", args.ConfigNum, args.ClientId, args.Seq)
}

func (kv *ShardKV) syncShardFmtKey(args *SyncShardArgs) string {
	return fmt.Sprintf("%v_%v_%v", args.Config.Num, args.ClientId, args.Seq)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	cmd := *args
	for {
		*reply = GetReply{}
		index, _, isLeader := kv.rf.Start(cmd)
		if !isLeader {
			reply.WrongLeader = true
			return
		}

		kv.mu.Lock()
		if _, ok := kv.notices[index]; !ok {
			kv.notices[index] = sync.NewCond(&kv.mu)
		}
		kv.notices[index].Wait()

		ret := kv.appliedCmds[index]
		k := kv.getFmtKey(args)
		if ret.Key == k {
			switch ret.Result.(type) {
			case GetReply:
				*reply = ret.Result.(GetReply)
				kv.mu.Unlock()
				return
			default:
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	if seq, ok := kv.clientSeqs[args.ClientId]; ok && args.Seq <= seq {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	cmd := *args
	for {
		*reply = PutAppendReply{}
		index, _, isLeader := kv.rf.Start(cmd)
		if !isLeader {
			reply.WrongLeader = true
			return
		}

		kv.mu.Lock()
		if _, ok := kv.notices[index]; !ok {
			kv.notices[index] = sync.NewCond(&kv.mu)
		}
		kv.notices[index].Wait()

		ret := kv.appliedCmds[index]
		k := kv.putAppendFmtKey(args)
		if ret.Key == k {
			switch ret.Result.(type) {
			case PutAppendReply:
				*reply = ret.Result.(PutAppendReply)
				kv.mu.Unlock()
				return
			default:
			}
		}
		kv.mu.Unlock()
	}
}

// RPC handle for migrating shards
func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	defer log.Printf("Migrate me-cfgn: %#v-%#v, args: %#v, reply: %#v\n", kv.name, kv.config.Num, args, reply)

	kv.mu.Lock()
	if args.ConfigNum > kv.config.Num {
		reply.Err = ErrNoKey
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// agree on deleted shards
	cmd := *args
loop:
	for {
		*reply = MigrateShardReply{}
		index, _, isLeader := kv.rf.Start(cmd)
		if !isLeader {
			reply.WrongLeader = true
			return
		}

		kv.mu.Lock()
		if _, ok := kv.notices[index]; !ok {
			kv.notices[index] = sync.NewCond(&kv.mu)
		}
		kv.notices[index].Wait()

		ret := kv.appliedCmds[index]
		k := kv.migrateShardFmtKey(args)
		if ret.Key == k {
			switch ret.Result.(type) {
			case MigrateShardReply:
				*reply = ret.Result.(MigrateShardReply)
				kv.mu.Unlock()
				break loop
			default:
			}
		}
		kv.mu.Unlock()
	}

	kv.mu.Lock()
	reply.Err = OK
	reply.Data = make([]map[string]string, shardmaster.NShards)
	for i := 0; i < shardmaster.NShards; i++ {
		reply.Data[i] = make(map[string]string)
	}
	for _, shard := range args.Shards {
		for k, v := range kv.db[shard] { // copy data shard
			reply.Data[shard][k] = v
		}
	}

	reply.ClientSeqs = make(map[int64]int64)
	for clientId, seq := range kv.clientSeqs { // copy client seq
		reply.ClientSeqs[clientId] = seq
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) SyncShard(args *SyncShardArgs, reply *SyncShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	if seq, ok := kv.clientSeqs[args.ClientId]; ok && args.Seq <= seq {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	cmd := *args
	for {
		*reply = SyncShardReply{}
		index, _, isLeader := kv.rf.Start(cmd)
		if !isLeader {
			reply.WrongLeader = true
			return
		}

		kv.mu.Lock()
		if _, ok := kv.notices[index]; !ok {
			kv.notices[index] = sync.NewCond(&kv.mu)
		}
		kv.notices[index].Wait()

		ret := kv.appliedCmds[index]
		k := kv.syncShardFmtKey(args)
		if ret.Key == k {
			switch ret.Result.(type) {
			case SyncShardReply:
				*reply = ret.Result.(SyncShardReply)
				kv.mu.Unlock()
				return
			default:
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) Name() string {
	return kv.name
}

// Apply method called by Raft instance
func (kv *ShardKV) Apply(applyMsg interface{}) {
	kv.mu.Lock()
	msg := applyMsg.(*raft.ApplyMsg)
	index := msg.CommandIndex
	cmd := msg.Command
	isLeader := msg.IsLeader
	switch cmd.(type) {
	case GetArgs:
		kv.trySnapshot(index)
		if !isLeader {
			kv.mu.Unlock()
			break
		}
		args := cmd.(GetArgs)
		reply := kv.get(&args, index, isLeader)
		kv.appliedCmds[index] = &appliedResult{
			Key:    kv.getFmtKey(&args),
			Result: reply,
		}
		if _, ok := kv.notices[index]; ok {
			kv.mu.Unlock()
			kv.notices[index].Broadcast()
		} else {
			kv.mu.Unlock()
		}
	case PutAppendArgs:
		args := cmd.(PutAppendArgs)
		reply := kv.putAppend(&args, index, isLeader)
		kv.appliedCmds[index] = &appliedResult{
			Key:    kv.putAppendFmtKey(&args),
			Result: reply,
		}
		kv.trySnapshot(index)
		if _, ok := kv.notices[index]; ok {
			kv.mu.Unlock()
			kv.notices[index].Broadcast()
		} else {
			kv.mu.Unlock()
		}
	case *raft.InstallSnapshotArgs: // install snapshot
		args := cmd.(*raft.InstallSnapshotArgs)
		kv.readSnapshot(kv.persister.ReadSnapshot())
		log.Printf("Op \"InstallSnapshot\" at %#v, get values: %#v, reqId: %#v, leaderId-term-index:%#v-%#v-%#v\n",
			kv.name, kv.db, args.ReqId, args.LeaderId, args.LastIncludedTerm, args.LastIncludedIndex)
		kv.mu.Unlock()
	case SyncShardArgs:
		args := cmd.(SyncShardArgs)
		log.Printf("Op \"SyncShard\" at %#v, kv.configNum-arg.config:%#v-%#v, clientId-seq:%#v-%#v, get values: %#v",
			kv.name, kv.config.Num, args.Config.Num, args.ClientId, args.Seq, args.Data)
		reply := kv.syncShard(&args, index, isLeader)
		kv.appliedCmds[index] = &appliedResult{
			Key:    kv.syncShardFmtKey(&args),
			Result: reply,
		}
		if _, ok := kv.notices[index]; ok {
			kv.mu.Unlock()
			kv.notices[index].Broadcast()
		} else {
			kv.mu.Unlock()
		}
	case MigrateShardArgs:
		args := cmd.(MigrateShardArgs)
		reply := kv.migrateShard(&args, index, isLeader)
		kv.appliedCmds[index] = &appliedResult{
			Key:    kv.migrateShardFmtKey(&args),
			Result: reply,
		}
		if _, ok := kv.notices[index]; ok {
			kv.mu.Unlock()
			kv.notices[index].Broadcast()
		} else {
			kv.mu.Unlock()
		}
	default:
		kv.trySnapshot(index)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) get(args *GetArgs, index int, isLeader bool) GetReply {
	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		return GetReply{Value: "", Err: ErrWrongGroup}
	}
	if value, ok := kv.db[key2shard(args.Key)][args.Key]; !ok {
		return GetReply{Value: "", Err: ErrNoKey}
	} else {
		log.Printf("Op Get at %#v, kv.configNum-arg.config:%#v-%#v, key:%#v value:%#v, clientId-seq-index:%#v-%#v-%#v, %#v\n",
			kv.name, kv.config.Num, args.ConfigNum, args.Key, value, args.ClientId, args.Seq, index, isLeader)
		return GetReply{Value: value, Err: OK}
	}
}

// log.Printf("Op %#v at %#v, kv.configNum-arg.config:%#v-%#v, key:%#v, value:%#v, client:%#v, seq:%#v, original:%#v, clientId-seq-index:%#v-%#v-%#v, %#v, gid-kv.gid:%#v-%#v\n",
// 			args.Op, kv.name, kv.config.Num, args.ConfigNum, args.Key, args.Value, args.ClientId, args.Seq, kv.db[key2shard(args.Key)][args.Key], args.ClientId, args.Seq, index, isLeader, kv.config.Shards[key2shard(args.Key)], kv.gid)

// 		log.Printf("Op %#v at %#v, kv.configNum-arg.config:%#v-%#v, key:%#v, value:%#v, client:%#v, seq:%#v, original:%#v, clientId-seq-index:%#v-%#v-%#v, %#v, filtered by %#v\n",
// 			args.Op, kv.name, kv.config.Num, args.ConfigNum, args.Key, args.Value, args.ClientId, args.Seq, kv.db[key2shard(args.Key)][args.Key], args.ClientId, args.Seq, index, isLeader, seq)

// 	log.Printf("Op %#v at %#v, kv.configNum-arg.config:%#v-%#v, key:%#v, value:%#v, client:%#v, seq:%#v, original:%#v, clientId-seq-index:%#v-%#v-%#v, %#v\n",
// 		args.Op, kv.name, kv.config.Num, args.ConfigNum, args.Key, args.Value, args.ClientId, args.Seq, kv.db[key2shard(args.Key)][args.Key], args.ClientId, args.Seq, index, isLeader)

func (kv *ShardKV) putAppend(args *PutAppendArgs, index int, isLeader bool) PutAppendReply {
	// if kv.config.Num < args.ConfigNum || kv.config.Shards[key2shard(args.Key)] != kv.gid {
	// }
	if !(kv.config.Num == args.ConfigNum && kv.config.Shards[key2shard(args.Key)] == kv.gid) {
		// case 1
		return PutAppendReply{Err: ErrWrongGroup}
	}
	// key may has been pulled, when kv.config and args.config are both old config
	kv.delta.mu.Lock()
	if diff, ok := kv.delta.delDiff[args.ConfigNum]; ok {
		for _, shard := range diff {
			if shard == key2shard(args.Key) {
				panic("Pulled!")
			}
		}
	}
	kv.delta.mu.Unlock()
	if seq, ok := kv.clientSeqs[args.ClientId]; ok && seq >= args.Seq {
		return PutAppendReply{Err: OK}
	}
	kv.clientSeqs[args.ClientId] = args.Seq
	if args.Op == "Put" {
		kv.db[key2shard(args.Key)][args.Key] = args.Value
	} else {
		kv.db[key2shard(args.Key)][args.Key] += args.Value
	}
	return PutAppendReply{Err: OK}
}

func (kv *ShardKV) syncShard(args *SyncShardArgs, index int, isLeader bool) SyncShardReply {
	if args.Config.Num <= kv.config.Num {
		return SyncShardReply{Err: OK, WrongLeader: false}
	}
	for shard, data := range args.Data {
		for k, v := range data {
			kv.db[shard][k] = v
		}
	}
	for clientId, seq := range args.ClientSeqs {
		if seq1, ok := kv.clientSeqs[clientId]; !ok || seq1 < seq {
			kv.clientSeqs[clientId] = seq
		}
	}
	kv.clientSeqs[args.ClientId] = args.Seq

	// check pending config
	// if kv.pendingConfig != nil {
	// 	kv.pendingConfig.mu.Lock()
	// 	for shard := range kv.pendingConfig.ShardsNeedToAdd {
	// 		kv.pendingConfig.ShardsNeedToAdd[shard] = false
	// 	}
	// 	kv.pendingConfig.IsSynced = true
	// 	kv.pendingConfig.mu.Unlock()
	// }

	// if kv.pendingConfig != nil {
	// 	kv.pendingConfig.mu.Lock()
	// 	if !isLeader {
	// 		panic("needs resync shards")
	// 	}
	// 	if args.Config.Num != kv.pendingConfig.Config.Num {
	// 		panic("wrong config order!")
	// 	}
	// 	if kv.pendingConfig.IsSynced && kv.pendingConfig.IsDeleted {
	// 		kv.config = kv.pendingConfig.Config
	// 		// TODO: sync config with other nodes
	// 	}
	// 	kv.pendingConfig.mu.Unlock()
	// }
	if args.ConfigNum != kv.config.Num { // check arg
		panic("HEHE")
	}
	kv.delta.mu.Lock()
	if _, ok := kv.delta.addDiff[args.ConfigNum]; !ok {
		kv.delta.addDiff[args.ConfigNum] = []int{}
	}
loop:
	for _, shard := range args.Shards {
		for _, shard1 := range kv.delta.addDiff[args.ConfigNum] {
			if shard == shard1 {
				continue loop
			}
		}
		kv.delta.addDiff[args.ConfigNum] = append(kv.delta.addDiff[args.ConfigNum], shard)
	}
	if add, ok0 := kv.delta.addDiff[args.ConfigNum]; ok0 {
		if del, ok1 := kv.delta.delDiff[args.ConfigNum]; ok1 {
			log.Printf("Node:%#v, config:%#v->%#v, add:%#v, del:%#v\n", kv.Name(), args.ConfigNum, args.Config.Num, add, del)
			// kv.config = args.Config
		}
	}
	kv.delta.mu.Unlock()

	// kv.config = args.Config
	return SyncShardReply{Err: OK, WrongLeader: false}
}

func (kv *ShardKV) migrateShard(args *MigrateShardArgs, index int, isLeader bool) MigrateShardReply {
	// TODO: persist deleted shards state
	kv.delta.mu.Lock()
	if _, ok := kv.delta.delDiff[args.ConfigNum]; !ok {
		kv.delta.delDiff[args.ConfigNum] = []int{}
	}
loop:
	for _, shard := range args.Shards {
		for _, shard1 := range kv.delta.delDiff[args.ConfigNum] {
			if shard == shard1 {
				continue loop
			}
		}
		kv.delta.delDiff[args.ConfigNum] = append(kv.delta.delDiff[args.ConfigNum], shard)
	}
	kv.delta.mu.Unlock()

	return MigrateShardReply{}
}

func (kv *ShardKV) trySnapshot(index int) {
	if !(kv.maxraftstate != -1 &&
		float64(kv.persister.RaftStateSize()) >= float64(kv.maxraftstate)*0.8) {
		return
	}

	logs := kv.rf.GetLog()
	kv.lastIncludedIndex = index
	kv.lastIncludedTerm = logs[kv.rf.Index(index)].Term
	kv.persist()
	return
}

func (kv *ShardKV) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.config)
	e.Encode(kv.lastIncludedIndex)
	e.Encode(kv.lastIncludedTerm)
	e.Encode(kv.clientSeqs)
	e.Encode(kv.db)
	data := w.Bytes()
	kv.persister.SaveSnapshot(data)
	kv.rf.TruncateLog(kv.lastIncludedIndex)
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	var (
		lastIncludedIndex int = 0
		lastIncludedTerm  int = 0
		config                = shardmaster.Config{}
	)
	kv.db = [shardmaster.NShards]map[string]string{}
	for shard := 0; shard < shardmaster.NShards; shard++ {
		kv.db[shard] = make(map[string]string)
	}
	kv.clientSeqs = make(map[int64]int64)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&config)
	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)
	d.Decode(&kv.clientSeqs)
	d.Decode(&kv.db)
	kv.config = config
	kv.lastIncludedIndex = lastIncludedIndex
	kv.lastIncludedTerm = lastIncludedTerm
}

func (kv *ShardKV) sendMigrateShard(server string, args *MigrateShardArgs, reply *MigrateShardReply) bool {
	srv := kv.make_end(server)
	ok := srv.Call("ShardKV.MigrateShard", args, reply)
	return ok
}

func (kv *ShardKV) tryPullShard(args *MigrateShardArgs, done chan MigrateShardReply, leader int, servers []string) {
	server := servers[leader]
	reply := MigrateShardReply{}
	ok := kv.sendMigrateShard(server, args, &reply)
	if ok && reply.Err == OK {
		done <- reply
	} else if ok && reply.Err == ErrNoKey {
		time.Sleep(time.Duration(100) * time.Millisecond)
		go kv.tryPullShard(args, done, leader, servers)
	} else if ok && reply.WrongLeader {
		if leader == len(servers)-1 { // No leader at all, wait 500ms
			time.Sleep(time.Duration(500) * time.Millisecond)
		}
		go kv.tryPullShard(args, done, (leader+1)%len(servers), servers)
	}
}

func (kv *ShardKV) pullShard(args *MigrateShardArgs, reply *MigrateShardReply, gid int) {
	servers := kv.config.Groups[gid]
	leader, done := 0, make(chan MigrateShardReply)
	go kv.tryPullShard(args, done, leader, servers)
	for {
		select {
		case <-time.After(time.Duration(100) * time.Millisecond):
			leader = (leader + 1) % len(servers)
			go kv.tryPullShard(args, done, leader, servers)
		case *reply = <-done:
			return
		}
	}
}

func (kv *ShardKV) calcDiff(config shardmaster.Config) (map[int]bool, map[int]bool) {
	var (
		shardsOfOriginConfig = map[int]bool{}
		shardsOfNewConfig    = map[int]bool{}
		shardsOfIntersection = map[int]bool{}
		shardsNeedToAdd      = map[int]bool{}
		shardsNeedToDelete   = map[int]bool{}
	)
	for i := 0; i < shardmaster.NShards; i++ {
		if kv.config.Shards[i] == kv.gid {
			shardsOfOriginConfig[i] = true
		}
		if config.Shards[i] == kv.gid {
			shardsOfNewConfig[i] = true
		}
		if kv.config.Shards[i] == kv.gid && config.Shards[i] == kv.gid {
			shardsOfIntersection[i] = true
		}
	}
	for shard, _ := range shardsOfNewConfig {
		if _, ok := shardsOfIntersection[shard]; !ok {
			shardsNeedToAdd[shard] = true
		}
	}
	for shard, _ := range shardsOfOriginConfig {
		if _, ok := shardsOfIntersection[shard]; !ok {
			shardsNeedToDelete[shard] = true
		}
	}
	return shardsNeedToAdd, shardsNeedToDelete
}

func (kv *ShardKV) pullAndSyncShards(config shardmaster.Config) bool {
	kv.mu.Lock()
	shardsNeedToAdd, shardsNeedToDelete := kv.calcDiff(config)
	// create pending config
	kv.pendingConfig = &PendingConfig{
		mu:                 sync.Mutex{},
		Config:             config,
		ShardsNeedToAdd:    shardsNeedToAdd,
		ShardsNeedToDelete: shardsNeedToDelete,
		IsDeleted:          false,
		IsSynced:           false,
	}

	// RPC parameter for sync data shards
	mtx := sync.Mutex{}
	syncShardArgs := SyncShardArgs{
		Data:       make([]map[string]string, shardmaster.NShards),
		ClientSeqs: make(map[int64]int64),
		Shards:     make([]int, 0),
		ConfigNum:  kv.config.Num, // prev config num
		Config:     config,        // new config
		ClientId:   kv.id,
		Seq:        kv.seq,
	}
	kv.seq += 1
	kv.mu.Unlock()
	for shard := 0; shard < shardmaster.NShards; shard++ {
		syncShardArgs.Data[shard] = make(map[string]string)
	}

	// pull shards from other group
	gidToShards := map[int][]int{}
	for shard, _ := range shardsNeedToAdd {
		gid := kv.config.Shards[shard]
		if _, ok := gidToShards[gid]; !ok {
			gidToShards[gid] = []int{shard}
		} else {
			gidToShards[gid] = append(gidToShards[gid], shard)
		}
	}
	log.Printf("At %#v, Old config: %#v, new config: %#v, gidToShards: %#v\n", kv.name, kv.config, config, gidToShards)
	if kv.config.Num != 0 {
		var wg sync.WaitGroup
		for k := range gidToShards {
			gid, shards := k, gidToShards[k] // local variable for goroutine
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				kv.mu.Lock()
				args := MigrateShardArgs{
					Shards:    shards,
					ConfigNum: kv.config.Num, // old config
					ClientId:  kv.id,
					Seq:       kv.seq,
					Name:      kv.name,
				}
				kv.seq += 1
				kv.mu.Unlock()
				reply := MigrateShardReply{}
				kv.pullShard(&args, &reply, gid)
				mtx.Lock()
				defer mtx.Unlock()
				for shard, kv := range reply.Data {
					for k, v := range kv {
						syncShardArgs.Data[shard][k] = v
					}
					syncShardArgs.Shards = append(syncShardArgs.Shards, shard)
				}
				for clientId, seq := range reply.ClientSeqs {
					if seq1, ok := syncShardArgs.ClientSeqs[clientId]; !ok || seq1 < seq {
						syncShardArgs.ClientSeqs[clientId] = seq
					}
				}
			}(&wg)
		}
		wg.Wait()
	}

	// sync shards to group members
	log.Printf("At %#v, pulled shards: %#v\n", kv.name, syncShardArgs)
	syncShardReply := SyncShardReply{}
	kv.SyncShard(&syncShardArgs, &syncShardReply)
	if syncShardReply.WrongLeader {
		return false
	}
	return true
}

func (kv *ShardKV) pollConfig(ms int) {
	// poll configs
	for {
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			latestConfig := kv.mck.Query(-1)
			for i := kv.config.Num + 1; i < latestConfig.Num; i++ {
				newConfig := kv.mck.Query(i)
				if _, isLeader := kv.rf.GetState(); isLeader {
					ok := kv.pullAndSyncShards(newConfig)
					if !ok {
						break
					}
				}
				kv.checkAndInstallNewConfig(newConfig)
			}
			// if _, isLeader := kv.rf.GetState(); isLeader {
			// 	latestConfig := kv.mck.Query(-1)
			// 	for i := kv.config.Num + 1; i <= latestConfig.Num; i++ {
			// 		ok := kv.pullAndSyncShards(kv.mck.Query(i))
			// 		if !ok {
			// 			break
			// 		}
			// 	}
			// }
		}
	}
}

func (kv *ShardKV) checkAndInstallNewConfig(config shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardsNeedToAdd, shardsNeedToDelete := kv.calcDiff(config)
	mustCnt, hasCnt := 0, 0
	for shard, _ := range shardsNeedToAdd {
		mustCnt += 1
		for _, shard1 := range kv.delta.addDiff[kv.config.Num] {
			if shard == shard1 {
				hasCnt += 1
				break
			}
		}
	}
	if mustCnt != hasCnt {
		return
	}

	mustCnt, hasCnt = 0, 0
	for shard, _ := range shardsNeedToDelete {
		mustCnt += 1
		for _, shard1 := range kv.delta.delDiff[kv.config.Num] {
			if shard == shard1 {
				hasCnt += 1
				break
			}
		}
	}
	if mustCnt != hasCnt {
		return
	}

	kv.config = config // install new config
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister,
	maxraftstate int, gid int, masters []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(MigrateShardArgs{})
	labgob.Register(MigrateShardReply{})
	labgob.Register(SyncShardArgs{})
	labgob.Register(SyncShardReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.name = fmt.Sprintf("%v-%v", kv.gid, kv.me)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, kv.me, persister, kv.applyCh)

	kv.persister = persister
	kv.rf.SetApp(kv)
	kv.clientSeqs = make(map[int64]int64)
	kv.notices = make(map[int]*sync.Cond)
	kv.appliedCmds = make(map[int]*appliedResult)
	kv.db = [shardmaster.NShards]map[string]string{}
	for i := 0; i < shardmaster.NShards; i++ {
		kv.db[i] = make(map[string]string)
	}
	kv.delta = DeltaConfig{
		mu:      sync.Mutex{},
		addDiff: map[int][]int{},
		delDiff: map[int][]int{},
	}
	kv.readSnapshot(kv.persister.ReadSnapshot())
	log.Printf("Op \"InstallSnapshot\" at %#v, get values: %#v, reqId: %#v\n", kv.name, kv.db, -1)

	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.id = nrand()
	kv.seq = 0

	go kv.pollConfig(100)

	go func() {
		for applyMsg := range kv.applyCh {
			if false {
				DPrintf("%#v\n", applyMsg)
			}
		}
	}()

	return kv
}
