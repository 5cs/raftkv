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

	name   string
	id     int64
	seq    int64
	mck    *shardmaster.Clerk
	config shardmaster.Config // persist state
}

type appliedResult struct {
	Key    string
	Result interface{}
}

func (kv *ShardKV) getFmtKey(args *GetArgs) string {
	return fmt.Sprintf("%v_%v_%v", args.Key, args.ClientId, args.Seq)
}

func (kv *ShardKV) putAppendFmtKey(args *PutAppendArgs) string {
	return fmt.Sprintf("%v_%v_%v", args.Key, args.ClientId, args.Seq)
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
		kv.trySnapshot(index)
		args := cmd.(PutAppendArgs)
		reply := kv.putAppend(&args, index, isLeader)
		kv.appliedCmds[index] = &appliedResult{
			Key:    kv.putAppendFmtKey(&args),
			Result: reply,
		}
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
		log.Printf("Op \"SyncShard\" at %#v, kv.configNum-arg.config:%#v-%#v, clientId-seq:%#v-%#v, get values: %#v", kv.name, kv.config.Num, args.Config.Num, args.ClientId, args.Seq, args.Data)
		reply := kv.applySyncShard(&args, index, isLeader)
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
	default:
		kv.trySnapshot(index)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) get(args *GetArgs, index int, isLeader bool) GetReply {
	if kv.config.Shards[key2shard(args.Key)] != kv.gid || kv.config.Num != args.ConfigNum {
		// if kv.config.Shards[key2shard(args.Key)] != kv.gid {
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

func (kv *ShardKV) putAppend(args *PutAppendArgs, index int, isLeader bool) PutAppendReply {
	if kv.config.Shards[key2shard(args.Key)] != kv.gid || kv.config.Num != args.ConfigNum {
		// if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		return PutAppendReply{Err: ErrWrongGroup}
	}
	if seq, ok := kv.clientSeqs[args.ClientId]; ok && seq >= args.Seq {
		log.Printf("Op %#v at %#v, kv.configNum-arg.config:%#v-%#v, key:%#v, value:%#v, client:%#v, seq:%#v, original:%#v, clientId-seq-index:%#v-%#v-%#v, %#v, filtered by %#v\n",
			args.Op, kv.name, kv.config.Num, args.ConfigNum, args.Key, args.Value, args.ClientId, args.Seq, kv.db[key2shard(args.Key)][args.Key], args.ClientId, args.Seq, index, isLeader, seq)
		return PutAppendReply{Err: OK}
	}
	log.Printf("Op %#v at %#v, kv.configNum-arg.config:%#v-%#v, key:%#v, value:%#v, client:%#v, seq:%#v, original:%#v, clientId-seq-index:%#v-%#v-%#v, %#v\n",
		args.Op, kv.name, kv.config.Num, args.ConfigNum, args.Key, args.Value, args.ClientId, args.Seq, kv.db[key2shard(args.Key)][args.Key], args.ClientId, args.Seq, index, isLeader)
	kv.clientSeqs[args.ClientId] = args.Seq
	if args.Op == "Put" {
		kv.db[key2shard(args.Key)][args.Key] = args.Value
	} else {
		kv.db[key2shard(args.Key)][args.Key] += args.Value
	}
	return PutAppendReply{Err: OK}
}

func (kv *ShardKV) applySyncShard(args *SyncShardArgs, index int, isLeader bool) SyncShardReply {
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
	kv.config = args.Config
	return SyncShardReply{Err: OK, WrongLeader: false}
}

func (kv *ShardKV) trySnapshot(index int) {
	if !(kv.maxraftstate != -1 &&
		float64(kv.persister.RaftStateSize()) >= float64(kv.maxraftstate)*0.8) {
		return
	}

	var (
		lastIncludedIndex int = 0
		lastIncludedTerm  int = 0
		config                = shardmaster.Config{}
	)
	logs := kv.rf.GetLog()
	state := kv.persister.ReadSnapshot()
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	if kv.persister.SnapshotSize() != 0 &&
		(d.Decode(&config) != nil ||
			d.Decode(&lastIncludedIndex) != nil ||
			d.Decode(&lastIncludedTerm) != nil) {
		return
	}

	for i := lastIncludedIndex + 1; i <= index; i++ {
		logEntry := logs[kv.rf.Index(i)]
		switch logEntry.Command.(type) {
		case PutAppendArgs:
			cmd := logEntry.Command.(PutAppendArgs)
			if seq, ok := kv.clientSeqs[cmd.ClientId]; ok && seq >= cmd.Seq {
				continue
			}
			kv.clientSeqs[cmd.ClientId] = cmd.Seq
			if cmd.Op == "Put" {
				kv.db[key2shard(cmd.Key)][cmd.Key] = cmd.Value
			} else if cmd.Op == "Append" {
				kv.db[key2shard(cmd.Key)][cmd.Key] += cmd.Value
			} else {
				panic("Can't happen")
			}
		default:
		}
	}

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

// RPC handle for migrating shards
func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer log.Printf("args: %#v, reply: %#v\n", args, reply)
	if _, isLeader := kv.rf.GetState(); !isLeader { // pull from group leader?
		reply.WrongLeader = true
		return
	}
	if args.ConfigNum > kv.config.Num {
		reply.Err = ErrNoKey
		return
	}

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
		go kv.tryPullShard(args, done, (leader+1)%len(servers), servers)
	} else if ok && reply.WrongLeader {
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
			leader = (leader + 1) & len(servers)
			go kv.tryPullShard(args, done, (leader+1)%len(servers), servers)
		case *reply = <-done:
			return
		}
	}
}

func (kv *ShardKV) pullAndSyncShards(config shardmaster.Config) bool {
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

	// RPC parameter for sync data shards
	mtx := sync.Mutex{}
	kv.mu.Lock()
	syncShardArgs := SyncShardArgs{
		Data:       make([]map[string]string, shardmaster.NShards),
		ClientSeqs: make(map[int64]int64),
		Config:     config, // new config
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
					ConfigNum: kv.config.Num,
					ClientId:  kv.id,
					Seq:       kv.seq,
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
	return true
}

func (kv *ShardKV) pollConfig(ms int) {
	// poll configs
	for {
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			if _, isLeader := kv.rf.GetState(); isLeader {
				latestConfig := kv.mck.Query(-1)
				for i := kv.config.Num + 1; i <= latestConfig.Num; i++ {
					ok := kv.pullAndSyncShards(kv.mck.Query(i))
					if !ok {
						break
					}
				}
			}
		}
	}
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
