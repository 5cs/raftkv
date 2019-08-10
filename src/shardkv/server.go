package shardkv

import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "labgob"
import "fmt"
import "log"
import "time"
import "bytes"
import "sync/atomic"
import "sort"

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
	mck               *shardmaster.Clerk
	config            *shardmaster.Config
	persister         *raft.Persister
	notices           map[int]*sync.Cond
	appliedCmds       map[int]*appliedResult
	lastIncludedIndex int
	lastIncludedTerm  int
	id                int64
	seq               int64
	ids               [shardmaster.NShards]int64
	seqs              [shardmaster.NShards]int64
	addedShards       map[int]map[int]*int32
	configScheduler   chan *shardmaster.Config
	reconfiging       int32
	configs           map[int]*configStruct
	seenMaxConfigNum  int
}

type configStruct struct {
	config *shardmaster.Config
	done   bool
}
type appliedResult struct {
	Key    string
	Result interface{}
}

type PushShardArgs struct {
	Data          map[string]string
	Logs          []raft.LogEntry
	MaxClientSeqs map[int64]int64
	Shard         int
	ConfigNum     int
	ClientId      int64
	Seq           int64
}
type PushShardReply struct {
	Err         Err
	WrongLeader bool
	ConfigNum   int
}

func (kv *ShardKV) PushShard(args *PushShardArgs, reply *PushShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	for args.ConfigNum > kv.config.Num {
		time.Sleep(100 * time.Millisecond)
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	if kv.isWrongGroup(args.Shard) {
		reply.Err = ErrWrongGroup
		return
	}

	cmd := *args
	for {
		*reply = PushShardReply{}
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
		key := fmt.Sprintf("%v:%v_%v_%v",
			args.ConfigNum, args.Shard, args.ClientId, args.Seq)
		if ret.Key == key {
			switch ret.Result.(type) {
			case PushShardReply:
				*reply = ret.Result.(PushShardReply)
				kv.mu.Unlock()
				return
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) collectData(shards []int, index int) (
	map[int]map[string]string,
	map[int][]raft.LogEntry,
	map[int64]int64) {

	shardDataMap := map[int]map[string]string{}
	shardLogMap := map[int][]raft.LogEntry{}
	for _, shard := range shards {
		shardDataMap[shard] = map[string]string{}
	}
	// get values from snapshot
	var (
		lastIncludedIndex int = 0
		lastIncludedTerm  int = 0
		seqs              []*ClientSeq
		kvs               []*KeyValue
	)
	maxClientSeqs := map[int64]int64{}
	state := kv.persister.ReadSnapshot()
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	if kv.persister.SnapshotSize() != 0 &&
		(d.Decode(&lastIncludedIndex) != nil ||
			d.Decode(&lastIncludedTerm) != nil ||
			d.Decode(&seqs) != nil ||
			d.Decode(&kvs) != nil) {
		// return "", err	// TODO: fixme
	}
	for _, kv := range kvs {
		shard := key2shard(kv.Key)
		if _, ok := shardDataMap[shard]; ok {
			shardDataMap[shard][kv.Key] = kv.Value
		}
	}
	for i := range seqs {
		maxClientSeqs[seqs[i].ClientId] = seqs[i].Seq
	}

	// valid log entries
	logs := kv.rf.GetLog()
	for i := lastIncludedIndex + 1; i <= index; i++ {
		logEntry := logs[kv.rf.Index(i)]
		switch logEntry.Command.(type) {
		case PutAppendArgs:
			cmd := logEntry.Command.(PutAppendArgs)
			shardLogMap[key2shard(cmd.Key)] = append(shardLogMap[key2shard(cmd.Key)],
				logEntry)
		case PushShardArgs:
			cmd := logEntry.Command.(PushShardArgs)
			shardLogMap[cmd.Shard] = append(shardLogMap[cmd.Shard], logEntry)
		default:
		}
	}

	return shardDataMap, shardLogMap, maxClientSeqs
}
func (kv *ShardKV) pushShard(args *PushShardArgs, targets []string) *PushShardReply {
	leader, done := 0, make(chan PushShardReply)
	go kv.tryPushShard(done, args, leader, targets)
	for {
		select {
		case <-time.After(100 * time.Millisecond):
			leader = (leader + 1) % len(targets)
			go kv.tryPushShard(done, args, leader, targets)
		case reply := <-done:
			return &reply
		}
	}
}
func (kv *ShardKV) tryPushShard(
	done chan PushShardReply,
	args *PushShardArgs,
	leader int, targets []string) {

	reply := &PushShardReply{}
	srv := kv.make_end(targets[leader])
	ok := srv.Call("ShardKV.PushShard", args, reply)
	if ok && reply.Err == OK {
		done <- *reply
	} else if ok && (reply.ConfigNum > args.ConfigNum || reply.Err == ErrWrongGroup) { // outdated config
		done <- *reply
	} else if ok && reply.WrongLeader {
		kv.tryPushShard(done, args, (leader+1)%len(targets), targets)
	}
}

//
// special command indicates push shard begin
type PushShardsMarkArgs struct {
	ClientId  int64
	Seq       int64
	ConfigNum int
}
type PushShardsMarkReply struct{}

// call this func with kv.mu hold
func (kv *ShardKV) agreeOnPushShards(stage bool) int {
	kv.mu.Unlock()
	defer kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return -1
	}
	kv.mu.Lock()
	cmd := PushShardsMarkArgs{
		ClientId: kv.id,
		Seq:      kv.seq,
	}
	if stage {
		cmd.ConfigNum = kv.config.Num
	}
	kv.seq++
	kv.mu.Unlock()
	for {
		index, _, isLeader := kv.rf.Start(cmd)
		if !isLeader {
			return -1
		}

		kv.mu.Lock()
		if _, ok := kv.notices[index]; !ok {
			kv.notices[index] = sync.NewCond(&kv.mu)
		}
		kv.notices[index].Wait()
		ret := kv.appliedCmds[index]
		key := fmt.Sprintf("%v_%v_%v", "pushShardsMark", cmd.ClientId, cmd.Seq)
		if ret.Key == key {
			switch ret.Result.(type) {
			case PushShardsMarkReply:
				kv.mu.Unlock()
				return index
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) printShardStats(isPush, isBefore bool, m map[int]*int32) {
	if len(m) == 0 {
		return
	}
	if term, isLeader := kv.rf.GetStateNoLock(); isLeader {
		shards := []int{}
		for k, _ := range m {
			shards = append(shards, k)
		}
		sort.Ints(shards)
		str := fmt.Sprintf("term %#v configNum(%v):[", term, kv.config.Num)
		for _, shard := range shards {
			str += fmt.Sprintf("%v:%v,", shard, atomic.LoadInt32(m[shard]))
		}
		str += "]"
		fmt := "ServerShard %#v "
		if isBefore {
			fmt += "before "
		} else {
			fmt += "after "
		}
		if isPush {
			fmt += "push:%#v\n"
		} else {
			fmt += "pull:%#v\n"
		}
		DPrintf(fmt, kv.Name(), str)
	}
}

func (kv *ShardKV) migrateShards(one *shardmaster.Config) {
	// `config` changed from kv.config to one
	var (
		oldShards     = map[int]bool{}
		newShards     = map[int]bool{}
		intersection  = map[int]bool{}
		deletedShards = map[int]*int32{}
		addedShards   = map[int]*int32{}
	)
	for i := 0; i < len(kv.config.Shards); i++ {
		if kv.config.Shards[i] == kv.gid {
			oldShards[i] = true
		}
		if one.Shards[i] == kv.gid {
			newShards[i] = true
		}
		if kv.config.Shards[i] == kv.gid && one.Shards[i] == kv.gid {
			intersection[i] = true
		}
	}
	for shard, _ := range oldShards {
		if _, ok := intersection[shard]; !ok {
			val := int32(1)
			deletedShards[shard] = &val
		}
	}
	for shard, _ := range newShards {
		if _, ok := intersection[shard]; !ok {
			val := int32(1)
			addedShards[shard] = &val
		}
	}

	kv.config = one // new config
	kv.addedShards[one.Num] = addedShards

	index := -1
	shards := []int{}
	for k, _ := range deletedShards {
		shards = append(shards, k)
	}

	for !kv.configs[one.Num].done && len(deletedShards) > 0 && kv.config.Num > kv.seenMaxConfigNum {
		// Push shards to other groups
		if _, isLeader := kv.rf.GetStateNoLock(); isLeader {
			index = kv.agreeOnPushShards(false)
			if index < 0 {
				continue
			}
			kv.printShardStats(true, true, deletedShards)
			kv.trySnapshot(index)

			deletedShardDataMap, deletedShardLogMap, maxClientSeqs := kv.collectData(shards, index)
			ch, ndone := make(chan bool), 0
			for shard, _ := range deletedShards {
				go func(shard int) {
					defer func() { ch <- true }()
					kv.seqs[shard] += 1
					args := &PushShardArgs{}
					args.Data = deletedShardDataMap[shard]
					args.Logs = deletedShardLogMap[shard]
					args.MaxClientSeqs = maxClientSeqs
					args.Shard = shard
					args.ConfigNum = kv.config.Num
					args.ClientId = kv.ids[shard]
					args.Seq = kv.seqs[shard]
					reply := kv.pushShard(args, one.Groups[one.Shards[shard]])
					if reply.ConfigNum > args.ConfigNum {
					}
					if reply.Err == OK {
						atomic.StoreInt32(deletedShards[shard], 0)
					}
				}(shard)
			}
		loop:
			for {
				if ndone >= len(deletedShards) {
					kv.configs[one.Num].done = true
					kv.printShardStats(true, false, deletedShards)
					index := kv.agreeOnPushShards(true)
					if index < 0 {
						// no longer leader
					}
					break
				}
				select {
				case <-ch:
					ndone += 1
				case <-time.After(100 * time.Millisecond):
					if _, isLeader := kv.rf.GetStateNoLock(); !isLeader {
						break loop
					}
					if kv.config.Num <= kv.seenMaxConfigNum {
						break loop
					}
				}
			}
		} else {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			kv.mu.Lock()
		}
	}

	if len(addedShards) > 0 {
		kv.mu.Unlock()
		kv.printShardStats(false, true, addedShards)
		for {
			aLen := int32(0)
			for _, vp := range addedShards {
				aLen += atomic.LoadInt32(vp)
			}
			if aLen == 0 || kv.seenMaxConfigNum >= kv.config.Num {
				break
			}
			time.Sleep(50 * time.Millisecond)
			kv.printShardStats(false, false, addedShards)
		}
		kv.printShardStats(false, false, addedShards)
		kv.mu.Lock()
	}
}

func (kv *ShardKV) isWrongGroup(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.gid != kv.config.Shards[shard]
}

func (kv *ShardKV) scheduleConfig() {
	for one := range kv.configScheduler {
		if one.Num <= kv.config.Num {
			continue
		}
		for atomic.LoadInt32(&kv.reconfiging) == 1 {
			time.Sleep(100 * time.Millisecond)
		}
		kv.mu.Lock()
		atomic.StoreInt32(&kv.reconfiging, 1)
		kv.migrateShards(one)
		atomic.StoreInt32(&kv.reconfiging, 0)
		kv.mu.Unlock()
	}
}
func (kv *ShardKV) periodicallyPoll(ms int) {
	for {
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			kv.poll()
		}
	}
}
func (kv *ShardKV) poll() {
	kv.mu.Lock()
	one := kv.mck.Query(kv.config.Num + 1)
	if kv.config != nil && kv.config.Num != 0 && one.Num == kv.config.Num+1 {
		if _, ok := kv.configs[one.Num]; ok {
			kv.mu.Unlock()
		} else {
			kv.configs[one.Num] = &configStruct{
				config: &one,
				done:   false,
			}
			kv.mu.Unlock()
			kv.configScheduler <- &one
		}
	} else if one.Num == kv.config.Num+1 {
		if _, ok := kv.configs[one.Num]; !ok {
			kv.configs[one.Num] = &configStruct{
				config: &one,
				done:   false,
			}
			kv.config = &one
		}
		kv.mu.Unlock()
	} else {
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) addedShardsStr() string {
	return ""
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	defer func() {
		if e := recover(); e != nil {
			reply.WrongLeader = true
			return
		}
	}()

	if args.ConfigNum < kv.config.Num {
		reply.Err = ErrWrongGroup
		return
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	for args.ConfigNum > kv.config.Num {
		time.Sleep(100 * time.Millisecond)
		if args.ConfigNum < kv.config.Num {
			reply.Err = ErrWrongGroup
			return
		}
		if _, isLeader := kv.rf.GetState(); !isLeader {
			reply.WrongLeader = true
			return
		}
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	if kv.isWrongGroup(args.Shard) {
		reply.Err = ErrWrongGroup
		return
	}
	if _, ok := kv.addedShards[args.ConfigNum]; ok {
		if _, ok := kv.addedShards[args.ConfigNum][args.Shard]; ok &&
			atomic.LoadInt32(kv.addedShards[args.ConfigNum][args.Shard]) > 0 {
			for atomic.LoadInt32(kv.addedShards[args.ConfigNum][args.Shard]) > 0 {
				time.Sleep(100 * time.Millisecond)
				if kv.config.Num != args.ConfigNum {
					reply.Err = ErrWrongGroup
					return
				}
				if _, isLeader := kv.rf.GetState(); !isLeader {
					reply.WrongLeader = true
					return
				}
			}
		}
	}

	cmd := *args
	for {
		*reply = GetReply{}
		index, _, isLeader := kv.rf.Start(cmd)
		if !isLeader {
			reply.WrongLeader = true
			return
		}
		if kv.isWrongGroup(args.Shard) {
			reply.Err = ErrWrongGroup
			return
		}

		kv.mu.Lock()
		if _, ok := kv.notices[index]; !ok {
			kv.notices[index] = sync.NewCond(&kv.mu)
		}
		kv.notices[index].Wait()

		ret := kv.appliedCmds[index]
		key := fmt.Sprintf("%v_%v_%v", args.Key, args.ClientId, args.Seq)
		if ret.Key == key {
			switch ret.Result.(type) {
			case GetReply:
				*reply = ret.Result.(GetReply)
				kv.mu.Unlock()
				if kv.isWrongGroup(args.Shard) {
					reply.Err = ErrWrongGroup
				}
				return
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	defer func() {
		if e := recover(); e != nil {
			reply.WrongLeader = true
			return
		}
	}()
	if args.ConfigNum < kv.config.Num {
		reply.Err = ErrWrongGroup
		return
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	for args.ConfigNum > kv.config.Num {
		time.Sleep(100 * time.Millisecond)
		if args.ConfigNum < kv.config.Num {
			reply.Err = ErrWrongGroup
			return
		}
		if _, isLeader := kv.rf.GetState(); !isLeader {
			reply.WrongLeader = true
			return
		}
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	if kv.isWrongGroup(args.Shard) {
		reply.Err = ErrWrongGroup
		return
	}
	if _, ok := kv.addedShards[args.ConfigNum]; ok {
		if _, ok := kv.addedShards[args.ConfigNum][args.Shard]; ok &&
			atomic.LoadInt32(kv.addedShards[args.ConfigNum][args.Shard]) > 0 {
			for atomic.LoadInt32(kv.addedShards[args.ConfigNum][args.Shard]) > 0 {
				time.Sleep(100 * time.Millisecond)
				if kv.config.Num != args.ConfigNum {
					reply.Err = ErrWrongGroup
					return
				}
				if _, isLeader := kv.rf.GetState(); !isLeader {
					reply.WrongLeader = true
					return
				}
			}
		}
	}

	cmd := *args
	for {
		*reply = PutAppendReply{}
		index, _, isLeader := kv.rf.Start(cmd)
		if !isLeader {
			reply.WrongLeader = true
			return
		}
		if kv.isWrongGroup(args.Shard) {
			reply.Err = ErrWrongGroup
			return
		}

		kv.mu.Lock()
		if _, ok := kv.notices[index]; !ok {
			kv.notices[index] = sync.NewCond(&kv.mu)
		}
		kv.notices[index].Wait()

		ret := kv.appliedCmds[index]
		key := fmt.Sprintf("%v_%v_%v", args.Key, args.ClientId, args.Seq)
		if ret.Key == key {
			switch ret.Result.(type) {
			case PutAppendReply:
				*reply = ret.Result.(PutAppendReply)
				kv.mu.Unlock()
				if kv.isWrongGroup(args.Shard) {
					reply.Err = ErrWrongGroup
				}
				return
			}
		}
		kv.mu.Unlock()
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

func (kv *ShardKV) doGet(index int, args GetArgs) *appliedResult {
	var (
		value string
	)
	dedup := map[string]bool{}
	logs := kv.rf.GetLog()

	var (
		lastIncludedIndex int = 0
		lastIncludedTerm  int = 0
		seqs              []*ClientSeq
		kvs               []*KeyValue
		validCmds         []int
	)
	maxClientSeqs := map[int64]int64{}
	state := kv.persister.ReadSnapshot()
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	if kv.persister.SnapshotSize() != 0 &&
		(d.Decode(&lastIncludedIndex) != nil ||
			d.Decode(&lastIncludedTerm) != nil ||
			d.Decode(&seqs) != nil ||
			d.Decode(&kvs) != nil) {
		// return "", err	// fixme
	}
	for _, kv := range kvs {
		if kv.Key == args.Key {
			value = kv.Value
			break
		}
	}
	for i := range seqs {
		clientSeq := seqs[i]
		maxClientSeqs[clientSeq.ClientId] = clientSeq.Seq
	}

	for i := lastIncludedIndex + 1; i <= index; i++ {
		cmd := logs[kv.rf.Index(i)].Command
		kv.handleCmd(&value, &args.Key, cmd, maxClientSeqs, dedup, &validCmds, i)
	}

	ret := appliedResult{
		Key: fmt.Sprintf("%v_%v_%v", args.Key, args.ClientId, args.Seq),
		Result: GetReply{
			WrongLeader: false,
			Err:         OK,
			Value:       value,
		},
	}
	return &ret
}
func (kv *ShardKV) handleCmd(
	value *string,
	key *string,
	cmd interface{},
	maxClientSeqs map[int64]int64,
	dedup map[string]bool,
	validCmds *[]int, index int) {
	switch cmd.(type) {
	case PutAppendArgs:
		pa := cmd.(PutAppendArgs)
		if pa.Key != *key {
			return
		}
		dkey := fmt.Sprintf("%v_%v_%v", pa.Key, pa.ClientId, pa.Seq)
		if _, ok := dedup[dkey]; ok {
			return
		}
		dedup[dkey] = true
		if seq, ok := maxClientSeqs[pa.ClientId]; ok && seq >= pa.Seq && index > 0 {
			return
		}
		if index > 0 {
			*validCmds = append(*validCmds, index)
		}
		if pa.Op == "Put" {
			*value = pa.Value
		} else if pa.Op == "Append" {
			*value += pa.Value
		} else {
			log.Fatal("Can not happend")
		}
	case PushShardArgs:
		ps := cmd.(PushShardArgs)
		if ps.Shard != key2shard(*key) {
			return
		}
		// dkey := fmt.Sprintf("%v:%v_%v_%v", ps.ConfigNum, ps.Shard, ps.ClientId, ps.Seq)
		dkey := fmt.Sprintf("%v:%v_%v_%v", ps.ConfigNum, ps.Shard, 0, 0)
		if _, ok := dedup[dkey]; ok {
			return
		}
		dedup[dkey] = true
		if seq, ok := maxClientSeqs[ps.ClientId]; ok && seq >= ps.Seq {
			return
		}
		if index > 0 {
			*validCmds = append(*validCmds, index)
		}
		if v, ok := ps.Data[*key]; ok {
			*value = v
		}
		for i := 0; i < len(ps.Logs); i++ {
			subCmd := ps.Logs[i].Command
			kv.handleCmd(value, key, subCmd, maxClientSeqs, dedup, nil, -1)
		}
	default:
	}
}

func (kv *ShardKV) doPutAppend(index int, args PutAppendArgs) *appliedResult {
	ret := appliedResult{
		Key:    fmt.Sprintf("%v_%v_%v", args.Key, args.ClientId, args.Seq),
		Result: PutAppendReply{Err: OK},
	}
	return &ret
}

func (kv *ShardKV) doPushShard(index int, args PushShardArgs) *appliedResult {
	return &appliedResult{
		Key: fmt.Sprintf("%v:%v_%v_%v",
			args.ConfigNum, args.Shard, args.ClientId, args.Seq),
		Result: PushShardReply{Err: OK, ConfigNum: kv.config.Num},
	}
}

func (kv *ShardKV) trySnapshot(index int) {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}

	logs := kv.rf.GetLog()
	var (
		lastIncludedIndex int = 0
		lastIncludedTerm  int = 0
		seqs              []*ClientSeq
		kvs               []*KeyValue
		validCmds         []int
	)
	oldMaxClientSeqs := map[int64]int64{}
	maxClientSeqs := map[int64]int64{}
	kvsMap := map[string]string{}
	state := kv.persister.ReadSnapshot()
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	if kv.persister.SnapshotSize() != 0 &&
		(d.Decode(&lastIncludedIndex) != nil ||
			d.Decode(&lastIncludedTerm) != nil ||
			d.Decode(&seqs) != nil ||
			d.Decode(&kvs) != nil) {
		return
	}
	// applied
	if lastIncludedIndex >= index {
		return
	}

	for i := range kvs {
		kv := kvs[i]
		kvsMap[kv.Key] = kv.Value
	}
	for i := range seqs {
		cs := seqs[i]
		oldMaxClientSeqs[cs.ClientId] = cs.Seq
		maxClientSeqs[cs.ClientId] = cs.Seq
	}

	dedup := map[string]bool{}
	for i := lastIncludedIndex + 1; i <= index; i++ {
		cmd := logs[kv.rf.Index(i)].Command
		kv.handleCmdSnap(kvsMap, cmd, oldMaxClientSeqs, maxClientSeqs, dedup, &validCmds, i)
	}
	kvsSnapshot := []*KeyValue{}
	for k, v := range kvsMap {
		kvsSnapshot = append(kvsSnapshot, &KeyValue{Key: k, Value: v})
	}
	seqsSnapshot := []*ClientSeq{}
	for clientId, seq := range maxClientSeqs {
		seqsSnapshot = append(seqsSnapshot, &ClientSeq{ClientId: clientId, Seq: seq})
	}

	kv.lastIncludedIndex = index
	kv.lastIncludedTerm = logs[kv.rf.Index(index)].Term
	kv.persist(kvsSnapshot, seqsSnapshot)
	kv.rf.Persist(index)

	// kv.dump()
	return
}
func (kv *ShardKV) handleCmdSnap(
	kvsMap map[string]string,
	cmd interface{},
	oldMaxClientSeqs map[int64]int64,
	maxClientSeqs map[int64]int64,
	dedup map[string]bool,
	validCmds *[]int, index int) {
	switch cmd.(type) {
	case PutAppendArgs:
		pa := cmd.(PutAppendArgs)
		if seq, ok := oldMaxClientSeqs[pa.ClientId]; ok && seq >= pa.Seq && index > 0 {
			return
		}
		dkey := fmt.Sprintf("%v_%v_%v", pa.Key, pa.ClientId, pa.Seq)
		if _, ok := dedup[dkey]; ok {
			return
		}
		if index > 0 {
			*validCmds = append(*validCmds, index)
		}
		dedup[dkey] = true
		if seq, ok := maxClientSeqs[pa.ClientId]; !ok || seq < pa.Seq {
			maxClientSeqs[pa.ClientId] = pa.Seq
		}
		if pa.Op == "Put" {
			kvsMap[pa.Key] = pa.Value
		} else if pa.Op == "Append" {
			kvsMap[pa.Key] += pa.Value
		} else {
			log.Fatal("Can not happend")
		}
	case PushShardArgs:
		ps := cmd.(PushShardArgs)
		// if kv.config.Shards[ps.Shard] != kv.gid {
		// 	return
		// }
		if seq, ok := oldMaxClientSeqs[ps.ClientId]; ok && seq >= ps.Seq {
			return
		}
		// dkey := fmt.Sprintf("%v:%v_%v_%v",
		// 		ps.ConfigNum, ps.Shard, ps.ClientId, ps.Seq)
		dkey := fmt.Sprintf("%v:%v_%v_%v",
			ps.ConfigNum, ps.Shard, 0, 0)
		if _, ok := dedup[dkey]; ok {
			return
		}
		if index > 0 {
			*validCmds = append(*validCmds, index)
		}
		dedup[dkey] = true
		if seq, ok := maxClientSeqs[ps.ClientId]; !ok || seq < ps.Seq {
			maxClientSeqs[ps.ClientId] = ps.Seq
		}
		// execute push shard command
		for k, v := range ps.Data {
			kvsMap[k] = v
		}
		for i := 0; i < len(ps.Logs); i++ {
			subCmd := ps.Logs[i].Command
			kv.handleCmdSnap(kvsMap, subCmd, oldMaxClientSeqs, maxClientSeqs, dedup, nil, -1)
		}
	default:
	}
}
func (kv *ShardKV) persist(kvsSnapshot []*KeyValue, seqsSnapshot []*ClientSeq) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastIncludedIndex)
	e.Encode(kv.lastIncludedTerm)
	e.Encode(seqsSnapshot)
	e.Encode(kvsSnapshot)
	data := w.Bytes()
	kv.persister.SaveSnapshot(data)
}

func (kv *ShardKV) Name() string {
	return fmt.Sprintf("server-%v-%v", kv.gid, kv.me)
}

func (kv *ShardKV) Apply(index int, cmd interface{}, isLeader bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch cmd.(type) {
	case GetArgs:
		c := cmd.(GetArgs)
		if c.ConfigNum > kv.seenMaxConfigNum {
			kv.seenMaxConfigNum = c.ConfigNum
		}
		if !isLeader {
			kv.trySnapshot(index)
			return
		}
		kv.appliedCmds[index] = kv.doGet(index, c)
	case PutAppendArgs:
		c := cmd.(PutAppendArgs)
		if c.ConfigNum > kv.seenMaxConfigNum {
			kv.seenMaxConfigNum = c.ConfigNum
		}
		if !isLeader {
			kv.trySnapshot(index)
			return
		}
		kv.appliedCmds[index] = kv.doPutAppend(index, c)
	case PushShardArgs:
		c := cmd.(PushShardArgs)
		if c.ConfigNum > kv.seenMaxConfigNum {
			kv.seenMaxConfigNum = c.ConfigNum
		}
		kv.appliedCmds[index] = kv.doPushShard(index, c)
		if _, ok := kv.addedShards[c.ConfigNum]; ok {
			if _, ok := kv.addedShards[c.ConfigNum][c.Shard]; ok {
				if atomic.LoadInt32(kv.addedShards[c.ConfigNum][c.Shard]) > 0 {
					atomic.StoreInt32(kv.addedShards[c.ConfigNum][c.Shard], 0)
				}
			}
		}
	case PushShardsMarkArgs:
		c := cmd.(PushShardsMarkArgs)
		if c.ConfigNum > kv.seenMaxConfigNum {
			kv.seenMaxConfigNum = c.ConfigNum
		}
		if _, ok := kv.configs[c.ConfigNum]; ok {
			kv.configs[c.ConfigNum].done = true
		}
		kv.appliedCmds[index] = &appliedResult{
			Key:    fmt.Sprintf("%v_%v_%v", "pushShardsMark", c.ClientId, c.Seq),
			Result: PushShardsMarkReply{},
		}
	default:
	}
	kv.mu.Unlock()
	if _, ok := kv.notices[index]; ok {
		kv.notices[index].Broadcast()
	}
	// snapshot
	kv.mu.Lock()
	kv.trySnapshot(index)
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
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PushShardArgs{})
	labgob.Register(PushShardsMarkArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	log.SetFlags(log.Llongfile | log.LstdFlags)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.persister = persister
	kv.rf.SetApp(kv)
	kv.notices = make(map[int]*sync.Cond)
	kv.appliedCmds = make(map[int]*appliedResult)
	kv.id = nrand()
	kv.seq = 0
	kv.ids = [shardmaster.NShards]int64{}
	kv.seqs = [shardmaster.NShards]int64{}
	kv.configScheduler = make(chan *shardmaster.Config)
	kv.reconfiging = 0
	for i := 0; i < shardmaster.NShards; i++ {
		kv.ids[i] = nrand()
	}
	kv.addedShards = make(map[int]map[int]*int32)
	kv.configs = make(map[int]*configStruct)

	// config num
	one := kv.mck.Query(-1)
	kv.config = &one

	// server loop
	go func() {
		for applyMsg := range kv.applyCh {
			if false {
				DPrintf("%#v", applyMsg)
			}
		}
	}()

	// poll shardmaster periodically
	go kv.periodicallyPoll(100)
	go kv.scheduleConfig()

	return kv
}
