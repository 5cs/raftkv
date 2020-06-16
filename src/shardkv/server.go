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
	keyValueStore     [shardmaster.NShards]map[string]string

	id     int64
	seq    int64
	mck    *shardmaster.Clerk
	config shardmaster.Config
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

// Apply method called by Raft instance
func (kv *ShardKV) Apply(index int, cmd interface{}, isLeader bool) {
	if !isLeader {
		kv.trySnapshot(index)
		return
	}

	kv.mu.Lock()
	switch cmd.(type) {
	case GetArgs:
		args := cmd.(GetArgs)
		value, err := kv.getValue(args.Key, index)
		kv.mu.Unlock()
		reply := GetReply{Value: value, Err: err}
		kv.mu.Lock()
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
		kv.clientSeqs[args.ClientId] = args.Seq
		kv.mu.Unlock()
		reply := PutAppendReply{Err: OK}
		kv.mu.Lock()
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
	default:
		kv.mu.Unlock()
	}

	kv.trySnapshot(index)
}

func (kv *ShardKV) getValue(key string, index int) (string, Err) {
	if value, ok := kv.keyValueStore[key]; !ok {
		return ErrNoKey
	} else {
		return value, OK
	}

	//var (
	//	value string
	//	err   Err = ErrNoKey
	//)

	//// get value from snapshot
	//var (
	//	lastIncludedIndex int = 0
	//	lastIncludedTerm  int = 0
	//	seqs              []*ClientSeq
	//	kvs               []*KeyValue
	//)
	//maxClientSeqs := map[int64]int64{}
	//state := kv.persister.ReadSnapshot()
	//r := bytes.NewBuffer(state)
	//d := labgob.NewDecoder(r)
	//if kv.persister.SnapshotSize() != 0 &&
	//	(d.Decode(&lastIncludedIndex) != nil ||
	//		d.Decode(&lastIncludedTerm) != nil ||
	//		d.Decode(&seqs) != nil ||
	//		d.Decode(&kvs) != nil) {
	//	return "", err
	//}
	//for _, kv := range kvs {
	//	if kv.Key == key {
	//		value = kv.Value
	//		break
	//	}
	//}
	//for i := range seqs {
	//	clientSeq := seqs[i]
	//	maxClientSeqs[clientSeq.ClientId] = clientSeq.Seq
	//}

	//dedup := map[string]bool{}
	//logs := kv.rf.GetLog()
	//for i := lastIncludedIndex + 1; i <= index; i++ {
	//	logEntry := logs[kv.rf.Index(i)]
	//	switch logEntry.Command.(type) {
	//	case PutAppendArgs:
	//		entry := logEntry.Command.(PutAppendArgs)
	//		if entry.Key != key {
	//			continue
	//		}
	//		dedupKey := kv.putAppendFmtKey(&entry)
	//		if _, ok := dedup[dedupKey]; ok {
	//			continue
	//		}
	//		dedup[dedupKey] = true
	//		if seq, ok := maxClientSeqs[entry.ClientId]; ok && seq >= entry.Seq {
	//			continue
	//		}
	//		err = OK
	//		if entry.Op == "Put" {
	//			value = entry.Value
	//		} else if entry.Op == "Append" {
	//			value += entry.Value
	//		} else {
	//			log.Fatal("Can not happend")
	//		}
	//	default:
	//	}
	//}

	//return value, err
}

func (kv *ShardKV) putAppendValue(key string, value string, index int) Err {

}

func (kv *ShardKV) trySnapshot(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}

	logs := kv.rf.GetLog()
	var (
		lastIncludedIndex int = 0
		lastIncludedTerm  int = 0
		seqs              []*ClientSeq
		kvs               []*KeyValue
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
	for i := range kvs {
		kv := kvs[i]
		kvsMap[kv.Key] = kv.Value
	}
	for i := range seqs {
		clientSeq := seqs[i]
		oldMaxClientSeqs[clientSeq.ClientId] = clientSeq.Seq
		maxClientSeqs[clientSeq.ClientId] = clientSeq.Seq
	}

	dedup := map[string]bool{}
	for i := lastIncludedIndex + 1; i <= index; i++ {
		logEntry := logs[kv.rf.Index(i)]
		switch logEntry.Command.(type) {
		case PutAppendArgs:
			entry := logEntry.Command.(PutAppendArgs)
			if seq, ok := oldMaxClientSeqs[entry.ClientId]; ok && seq >= entry.Seq {
				continue
			}
			dedupKey := kv.putAppendFmtKey(&entry)
			if _, ok := dedup[dedupKey]; ok {
				continue
			}
			dedup[dedupKey] = true
			if seq, ok := maxClientSeqs[entry.ClientId]; !ok || seq < entry.Seq {
				maxClientSeqs[entry.ClientId] = entry.Seq
			}
			if entry.Op == "Put" {
				kvsMap[entry.Key] = entry.Value
			} else if entry.Op == "Append" {
				kvsMap[entry.Key] += entry.Value
			} else {
				log.Fatal("Can not happend")
			}
		default:
		}
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
	return
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

func (kv *ShardKV) pullAndSyncShards(config *shardmaster.Config) bool {

}

func (kv *ShardKV) pollConfig(ms int) {
	// poll configs
	for {
		select {
		case <-time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
			if _, isLeader := kv.rf.GetState(); isLeader {
				latestConfig := kv.mck.Query(-1)
				for i := kv.config.Num + 1; i <= latestConfig.Num; i++ {
					ok := kv.pullAndSyncShards(kv.mck.Query(i))
					if !ok {
						break
					}
				}
			}
		}):
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
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})

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
	kv.rf = raft.Make(servers, kv.me, persister, kv.applyCh)

	kv.persister = persister
	kv.rf.SetApp(kv)
	kv.clientSeqs = make(map[int64]int64)
	kv.notices = make(map[int]*sync.Cond)
	kv.appliedCmds = make(map[int]*appliedResult)

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
