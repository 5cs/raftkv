package raftkv

import (
	"bytes"
	"fmt"
	"raftkv/labgob"
	"raftkv/labrpc"
	"log"
	"raftkv/raft"
	"sync"
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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister         *raft.Persister
	db                map[string]string
	clientSeqs        map[int64]int64
	notices           map[int]*sync.Cond
	appliedCmds       map[int]*appliedResult
	lastIncludedIndex int
	lastIncludedTerm  int
	restart           bool
}

type appliedResult struct {
	Key    string
	Result interface{}
}

func (kv *KVServer) getFmtKey(args *GetArgs) string {
	return fmt.Sprintf("%v_%v_%v", args.Key, args.ClientId, args.Seq)
}

func (kv *KVServer) putAppendFmtKey(args *PutAppendArgs) string {
	return fmt.Sprintf("%v_%v_%v", args.Key, args.ClientId, args.Seq)
}

func (kv *KVServer) processCmd(cmd interface{}, fmtKey string) (interface{}, bool) {
	for {
		index, _, isLeader := kv.rf.Start(cmd)
		if !isLeader {
			return nil, false
		}

		kv.mu.Lock()
		if _, ok := kv.notices[index]; !ok {
			kv.notices[index] = sync.NewCond(&kv.mu)
		}
		kv.notices[index].Wait()

		ret := kv.appliedCmds[index]
		kv.mu.Unlock()
		if ret.Key == fmtKey {
			return ret.Result, true
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := *args
	if res, isLeader := kv.processCmd(cmd, kv.getFmtKey(args)); !isLeader {
		reply.WrongLeader = true
	} else {
		*reply = res.(GetReply)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if seq, ok := kv.clientSeqs[args.ClientId]; ok && args.Seq <= seq {
		reply.Err = OK
		kv.mu.Unlock()
		return // applied
	}
	kv.mu.Unlock()

	cmd := *args
	if res, isLeader := kv.processCmd(cmd, kv.putAppendFmtKey(args)); !isLeader {
		reply.WrongLeader = true
	} else {
		*reply = res.(PutAppendReply)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) Name() string {
	return fmt.Sprintf("%#v", kv.me)
}

// Apply method for Raft
func (kv *KVServer) Apply(applyMsg interface{}) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	msg := applyMsg.(*raft.ApplyMsg)
	index := msg.CommandIndex
	cmd := msg.Command
	var (
		key   string      = ""
		reply interface{} = nil
	)
	switch cmd.(type) {
	case GetArgs:
		args := cmd.(GetArgs)
		key, reply = kv.getFmtKey(&args), kv.get(&args)
	case PutAppendArgs:
		args := cmd.(PutAppendArgs)
		key, reply = kv.putAppendFmtKey(&args), kv.putAppend(&args)
	case *raft.InstallSnapshotArgs: // install snapshot
		kv.readSnapshot(kv.persister.ReadSnapshot())
	default:
	}

	if reply != nil {
		kv.trySnapshot(index)
		// send result
		if _, ok := kv.notices[index]; ok {
			kv.appliedCmds[index] = &appliedResult{
				Key:    key,
				Result: reply,
			}
			kv.notices[index].Broadcast()
		}
	}
}

func (kv *KVServer) get(args *GetArgs) GetReply {
	if value, ok := kv.db[args.Key]; !ok {
		return GetReply{Value: "", Err: ErrNoKey}
	} else {
		return GetReply{Value: value, Err: OK}
	}
}

func (kv *KVServer) putAppend(args *PutAppendArgs) PutAppendReply {
	if seq, ok := kv.clientSeqs[args.ClientId]; ok && seq >= args.Seq {
		return PutAppendReply{Err: OK}
	}
	kv.clientSeqs[args.ClientId] = args.Seq
	if args.Op == "Put" {
		kv.db[args.Key] = args.Value
	} else {
		kv.db[args.Key] += args.Value
	}
	return PutAppendReply{Err: OK}
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	var (
		lastIncludedIndex int = 0
		lastIncludedTerm  int = 0
	)
	kv.db = make(map[string]string)
	kv.clientSeqs = make(map[int64]int64)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)
	d.Decode(&kv.clientSeqs)
	d.Decode(&kv.db)
	kv.lastIncludedIndex = lastIncludedIndex
	kv.lastIncludedTerm = lastIncludedTerm
}

func (kv *KVServer) trySnapshot(index int) {
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

func (kv *KVServer) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastIncludedIndex)
	e.Encode(kv.lastIncludedTerm)
	e.Encode(kv.clientSeqs)
	e.Encode(kv.db)
	data := w.Bytes()
	kv.persister.SaveSnapshot(data)
	kv.rf.TruncateLog(kv.lastIncludedIndex)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendReply{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.persister = persister
	kv.rf.SetApp(kv)
	kv.db = make(map[string]string)
	kv.clientSeqs = make(map[int64]int64)
	kv.notices = make(map[int]*sync.Cond)
	kv.appliedCmds = make(map[int]*appliedResult)
	kv.readSnapshot(kv.persister.ReadSnapshot())
	DPrintf("Op \"InstallSnapshot\" at %#v, get values: %#v, reqId: %#v\n", kv.me, kv.db, -1)

	// server loop
	go func() {
		for applyMsg := range kv.applyCh {
			if false {
				DPrintf("%#v", applyMsg)
			}
		}
	}()

	return kv
}
