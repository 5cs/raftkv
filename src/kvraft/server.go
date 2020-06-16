package raftkv

import (
	// "helper" // Applier interface
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	// "time"
)

const Debug = 1

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
	getNotice         map[int]*sync.Cond
	putAppendNotice   map[int]*sync.Cond
	appliedCmds       map[int]*appliedResult
	lastIncludedIndex int
	lastIncludedTerm  int
	restart           bool
}

type appliedResult struct {
	Key    string
	Result interface{}
}

func getFmtKey(args *GetArgs) string {
	return fmt.Sprintf("%v_%v_%v", args.Key, args.ClientId, args.Seq)
}

func putAppendFmtKey(args *PutAppendArgs) string {
	return fmt.Sprintf("%v_%v_%v", args.Key, args.ClientId, args.Seq)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
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
		if _, ok := kv.getNotice[index]; !ok {
			kv.getNotice[index] = sync.NewCond(&kv.mu)
		}
		kv.getNotice[index].Wait()

		ret := kv.appliedCmds[index]
		k := getFmtKey(&cmd)
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	if seq, ok := kv.clientSeqs[args.ClientId]; ok && args.Seq <= seq {
		reply.Err = OK
		kv.mu.Unlock()
		return // applied
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
		if _, ok := kv.putAppendNotice[index]; !ok {
			kv.putAppendNotice[index] = sync.NewCond(&kv.mu)
		}
		kv.putAppendNotice[index].Wait()

		ret := kv.appliedCmds[index]
		k := putAppendFmtKey(&cmd)
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

// Apply method for Raft
func (kv *KVServer) Apply(applyMsg interface{}) {
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
		reply := kv.getValue(&args, index, isLeader)
		kv.appliedCmds[index] = &appliedResult{
			Key:    getFmtKey(&args),
			Result: reply,
		}
		if _, ok := kv.getNotice[index]; ok {
			kv.mu.Unlock()
			kv.getNotice[index].Broadcast()
		} else {
			kv.mu.Unlock()
		}
	case PutAppendArgs:
		kv.trySnapshot(index)
		args := cmd.(PutAppendArgs)
		reply := kv.putAppendValue(&args, index, isLeader)
		kv.appliedCmds[index] = &appliedResult{
			Key:    putAppendFmtKey(&args),
			Result: reply,
		}
		if _, ok := kv.putAppendNotice[index]; ok {
			kv.mu.Unlock()
			kv.putAppendNotice[index].Broadcast()
		} else {
			kv.mu.Unlock()
		}
	case *raft.InstallSnapshotArgs: // install snapshot
		args := cmd.(*raft.InstallSnapshotArgs)
		kv.readSnapshot(kv.persister.ReadSnapshot())
		DPrintf("Op \"InstallSnapshot\" at %#v, get values: %#v, reqId: %#v, leaderId-term-index:%#v-%#v-%#v\n",
			kv.rf.GetRaftInstanceName(), kv.db, args.ReqId, args.LeaderId, args.LastIncludedTerm, args.LastIncludedIndex)
		kv.mu.Unlock()
	default:
		kv.trySnapshot(index)
		kv.mu.Unlock()
	}
}

func (kv *KVServer) trySnapshot(index int) {
	if !((kv.maxraftstate != -1 &&
		float64(kv.persister.RaftStateSize()) >= float64(kv.maxraftstate)*0.8) ||
		(kv.restart && kv.lastIncludedIndex > 0)) {
		return
	}
	kv.restart = false

	var (
		lastIncludedIndex int = 0
		lastIncludedTerm  int = 0
	)
	logs := kv.rf.GetLog()
	state := kv.persister.ReadSnapshot()
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	if kv.persister.SnapshotSize() != 0 &&
		(d.Decode(&lastIncludedIndex) != nil ||
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
				kv.db[cmd.Key] = cmd.Value
			} else if cmd.Op == "Append" {
				kv.db[cmd.Key] += cmd.Value
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

func (kv *KVServer) putAppendValue(args *PutAppendArgs, index int, isLeader bool) PutAppendReply {
	if seq, ok := kv.clientSeqs[args.ClientId]; ok && seq >= args.Seq {
		DPrintf("Op %#v at %#v, key:%#v, value:%#v, client:%#v, seq:%#v, original:%#v, clientId-seq-index:%#v-%#v-%#v, %#v, filtered by %#v\n",
			args.Op, kv.rf.GetRaftInstanceName(), args.Key, args.Value, args.ClientId, args.Seq, kv.db[args.Key], args.ClientId, args.Seq, index, isLeader, seq)
		return PutAppendReply{Err: OK}
	}
	DPrintf("Op %#v at %#v, key:%#v, value:%#v, client:%#v, seq:%#v, original:%#v, clientId-seq-index:%#v-%#v-%#v, %#v\n",
		args.Op, kv.rf.GetRaftInstanceName(), args.Key, args.Value, args.ClientId, args.Seq, kv.db[args.Key], args.ClientId, args.Seq, index, isLeader)
	kv.clientSeqs[args.ClientId] = args.Seq
	if args.Op == "Put" {
		kv.db[args.Key] = args.Value
	} else {
		kv.db[args.Key] += args.Value
	}
	return PutAppendReply{Err: OK}
}

func (kv *KVServer) getValue(args *GetArgs, index int, isLeader bool) GetReply {
	if value, ok := kv.db[args.Key]; !ok {
		return GetReply{Value: "", Err: ErrNoKey}
	} else {
		DPrintf("Op Get at %#v, key:%#v value:%#v, clientId-seq-index:%#v-%#v-%#v, %#v\n",
			kv.rf.GetRaftInstanceName(), args.Key, value, args.ClientId, args.Seq, index, isLeader)
		return GetReply{Value: value, Err: OK}
	}
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
	kv.getNotice = make(map[int]*sync.Cond)
	kv.putAppendNotice = make(map[int]*sync.Cond)
	kv.appliedCmds = make(map[int]*appliedResult)
	kv.readSnapshot(kv.persister.ReadSnapshot())
	kv.restart = true
	DPrintf("Op \"InstallSnapshot\" at %#v, get values: %#v, reqId: %#v\n", kv.rf.GetRaftInstanceName(), kv.db, -1)

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
