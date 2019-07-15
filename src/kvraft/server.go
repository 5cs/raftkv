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
	clientSeqs        map[int64]int64
	getNotice         map[int]*sync.Cond
	putAppendNotice   map[int]*sync.Cond
	appliedCmds       map[int]*appliedResult
	lastIncludedIndex int
	lastIncludedTerm  int
}

type appliedResult struct {
	Key    string
	Result interface{}
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
		k := fmt.Sprintf("%v_%v_%v", cmd.Key, cmd.ClientId, cmd.Seq)
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
		k := fmt.Sprintf("%v_%v_%v", cmd.Key, cmd.ClientId, cmd.Seq)
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
func (kv *KVServer) Apply(index int, cmd interface{}, isLeader bool) {
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
			Key:    fmt.Sprintf("%v_%v_%v", args.Key, args.ClientId, args.Seq),
			Result: reply,
		}
		if _, ok := kv.getNotice[index]; ok {
			kv.mu.Unlock()
			kv.getNotice[index].Broadcast()
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
			Key:    fmt.Sprintf("%v_%v_%v", args.Key, args.ClientId, args.Seq),
			Result: reply,
		}
		if _, ok := kv.putAppendNotice[index]; ok {
			kv.mu.Unlock()
			kv.putAppendNotice[index].Broadcast()
		} else {
			kv.mu.Unlock()
		}
	default:
		kv.mu.Unlock()
	}

	kv.trySnapshot(index)
}

func (kv *KVServer) trySnapshot(index int) {
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
	// Key := key_ClientId_Seq
	dedup := map[string]bool{}
	for i := lastIncludedIndex + 1; i <= index; i++ {
		logEntry := logs[kv.rf.Index(i)]
		switch logEntry.Command.(type) {
		case PutAppendArgs:
			entry := logEntry.Command.(PutAppendArgs)
			if seq, ok := oldMaxClientSeqs[entry.ClientId]; ok && seq >= entry.Seq {
				continue
			}
			dkey := fmt.Sprintf("%v_%v_%v", entry.Key, entry.ClientId, entry.Seq)
			if _, ok := dedup[dkey]; ok {
				continue
			}
			dedup[dkey] = true
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

	// persist snapshot and raft state
	kv.persist(kvsSnapshot, seqsSnapshot)
	kv.rf.Persist(index)
	return
}

// hold kv.mu
func (kv *KVServer) persist(kvsSnapshot []*KeyValue, seqsSnapshot []*ClientSeq) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastIncludedIndex)
	e.Encode(kv.lastIncludedTerm)
	e.Encode(seqsSnapshot)
	e.Encode(kvsSnapshot)
	data := w.Bytes()
	kv.persister.SaveSnapshot(data)
}

// execute raft's command, get value; hold kv.mu
func (kv *KVServer) getValue(key string, index int) (string, Err) {
	var (
		value string
		err   Err = ErrNoKey
	)

	state := kv.persister.ReadRaftState()
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	var (
		currentTerm       int
		votedFor          int
		lastExcludedIndex int
		lastExcludedTerm  int
		logs              []raft.LogEntry
	)

	// Key := key_ClientId_Seq
	dedup := map[string]bool{}
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastExcludedIndex) != nil ||
		d.Decode(&lastExcludedTerm) != nil ||
		d.Decode(&logs) != nil {
		return "", err
	}

	// get value from snapshot
	var (
		lastIncludedIndex int = 0
		lastIncludedTerm  int = 0
		seqs              []*ClientSeq
		kvs               []*KeyValue
	)
	maxClientSeqs := map[int64]int64{}
	state = kv.persister.ReadSnapshot()
	r = bytes.NewBuffer(state)
	d = labgob.NewDecoder(r)
	if kv.persister.SnapshotSize() != 0 &&
		(d.Decode(&lastIncludedIndex) != nil ||
			d.Decode(&lastIncludedTerm) != nil ||
			d.Decode(&seqs) != nil ||
			d.Decode(&kvs) != nil) {
		return "", err
	}
	for _, kv := range kvs {
		if kv.Key == key {
			value = kv.Value
			break
		}
	}
	for i := range seqs {
		clientSeq := seqs[i]
		maxClientSeqs[clientSeq.ClientId] = clientSeq.Seq
	}

	for i := lastIncludedIndex + 1; i <= index; i++ {
		logEntry := logs[kv.rf.Index(i)]
		switch logEntry.Command.(type) {
		case PutAppendArgs:
			entry := logEntry.Command.(PutAppendArgs)
			if entry.Key != key {
				continue
			}
			dkey := fmt.Sprintf("%v_%v_%v", entry.Key, entry.ClientId, entry.Seq)
			if _, ok := dedup[dkey]; ok {
				continue
			}
			dedup[dkey] = true
			if seq, ok := maxClientSeqs[entry.ClientId]; ok && seq >= entry.Seq {
				continue
			}
			err = OK
			if entry.Op == "Put" {
				value = entry.Value
			} else if entry.Op == "Append" {
				value += entry.Value
			} else {
				log.Fatal("Can not happend")
			}
		default:
		}
	}
	return value, err
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.persister = persister
	kv.rf.SetApp(kv)
	kv.clientSeqs = make(map[int64]int64)
	kv.getNotice = make(map[int]*sync.Cond)
	kv.putAppendNotice = make(map[int]*sync.Cond)
	kv.appliedCmds = make(map[int]*appliedResult)

	// server loop
	go func() {
		for applyMsg := range kv.applyCh {
			DPrintf("%#v", applyMsg)
		}
	}()

	return kv
}
