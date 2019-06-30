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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister         *raft.Persister
	getReplies        map[int](chan GetReply)
	putAppendReplies  map[int](chan PutAppendReply)
	clientSeqs        map[int64]int64
	clientPendingSeqs map[int64]int64
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

		replyChan := make(chan GetReply, 1)
		kv.mu.Lock()
		kv.getReplies[index] = replyChan
		kv.mu.Unlock()

	loop:
		for {
			select {
			case <-time.After(time.Duration(55 * time.Millisecond)):
				kv.mu.Lock()
				if replyChan != kv.getReplies[index] {
					kv.mu.Unlock()
					break loop
				}
				kv.mu.Unlock()
			case *reply = <-kv.getReplies[index]:
				kv.mu.Lock()
				if replyChan != kv.getReplies[index] {
					kv.mu.Unlock()
					kv.getReplies[index] <- *reply
					break loop
				} else {
					kv.mu.Unlock()
					return
				}
			}
		}
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
	if pSeq, ok := kv.clientPendingSeqs[args.ClientId]; ok && args.Seq <= pSeq {
		kv.mu.Unlock()
		return // make client re-send request
	}
	kv.clientPendingSeqs[args.ClientId] = args.Seq
	kv.mu.Unlock()
	cmd := *args
	for {
		*reply = PutAppendReply{}
		index, _, isLeader := kv.rf.Start(cmd)
		if !isLeader {
			reply.WrongLeader = true
			return
		}

		replyChan := make(chan PutAppendReply, 1)
		kv.mu.Lock()
		kv.putAppendReplies[index] = replyChan
		kv.mu.Unlock()

	loop:
		for {
			select {
			case <-time.After(55 * time.Millisecond):
				kv.mu.Lock()
				if replyChan != kv.putAppendReplies[index] {
					kv.mu.Unlock()
					break loop
				}
				kv.mu.Unlock()
			case *reply = <-kv.putAppendReplies[index]:
				kv.mu.Lock()
				if replyChan != kv.putAppendReplies[index] {
					kv.mu.Unlock()
					kv.putAppendReplies[index] <- *reply // pass through
					break loop
				} else {
					kv.mu.Unlock()
					return
				}
			}
		}
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
func (kv *KVServer) Apply(index int, cmd interface{}) {
	kv.mu.Lock()

	switch cmd.(type) {
	case GetArgs:
		getArgs := cmd.(GetArgs)
		value, err := kv.getValue(getArgs.Key, index)
		if _, ok := kv.getReplies[index]; ok {
			if len(kv.getReplies[index]) > 0 {
				<-kv.getReplies[index]
			}
			kv.mu.Unlock()
			kv.getReplies[index] <- GetReply{Value: value, Err: err}
		} else {
			kv.mu.Unlock()
		}
	case PutAppendArgs:
		putAppendArgs := cmd.(PutAppendArgs)
		kv.clientSeqs[putAppendArgs.ClientId] = putAppendArgs.Seq
		if _, ok := kv.putAppendReplies[index]; ok {
			if len(kv.putAppendReplies[index]) > 0 {
				<-kv.putAppendReplies[index]
			}
			kv.mu.Unlock()
			kv.putAppendReplies[index] <- PutAppendReply{Err: OK}
		} else {
			kv.mu.Unlock()
		}
	default:
		kv.mu.Unlock()
	}
}

// execute raft's command, get value
func (kv *KVServer) getValue(key string, index int) (string, Err) {
	var (
		value string
		err   Err = ErrNoKey
	)

	state := kv.persister.ReadRaftState() // TODO: Lock
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	var (
		currentTerm int
		votedFor    int
		logs        []raft.LogEntry
	)

	// Key := key_ClientId_Seq
	dedup := map[string]bool{}
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		return "", err
	} else {
		for i := 1; i <= index; i++ {
			logEntry := logs[i-1]
			switch logEntry.Command.(type) {
			case PutAppendArgs:
				entry := logEntry.Command.(PutAppendArgs)
				if entry.Key != key {
					continue
				}
				dkey := fmt.Sprintf("%v_%v_%v", key, entry.ClientId, entry.Seq)
				if _, ok := dedup[dkey]; ok {
					continue
				} else {
					dedup[dkey] = true
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
	kv.getReplies = make(map[int](chan GetReply))
	kv.putAppendReplies = make(map[int](chan PutAppendReply))
	kv.clientSeqs = make(map[int64]int64)
	kv.clientPendingSeqs = make(map[int64]int64)

	// server loop
	go func() {
		for applyMsg := range kv.applyCh {
			DPrintf("%#v", applyMsg)
		}
	}()

	return kv
}
