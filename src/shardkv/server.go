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
	restart           bool

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
		DPrintf("Op \"InstallSnapshot\" at %#v, get values: %#v, reqId: %#v, leaderId-term-index:%#v-%#v-%#v\n",
			kv.rf.GetRaftInstanceName(), kv.db, args.ReqId, args.LeaderId, args.LastIncludedTerm, args.LastIncludedIndex)
		kv.mu.Unlock()
	default:
		kv.trySnapshot(index)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) get(args *GetArgs, index int, isLeader bool) GetReply {
	if value, ok := kv.db[key2shard(args.Key)][args.Key]; !ok {
		return GetReply{Value: "", Err: ErrNoKey}
	} else {
		DPrintf("Op Get at %#v, key:%#v value:%#v, clientId-seq-index:%#v-%#v-%#v, %#v\n",
			kv.rf.GetRaftInstanceName(), args.Key, value, args.ClientId, args.Seq, index, isLeader)
		return GetReply{Value: value, Err: OK}
	}
}

func (kv *ShardKV) putAppend(args *PutAppendArgs, index int, isLeader bool) PutAppendReply {
	if seq, ok := kv.clientSeqs[args.ClientId]; ok && seq >= args.Seq {
		DPrintf("Op %#v at %#v, key:%#v, value:%#v, client:%#v, seq:%#v, original:%#v, clientId-seq-index:%#v-%#v-%#v, %#v, filtered by %#v\n",
			args.Op, kv.rf.GetRaftInstanceName(), args.Key, args.Value, args.ClientId, args.Seq, kv.db[key2shard(args.Key)][args.Key], args.ClientId, args.Seq, index, isLeader, seq)
		return PutAppendReply{Err: OK}
	}
	DPrintf("Op %#v at %#v, key:%#v, value:%#v, client:%#v, seq:%#v, original:%#v, clientId-seq-index:%#v-%#v-%#v, %#v\n",
		args.Op, kv.rf.GetRaftInstanceName(), args.Key, args.Value, args.ClientId, args.Seq, kv.db[key2shard(args.Key)][args.Key], args.ClientId, args.Seq, index, isLeader)
	kv.clientSeqs[args.ClientId] = args.Seq
	if args.Op == "Put" {
		kv.db[key2shard(args.Key)][args.Key] = args.Value
	} else {
		kv.db[key2shard(args.Key)][args.Key] += args.Value
	}
	return PutAppendReply{Err: OK}
}

func (kv *ShardKV) trySnapshot(index int) {
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
	)
	kv.db = [shardmaster.NShards]map[string]string{}
	for i := 0; i < shardmaster.NShards; i++ {
		kv.db[i] = make(map[string]string)
	}
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

func (kv *ShardKV) pullAndSyncShards(config shardmaster.Config) bool {
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

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

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
