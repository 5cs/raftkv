package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "raftkv/labrpc"
import "crypto/rand"
import "math/big"
import "raftkv/shardmaster"
import "time"
import "sync"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	mu     sync.Mutex
	id     int64
	seq    int64
	leader map[int]int
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.id, ck.seq = nrand(), 0
	ck.config = ck.sm.Query(-1)
	ck.leader = map[int]int{}
	for shard := 0; shard < len(ck.config.Shards); shard++ {
		ck.leader[shard] = 0
	}
	return ck
}

func (ck *Clerk) queryLatestConfig() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.config = ck.sm.Query(-1)
	return ck.config.Num
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	args := GetArgs{
		Key:       key,
		ClientId:  ck.id,
		Seq:       ck.seq,
		ConfigNum: ck.config.Num,
	}
	ck.seq++
	ck.mu.Unlock()

	done := make(chan GetReply, 0)
	go ck.tryGet(done, &args)
	for {
		select {
		case <-time.After(time.Duration(100) * time.Millisecond):
			args.ConfigNum = ck.queryLatestConfig()
			go ck.tryGet(done, &args)
		case reply := <-done:
			return reply.Value
		}
	}
}

func (ck *Clerk) tryGet(done chan GetReply, args *GetArgs) {
	ck.mu.Lock()
	if ck.config.Num == 0 {
		ck.mu.Unlock()
		return // wait valid config
	}
	// select leader
	shard := key2shard(args.Key)
	gid := ck.config.Shards[shard]
	servers := ck.config.Groups[gid]
	ck.leader[shard] = (ck.leader[shard] + 1) % len(servers)
	server := servers[ck.leader[shard]]
	srv := ck.make_end(server)
	ck.mu.Unlock()

	reply := &GetReply{}
	ok := srv.Call("ShardKV.Get", args, reply)
	if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
		done <- *reply
	} else if ok && reply.WrongLeader {
		go ck.tryGet(done, args)
	} else if ok && reply.Err == ErrWrongGroup {
		args.ConfigNum = ck.queryLatestConfig()
		go ck.tryGet(done, args)
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.id,
		Seq:       ck.seq,
		ConfigNum: ck.config.Num,
	}
	ck.seq++
	ck.mu.Unlock()

	done := make(chan PutAppendReply, 0)
	go ck.tryPutAppend(done, &args)
	for {
		select {
		case <-time.After(time.Duration(100) * time.Millisecond):
			args.ConfigNum = ck.queryLatestConfig()
			go ck.tryPutAppend(done, &args)
		case <-done:
			return
		}
	}
	return
}

func (ck *Clerk) tryPutAppend(done chan PutAppendReply, args *PutAppendArgs) {
	ck.mu.Lock()
	if ck.config.Num == 0 {
		ck.mu.Unlock()
		return // wait valid config
	}
	// select leader
	shard := key2shard(args.Key)
	gid := ck.config.Shards[shard]
	servers := ck.config.Groups[gid]
	ck.leader[shard] = (ck.leader[shard] + 1) % len(servers)
	server := servers[ck.leader[shard]]
	srv := ck.make_end(server)
	ck.mu.Unlock()

	reply := &PutAppendReply{}
	ok := srv.Call("ShardKV.PutAppend", args, reply)
	if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
		done <- *reply
	} else if ok && reply.WrongLeader {
		go ck.tryPutAppend(done, args)
	} else if ok && reply.Err == ErrWrongGroup {
		args.ConfigNum = ck.queryLatestConfig()
		go ck.tryPutAppend(done, args)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
