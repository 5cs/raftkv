package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "labrpc"
import "crypto/rand"
import "math/big"
import "shardmaster"
import "time"
import "sync"
import "fmt"

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
	mu      sync.Mutex
	id      int64
	seq     int64
	leaders map[int]int
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
	ck.leaders = map[int]int{}
	for shard := 0; shard < len(ck.config.Shards); shard++ {
		ck.leaders[shard] = 0
	}
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	shard := key2shard(key)
	ck.mu.Lock()
	args := GetArgs{
		Key:       key,
		ClientId:  ck.id,
		Seq:       ck.seq,
		ConfigNum: ck.config.Num,
		Shard:     shard,
	}
	ck.seq++
	ck.mu.Unlock()

	done := make(chan GetReply, 0)
	go ck.tryGet(done, &args, shard)
	for {
		select {
		case <-time.After(100 * time.Millisecond):
			ck.mu.Lock()
			ck.config = ck.sm.Query(-1)
			if ck.config.Num != 0 {
				ck.leaders[shard] = (ck.leaders[shard] + 1) % len(ck.config.Groups[ck.config.Shards[shard]])
			}
			ck.mu.Unlock()
			go ck.tryGet(done, &args, shard)
		case reply := <-done:
			return reply.Value
		}
	}

	return ""
}
func (ck *Clerk) tryGet(done chan GetReply, args *GetArgs, shard int) {
	if ck.config.Num == 0 {
		return
	}
	reply := &GetReply{}
	ck.mu.Lock()
	srv := ck.make_end(ck.config.Groups[ck.config.Shards[shard]][ck.leaders[shard]])
	args.ConfigNum = ck.config.Num
	ck.mu.Unlock()
	ok := srv.Call("ShardKV.Get", args, reply)
	if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
		done <- *reply
	} else if ok && reply.WrongLeader {
		ck.mu.Lock()
		ck.leaders[shard] = (ck.leaders[shard] + 1) % len(ck.config.Groups[ck.config.Shards[shard]])
		ck.mu.Unlock()
		go ck.tryGet(done, args, shard)
	} else if ok && (reply.Err == ErrWrongGroup) {
		ck.mu.Lock()
		ck.config = ck.sm.Query(-1)
		// args.ConfigNum = ck.config.Num
		ck.mu.Unlock()
		go ck.tryGet(done, args, shard)
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	shard := key2shard(key)
	ck.mu.Lock()
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.id,
		Seq:       ck.seq,
		ConfigNum: ck.config.Num,
		Shard:     shard,
	}
	ck.seq++
	ck.mu.Unlock()

	done := make(chan PutAppendReply, 0)
	go ck.tryPutAppend(done, &args, shard)
	for {
		select {
		case <-time.After(100 * time.Millisecond):
			ck.mu.Lock()
			ck.config = ck.sm.Query(-1)
			if ck.config.Num != 0 {
				ck.leaders[shard] = (ck.leaders[shard] + 1) % len(ck.config.Groups[ck.config.Shards[shard]])
			}
			ck.mu.Unlock()
			go ck.tryPutAppend(done, &args, shard)
		case <-done:
			return
		}
	}
	return
}
func (ck *Clerk) tryPutAppend(done chan PutAppendReply, args *PutAppendArgs, shard int) {
	if ck.config.Num == 0 {
		return
	}
	reply := &PutAppendReply{}
	ck.mu.Lock()
	srv := ck.make_end(ck.config.Groups[ck.config.Shards[shard]][ck.leaders[shard]])
	args.ConfigNum = ck.config.Num
	rpcLeader := ck.config.Groups[ck.config.Shards[shard]][ck.leaders[shard]]
	configNum := args.ConfigNum
	ck.mu.Unlock()
	ok := srv.Call("ShardKV.PutAppend", args, reply)
	if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
		fmt.Printf("Key-Value: %#v-%#v Served by %#v at config %#v with client:seq %#v:%#v\n", args.Key, args.Value, rpcLeader, configNum, args.ClientId, args.Seq)
		done <- *reply
	} else if ok && reply.WrongLeader {
		ck.mu.Lock()
		ck.leaders[shard] = (ck.leaders[shard] + 1) % len(ck.config.Groups[ck.config.Shards[shard]])
		ck.mu.Unlock()
		go ck.tryPutAppend(done, args, shard)
	} else if ok && reply.Err == ErrWrongGroup {
		ck.mu.Lock()
		ck.config = ck.sm.Query(-1)
		// args.ConfigNum = ck.config.Num
		ck.mu.Unlock()
		go ck.tryPutAppend(done, args, shard)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
