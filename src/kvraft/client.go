package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "time"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu     sync.Mutex
	id     int64
	seq    int64
	leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id, ck.seq = nrand(), 0
	return ck
}

func (ck *Clerk) getLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leader
}

func (ck *Clerk) setNextLeader() {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leader = (ck.leader + 1) % len(ck.servers)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	ck.seq += 1
	getArgs := GetArgs{Key: key, ClientId: ck.id, Seq: ck.seq}
	ck.mu.Unlock()

	done := make(chan GetReply, 0)
	go ck.tryGet(done, &getArgs)
	for {
		select {
		case <-time.After(time.Duration(500 * time.Millisecond)):
			ck.setNextLeader()
			go ck.tryGet(done, &getArgs)
		case reply := <-done:
			return reply.Value
		}
	}
}

func (ck *Clerk) tryGet(done chan GetReply, args *GetArgs) {
	leader := ck.getLeader()
	reply := &GetReply{}
	ck.servers[leader].Call("KVServer.Get", args, reply)
	if reply.Err == OK || reply.Err == ErrNoKey {
		done <- *reply
	} else if reply.WrongLeader {
		ck.setNextLeader()
		go ck.tryGet(done, args)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	ck.seq += 1
	putAppendArgs := PutAppendArgs{
		Key: key, Value: value, Op: op,
		ClientId: ck.id, Seq: ck.seq,
	}
	ck.mu.Unlock()

	done := make(chan struct{}, 0)
	go ck.tryPutAppend(done, &putAppendArgs)
loop:
	for {
		select {
		case <-time.After(time.Duration(500 * time.Millisecond)):
			ck.setNextLeader()
			go ck.tryPutAppend(done, &putAppendArgs)
		case <-done:
			break loop
		}
	}
}

func (ck *Clerk) tryPutAppend(done chan struct{}, args *PutAppendArgs) {
	leader := ck.getLeader()
	reply := &PutAppendReply{}
	ck.servers[leader].Call("KVServer.PutAppend", args, reply)
	if reply.Err == OK || reply.Err == ErrNoKey {
		done <- struct{}{}
	} else if reply.WrongLeader {
		ck.setNextLeader()
		go ck.tryPutAppend(done, args)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
