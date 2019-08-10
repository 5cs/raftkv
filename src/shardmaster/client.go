package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.id = nrand()
	ck.seq = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	ck.mu.Lock()
	args := &QueryArgs{
		Num:      num,
		ClientId: ck.id,
		Seq:      ck.seq,
	}
	ck.seq++
	ck.mu.Unlock()

	done := make(chan QueryReply, 0)
	go ck.tryQuery(done, args)
	for {
		select {
		case <-time.After(100 * time.Millisecond):
			ck.mu.Lock()
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.mu.Unlock()
			go ck.tryQuery(done, args)
		case reply := <-done:
			return reply.Config
		}
	}
}
func (ck *Clerk) tryQuery(done chan QueryReply, args *QueryArgs) {
	reply := &QueryReply{}
	ck.servers[ck.leader].Call("ShardMaster.Query", args, reply)
	if reply.Err == OK || reply.Err == ErrNotFound {
		done <- *reply
	} else if reply.WrongLeader {
		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()
		go ck.tryQuery(done, args)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	ck.mu.Lock()
	args := &JoinArgs{
		Servers:  servers,
		ClientId: ck.id,
		Seq:      ck.seq,
	}
	ck.seq++
	ck.mu.Unlock()

	done := make(chan JoinReply, 0)
	go ck.tryJoin(done, args)
	for {
		select {
		case <-time.After(100 * time.Millisecond):
			ck.mu.Lock()
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.mu.Unlock()
			go ck.tryJoin(done, args)
		case <-done:
			return
		}
	}
}
func (ck *Clerk) tryJoin(done chan JoinReply, args *JoinArgs) {
	reply := &JoinReply{}
	ck.servers[ck.leader].Call("ShardMaster.Join", args, reply)
	if reply.Err == OK {
		done <- *reply
	} else if reply.WrongLeader {
		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()
		go ck.tryJoin(done, args)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	ck.mu.Lock()
	args := &LeaveArgs{
		GIDs:     gids,
		ClientId: ck.id,
		Seq:      ck.seq,
	}
	ck.seq++
	ck.mu.Unlock()

	done := make(chan LeaveReply, 0)
	go ck.tryLeave(done, args)
	for {
		select {
		case <-time.After(100 * time.Millisecond):
			ck.mu.Lock()
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.mu.Unlock()
			go ck.tryLeave(done, args)
		case <-done:
			return
		}
	}
}
func (ck *Clerk) tryLeave(done chan LeaveReply, args *LeaveArgs) {
	reply := &LeaveReply{}
	ck.servers[ck.leader].Call("ShardMaster.Leave", args, reply)
	if reply.Err == OK {
		done <- *reply
	} else if reply.WrongLeader {
		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()
		go ck.tryLeave(done, args)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	ck.mu.Lock()
	args := &MoveArgs{
		Shard:    shard,
		GID:      gid,
		ClientId: ck.id,
		Seq:      ck.seq,
	}
	ck.seq++
	ck.mu.Unlock()

	done := make(chan MoveReply, 0)
	go ck.tryMove(done, args)
	for {
		select {
		case <-time.After(100 * time.Millisecond):
			ck.mu.Lock()
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.mu.Unlock()
			go ck.tryMove(done, args)
		case <-done:
			return
		}
	}
}
func (ck *Clerk) tryMove(done chan MoveReply, args *MoveArgs) {
	reply := &MoveReply{}
	ck.servers[ck.leader].Call("ShardMaster.Move", args, reply)
	if reply.Err == OK {
		done <- *reply
	} else if reply.WrongLeader {
		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()
		go ck.tryMove(done, args)
	}
}
