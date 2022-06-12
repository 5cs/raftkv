package shardkv

import "raftkv/shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	Seq       int64
	ConfigNum int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	Seq       int64
	ConfigNum int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type MigrateShardArgs struct {
	Shards     []int
	ConfigNum  int
	ClientId   int64
	Seq        int64
	ClientName string
}

type MigrateShardReply struct {
	Data        []map[string]string
	ClientSeqs  map[int64]int64
	ConfigNum   int
	Err         Err
	WrongLeader bool
}

type SyncShardArgs struct {
	Data       []map[string]string
	ClientSeqs map[int64]int64
	Shards     []int
	ConfigNum  int                // prev config num
	Config     shardmaster.Config // new config
	ClientId   int64
	Seq        int64
	ClientName string
}

type SyncShardReply struct {
	Err         Err
	WrongLeader bool
}

type InstallConfigArgs struct {
	Config     shardmaster.Config
	ClientId   int64
	ClientName string
}

type InstallConfigReply struct {
	Err         Err
	WrongLeader bool
}
