package shardmaster

import "raft"
import "labrpc"
import "sync"
import "labgob"
import "fmt"
import "log"
import "sort"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs     []Config // keyed by config num
	configNum   int
	clientSeqs  map[int64]int64
	notices     map[int]*sync.Cond
	appliedCmds map[int]*appliedResult
}

type appliedResult struct {
	Key    string
	Result interface{}
}

type Op struct {
	// Your data here.
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	cmd := *args
	for {
		*reply = JoinReply{}
		index, _, isLeader := sm.rf.Start(cmd)
		if !isLeader {
			reply.WrongLeader = true
			return
		}

		sm.mu.Lock()
		if _, ok := sm.notices[index]; !ok {
			sm.notices[index] = sync.NewCond(&sm.mu)
		}
		sm.notices[index].Wait()

		ret := sm.appliedCmds[index]
		key := fmt.Sprintf("%v_join_%v", cmd.ClientId, cmd.Seq)
		if ret.Key == key {
			switch ret.Result.(type) {
			case JoinReply:
				*reply = ret.Result.(JoinReply)
				sm.mu.Unlock()
				return
			}
		}
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	cmd := *args
	for {
		*reply = LeaveReply{}
		index, _, isLeader := sm.rf.Start(cmd)
		if !isLeader {
			reply.WrongLeader = true
			return
		}

		sm.mu.Lock()
		if _, ok := sm.notices[index]; !ok {
			sm.notices[index] = sync.NewCond(&sm.mu)
		}
		sm.notices[index].Wait()

		ret := sm.appliedCmds[index]
		key := fmt.Sprintf("%v_leave_%v", cmd.ClientId, cmd.Seq)
		if ret.Key == key {
			switch ret.Result.(type) {
			case LeaveReply:
				*reply = ret.Result.(LeaveReply)
				sm.mu.Unlock()
				return
			}
		}
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	cmd := *args
	for {
		*reply = MoveReply{}
		index, _, isLeader := sm.rf.Start(cmd)
		if !isLeader {
			reply.WrongLeader = true
			return
		}

		sm.mu.Lock()
		if _, ok := sm.notices[index]; !ok {
			sm.notices[index] = sync.NewCond(&sm.mu)
		}
		sm.notices[index].Wait()

		ret := sm.appliedCmds[index]
		key := fmt.Sprintf("%v_move_%v", cmd.ClientId, cmd.Seq)
		if ret.Key == key {
			switch ret.Result.(type) {
			case MoveReply:
				*reply = ret.Result.(MoveReply)
				sm.mu.Unlock()
				return
			}
		}
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	cmd := *args
	for {
		*reply = QueryReply{}
		index, _, isLeader := sm.rf.Start(cmd)
		if !isLeader {
			reply.WrongLeader = true
			return
		}

		sm.mu.Lock()
		if _, ok := sm.notices[index]; !ok {
			sm.notices[index] = sync.NewCond(&sm.mu)
		}
		sm.notices[index].Wait()

		ret := sm.appliedCmds[index]
		key := fmt.Sprintf("%v_query_%v", cmd.ClientId, cmd.Seq)
		if ret.Key == key {
			switch ret.Result.(type) {
			case QueryReply:
				*reply = ret.Result.(QueryReply)
				sm.mu.Unlock()
				return
			}
		}
		sm.mu.Unlock()
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) doAssert(one *Config) {
	loadMap := map[int]int{}
	for _, shard := range one.Shards {
		loadMap[shard] += 1
	}
	for _, shard := range one.Shards {
		if _, ok := one.Groups[shard]; !ok {
			log.Fatal("Failed\n")
		}
	}

	c := one
	// more or less balanced sharding?
	counts := map[int]int{}
	for _, g := range c.Shards {
		counts[g] += 1
	}
	min := 257
	max := 0
	for g, _ := range c.Groups {
		if counts[g] > max {
			max = counts[g]
		}
		if counts[g] < min {
			min = counts[g]
		}
	}
	if max > min+1 {
		log.Fatalf("max %v too much larger than min %v", max, min)
	}
}

func (sm *ShardMaster) doQuery(index int, args QueryArgs) *appliedResult {
	var (
		ret       *appliedResult
		maxConfig *appliedResult
		max       int = -1
	)

	key := fmt.Sprintf("%v_query_%v", args.ClientId, args.Seq)
	for i := range sm.configs {
		if num := sm.configs[i].Num; num == args.Num {
			ret = &appliedResult{
				Key: key,
				Result: QueryReply{
					WrongLeader: false,
					Err:         OK,
					Config:      sm.configs[i],
				},
			}
			break
		} else if num > max {
			maxConfig = &appliedResult{
				Key: key,
				Result: QueryReply{
					WrongLeader: false,
					Err:         OK,
					Config:      sm.configs[i],
				},
			}
			max = num
		}
	}

	if ret == nil {
		if args.Num == -1 || args.Num > max {
			ret = maxConfig
		} else {
			ret = &appliedResult{
				Key: key,
				Result: QueryReply{
					WrongLeader: false,
					Err:         ErrNotFound,
				},
			}
		}
	}

	return ret
}

func (sm *ShardMaster) doJoin(index int, args JoinArgs) *appliedResult {
	logs := sm.rf.GetLog()
	Key := fmt.Sprintf("%v_join_%v", args.ClientId, args.Seq)
	ret := appliedResult{
		Key:    Key,
		Result: JoinReply{WrongLeader: false, Err: OK},
	}
	for i := 1; i < index; i++ {
		logEntry := logs[sm.rf.Index(i)]
		switch logEntry.Command.(type) {
		case JoinArgs:
			cmd := logEntry.Command.(JoinArgs)
			key := fmt.Sprintf("%v_join_%v", cmd.ClientId, cmd.Seq)
			if key == Key { // applied
				return &ret
			}
		default:
		}
	}

	// newest config
	var (
		config *Config
		max    int = -1
		gids   []int
	)
	for i := range sm.configs {
		if max < sm.configs[i].Num {
			config = &sm.configs[i]
			max = sm.configs[i].Num
		}
	}
	sm.configNum++
	one := Config{
		Num:    sm.configNum,
		Groups: map[int][]string{},
	}
	if config != nil {
		for k, v := range config.Groups {
			one.Groups[k] = make([]string, len(v))
			copy(one.Groups[k], v)
			gids = append(gids, k)
		}
		for i, shard := range config.Shards {
			one.Shards[i] = shard
		}
	}
	for k, v := range args.Servers {
		one.Groups[k] = v
		gids = append(gids, k)
	}

	// reshards
	loadMap := map[int]int{}
	gid2Shards := map[int][]int{}
	nonZeroGid := 0
	for _, gid := range gids {
		if gid != 0 {
			loadMap[gid] = 0
			nonZeroGid = gid
		}
	}
	for i := 0; i < NShards; i++ {
		if one.Shards[i] == 0 {
			one.Shards[i] = nonZeroGid
		}
	}
	for i := 0; i < NShards; i++ {
		loadMap[one.Shards[i]] += 1
		gid2Shards[one.Shards[i]] = append(gid2Shards[one.Shards[i]], i)
	}

	DPrintf("#args: %#v\n", args)
	DPrintf("reshard(join): %#v, %#v, %#v\n", gids, gid2Shards, config.Shards)

	for {
		maxGid, minGid := nonZeroGid, nonZeroGid
		maxLoad, minLoad := 0, 257
		for gid, load := range loadMap {
			if load > maxLoad {
				maxGid, maxLoad = gid, load
			}
			if load < minLoad {
				minGid, minLoad = gid, load
			}
		}
		if maxLoad <= minLoad+1 {
			break
		}
		gid2Shards[minGid] = append(gid2Shards[minGid], gid2Shards[maxGid][0])
		gid2Shards[maxGid] = gid2Shards[maxGid][1:]
		loadMap[minGid] += 1
		loadMap[maxGid] -= 1
	}
	for gid, shards := range gid2Shards {
		for _, shard := range shards {
			one.Shards[shard] = gid
		}
	}
	sm.configs = append(sm.configs, one)

	DPrintf("=reshard(join): %#v, %#v, %#v\n", gids, gid2Shards, one.Shards)
	// sm.doAssert(&one)

	return &ret
}

func (sm *ShardMaster) doLeave(index int, args LeaveArgs) *appliedResult {
	logs := sm.rf.GetLog()
	Key := fmt.Sprintf("%v_leave_%v", args.ClientId, args.Seq)
	ret := appliedResult{
		Key:    Key,
		Result: LeaveReply{WrongLeader: false, Err: OK},
	}

	for i := 1; i < index; i++ {
		logEntry := logs[sm.rf.Index(i)]
		switch logEntry.Command.(type) {
		case LeaveArgs:
			cmd := logEntry.Command.(LeaveArgs)
			key := fmt.Sprintf("%v_leave_%v", cmd.ClientId, cmd.Seq)
			if key == Key { // applied
				return &ret
			}
		default:
		}
	}

	// newest config
	var (
		config *Config
		max    int = -1
		gids   []int
	)
	for i := range sm.configs {
		if max < sm.configs[i].Num {
			config = &sm.configs[i]
			max = sm.configs[i].Num
		}
	}
	sm.configNum++
	one := Config{
		Num:    sm.configNum,
		Groups: map[int][]string{},
	}
	if config != nil {
	outLoop:
		for k, v := range config.Groups {
			for _, leavedGid := range args.GIDs {
				if k == leavedGid {
					continue outLoop
				}
			}
			one.Groups[k] = make([]string, len(v))
			copy(one.Groups[k], v)
			gids = append(gids, k)
		}
		for i, shard := range config.Shards {
			one.Shards[i] = shard
		}
	}
	// reshards
	if len(gids) == 0 { // no gids anymore
		for i := 0; i < NShards; i++ {
			one.Shards[i] = 0
		}
		one.Groups = map[int][]string{}
		sm.configs = append(sm.configs, one)
		return &ret
	}

	loadMap0, loadMap1 := map[int]int{}, map[int]int{}
	for _, gid := range gids {
		loadMap0[gid] = 0
	}
	for _, gid := range args.GIDs {
		loadMap1[gid] = 0
	}
	gid2Shards := map[int][]int{}
	for i := 0; i < NShards; i++ {
		if _, ok := loadMap0[one.Shards[i]]; ok {
			loadMap0[one.Shards[i]] += 1
		} else if _, ok1 := loadMap1[one.Shards[i]]; ok1 {
			loadMap1[one.Shards[i]] += 1
		} else {
			log.Fatalf("Can not happend\n")
		}
		gid2Shards[one.Shards[i]] = append(gid2Shards[one.Shards[i]], i)
	}
	type Load struct {
		gid  int
		load int
	}
	loadArray0, loadArray1 := []Load{}, []Load{}
	meanLoad := NShards / len(gids)
	if meanLoad == 0 {
		meanLoad = 1
	}
	for k, v := range loadMap0 {
		loadArray0 = append(loadArray0, Load{gid: k, load: v})
	}
	for k, v := range loadMap1 {
		loadArray1 = append(loadArray1, Load{gid: k, load: v})
	}
	sort.Slice(loadArray0, func(i, j int) bool {
		return loadArray0[i].load > loadArray0[j].load
	})
	sort.Slice(loadArray1, func(i, j int) bool {
		return loadArray1[i].load < loadArray1[j].load
	})

	DPrintf("#args: %#v\n", args)
	DPrintf("reshard(leave): %#v,\n %#v,\n %v\n", gids, gid2Shards, config.Groups)

	i, j := 0, 0
	for i < len(loadArray0) && j < len(loadArray1) {
		l, r := &loadArray0[i], &loadArray1[j]
		if l.load >= meanLoad {
			i++
			continue
		}
		if r.load == 0 {
			j++
			continue
		}

		var (
			moved int
			need  int = meanLoad - l.load
			has   int = r.load
		)
		if has >= need {
			moved = need
		} else {
			moved = has
		}

		DPrintf("from %#v to %#v, move(leave): %#v\n", r.gid, l.gid, moved)

		l.load += moved
		r.load -= moved
		gid2Shards[l.gid] = append(gid2Shards[l.gid], gid2Shards[r.gid][:moved]...)
		gid2Shards[r.gid] = gid2Shards[r.gid][moved:]
	}
	for gid, shards := range gid2Shards {
		for _, shard := range shards {
			one.Shards[shard] = gid
		}
	}
	sm.configs = append(sm.configs, one)

	DPrintf("=reshard(leave): %#v, %#v, %#v\n\n", gids, gid2Shards, one.Shards)
	// sm.doAssert(&one)

	return &ret
}

func (sm *ShardMaster) doMove(index int, args MoveArgs) *appliedResult {
	logs := sm.rf.GetLog()
	Key := fmt.Sprintf("%v_move_%v", args.ClientId, args.Seq)
	ret := appliedResult{
		Key:    Key,
		Result: MoveReply{WrongLeader: false, Err: OK},
	}

	for i := 1; i < index; i++ {
		logEntry := logs[sm.rf.Index(i)]
		switch logEntry.Command.(type) {
		case MoveArgs:
			cmd := logEntry.Command.(MoveArgs)
			key := fmt.Sprintf("%v_leave_%v", cmd.ClientId, cmd.Seq)
			if key == Key { // applied
				return &ret
			}
		default:
		}
	}

	// newest config
	var (
		config *Config
		max    int = -1
		gids   []int
	)
	for i := range sm.configs {
		if max < sm.configs[i].Num {
			config = &sm.configs[i]
			max = sm.configs[i].Num
		}
	}
	sm.configNum++
	one := Config{
		Num:    sm.configNum,
		Groups: map[int][]string{},
	}
	if config != nil {
		for k, v := range config.Groups {
			one.Groups[k] = make([]string, len(v))
			copy(one.Groups[k], v)
			gids = append(gids, k)
		}
		for i, shard := range config.Shards {
			one.Shards[i] = shard
		}
	}
	// reshards
	gid2Shards := map[int][]int{}
	for i := 0; i < NShards; i++ {
		gid2Shards[one.Shards[i]] = append(gid2Shards[one.Shards[i]], i)
	}

	DPrintf("#args: %#v\n", args)
	DPrintf("reshard(move): %#v, %#v, %#v\n", gids, gid2Shards, config.Shards)

	fromGid := one.Shards[args.Shard]
	fromShards := []int{}
	for _, shard := range gid2Shards[fromGid] {
		if shard == args.Shard {
			continue
		}
		fromShards = append(fromShards, shard)
	}
	if one.Shards[args.Shard] != args.GID {
		gid2Shards[args.GID] = append(gid2Shards[args.GID], args.Shard)
		gid2Shards[fromGid] = fromShards
		maxLoadGid, maxLoad := -1, -1
		for gid, _ := range gid2Shards {
			if len(gid2Shards[gid]) > maxLoad {
				maxLoad = len(gid2Shards[gid])
				maxLoadGid = gid
			}
		}
		gid2Shards[fromGid] = append(gid2Shards[fromGid], gid2Shards[maxLoadGid][0])
		gid2Shards[maxLoadGid] = gid2Shards[maxLoadGid][1:]

		for gid, shards := range gid2Shards {
			for _, shard := range shards {
				one.Shards[shard] = gid
			}
		}
	}
	sm.configs = append(sm.configs, one)

	DPrintf("=reshard(move): %#v, %#v, %#v\n", gids, gid2Shards, one.Shards)
	// sm.doAssert(&one)

	return &ret
}

func (sm *ShardMaster) Name() string {
	return fmt.Sprintf("shardmaster %#v", sm.me)
}

func (sm *ShardMaster) Apply(applyMsg interface{}) {
	msg := applyMsg.(*raft.ApplyMsg)
	index := msg.CommandIndex
	cmd := msg.Command
	sm.mu.Lock()
	switch cmd.(type) {
	case QueryArgs:
		sm.appliedCmds[index] = sm.doQuery(index, cmd.(QueryArgs))
	case JoinArgs:
		sm.appliedCmds[index] = sm.doJoin(index, cmd.(JoinArgs))
	case LeaveArgs:
		sm.appliedCmds[index] = sm.doLeave(index, cmd.(LeaveArgs))
	case MoveArgs:
		sm.appliedCmds[index] = sm.doMove(index, cmd.(MoveArgs))
	default:
	}
	sm.mu.Unlock()
	if _, ok := sm.notices[index]; ok {
		sm.notices[index].Broadcast()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(QueryArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.rf.SetApp(sm)
	sm.clientSeqs = make(map[int64]int64)
	sm.notices = make(map[int]*sync.Cond)
	sm.appliedCmds = make(map[int]*appliedResult)

	// server loop
	go func() {
		for applyMsg := range sm.applyCh {
			DPrintf("App:apply at %v, %#v", sm.me, applyMsg)
		}
	}()

	return sm
}
