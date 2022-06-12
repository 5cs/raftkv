package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "bytes"
import "log"
import "math/rand"
import "sync"
import "sync/atomic"
import "time"

import "raftkv/labrpc"
import "raftkv/labgob"
import "raftkv/app"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	IsLeader     bool
}

// Log entry
type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

const (
	ElectionTimeOut       = 300
	ElectionTimeOutLength = 100
	RpcDue                = 50
	RpcDueLength          = 10
	Heartbeat             = 50
	HeatbeatLength        = 10
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state                  int
	currentTerm            int
	votedFor               int
	log                    []LogEntry
	commitIndex            int
	lastApplied            int
	nextIndex              []int
	matchIndex             []int
	refreshElectionTimeout chan struct{}
	applyCh                chan ApplyMsg
	killed                 bool
	app                    app.Applier // upper app
	lastExcludedIndex      int
	lastExcludedTerm       int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// Attach raft instance with upper applier App
func (rf *Raft) SetApp(app app.Applier) {
	rf.app = app
}

func (rf *Raft) GetLog() []LogEntry {
	return rf.log
}

func (rf *Raft) TruncateLog(i int) {
	if rf.Index(i) < 0 || rf.Index(i) >= len(rf.log) {
		return
	}
	lastExcludedLog := rf.log[rf.Index(i)]
	rf.log = rf.log[rf.Index(i)+1:]
	rf.lastExcludedTerm = lastExcludedLog.Term
	rf.lastExcludedIndex = i
	rf.persist()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastExcludedIndex)
	e.Encode(rf.lastExcludedTerm)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.lastExcludedIndex) != nil ||
		d.Decode(&rf.lastExcludedTerm) != nil ||
		d.Decode(&rf.log) != nil {
		panic("Persistent state is corrupted!")
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// compare logs of caller (args) and callee (rf)
//
func (rf *Raft) compareLog(args *RequestVoteArgs) bool {
	if rf.Len() == 0 {
		return true
	}
	var (
		lastLogIndex int = rf.lastExcludedIndex
		lastLogTerm  int = rf.lastExcludedTerm
	)
	index := rf.Index(rf.Len())
	if index >= 0 {
		lastLogTerm = rf.log[index].Term
		lastLogIndex = index + 1 + rf.lastExcludedIndex
	}
	if args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		return true
	}
	return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("RPC:RequestVote Callee id: %v term: %v votedFor: %v Caller id: %v, term: %v\n", rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term)
	defer DPrintf("=RPC:RequestVote Callee id: %v term: %v votedFor: %v Caller id: %v, term: %v\n", rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term)

	rf.mu.Lock()

	// < means outdated candidate, = && -1 means rf has seen this term
	// but has voted for this term
	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.votedFor != -1) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		// grant it when request has more update logs
		currentTerm := rf.currentTerm
		rf.currentTerm = args.Term
		reply.Term = args.Term
		if rf.compareLog(args) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			if rf.state == LEADER || rf.state == CANDIDATE {
				rf.state = FOLLOWER
			}
		} else {
			rf.votedFor = -1
			reply.VoteGranted = false
			// leader to follower
			if currentTerm < args.Term && (rf.state == LEADER || rf.state == CANDIDATE) {
				rf.state = FOLLOWER
			}
		}
	}

	rf.persist()
	rf.mu.Unlock()

	// reset timer
	if reply.VoteGranted {
		rf.refreshElectionTimeout <- struct{}{}
	}
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
	ReqId        int64
}

type AppendEntryReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

// AppendEntries RPC handler, given a term has at most one leader!
func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	DPrintf("RPC:AppendEntries Callee id: %v term: %v Caller id: %v term: %v len: %v prevLogTerm %v prevLogIndex %v commitIndex %v\n",
		rf.me, rf.currentTerm, args.LeaderId, args.Term, len(args.Entries), args.PrevLogTerm, args.PrevLogIndex, args.LeaderCommit)
	defer DPrintf("=RPC:AppendEntries Callee id: %v term: %v Caller id: %v term: %v len: %v prevLogTerm %v prevLogIndex %v commitIndex %v\n",
		rf.me, rf.currentTerm, args.LeaderId, args.Term, len(args.Entries), args.PrevLogTerm, args.PrevLogIndex, args.LeaderCommit)

	rf.mu.Lock()

	refresh := false
	if args.Term < rf.currentTerm { // caller is outdated leader!
		reply.Term = rf.currentTerm
		reply.Success = false
	} else { // args.Term == rf.currentTerm (may candidate) || args.Term > rf.currentTerm
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		if args.PrevLogIndex == 0 {
			reply.Success = true // no prev anymore
		} else if rf.Len() == 0 {
			reply.Success = false // has prev, but my log is empty
			reply.ConflictIndex = 1
		} else if rf.Len() != 0 && rf.Len() < args.PrevLogIndex {
			reply.Success = false
			reply.ConflictIndex = rf.Len()
		} else if rf.Len() != 0 && rf.Len() >= args.PrevLogIndex {
			prevLogIndex := rf.Index(args.PrevLogIndex)
			if prevLogIndex >= 0 {
				prevLog := rf.log[prevLogIndex]
				if prevLog.Term != args.PrevLogTerm {
					reply.Success = false
					j := prevLogIndex
					for j > 0 && rf.log[j].Term == prevLog.Term {
						j -= 1
					}
					reply.ConflictIndex = j + 1 + rf.lastExcludedIndex
				} else {
					reply.Success = true
				}
			} else if args.PrevLogIndex == rf.lastExcludedIndex &&
				args.PrevLogTerm == rf.lastExcludedTerm {
				reply.Success = true // snapshotted, prevlog match lastExcludedLog, reply true
			} else { // snapshotted, reply false, let leader do consistency check from lastExcludedIndex
				reply.Success = false
				reply.ConflictIndex = rf.lastExcludedIndex + 1
			}
		} else {
			panic("Can't happen!")
		}

		// update log state
		if reply.Success {
			// append entries
			if rf.Index(args.PrevLogIndex) >= -1 {
				rf.log = rf.log[:rf.Index(args.PrevLogIndex)+1]
			}
			rf.log = append(rf.log, args.Entries...)
			// update commitIndex
			N := rf.lastApplied
			if args.LeaderCommit < rf.Len() {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.Len()
			}
			for k := N + 1; k <= rf.commitIndex; k++ {
				if rf.Index(k) < 0 {
					continue
				}
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.Index(k)].Command,
					CommandIndex: k,
					IsLeader:     false,
				}
				DPrintf("Apply at append entries %v, %#v\n", rf.me, applyMsg)
				rf.applyCh <- applyMsg
				if rf.app != nil {
					rf.app.Apply(&applyMsg) // apply
				}
				rf.lastApplied = k
			}
			if rf.commitIndex >= N+1 {
				DPrintf("Raft %#v apply range:[%#v, %#v], role: %#v\n", rf.me, N+1, rf.commitIndex, rf.state)
			}
		}

		if rf.state == LEADER || rf.state == CANDIDATE {
			rf.state = FOLLOWER
		}
		if rf.state != LEADER {
			refresh = true
		}
	}

	rf.persist()
	rf.mu.Unlock()

	if refresh {
		rf.refreshElectionTimeout <- struct{}{}
	}
}

//
// InstallSnapshot
//
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	ReqId             int64
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("RPC:InstallSnapshot Callee id: %v term: %v Caller id: %v term: %v\n",
		rf.me, rf.currentTerm, args.LeaderId, args.Term)
	defer DPrintf("=RPC:InstallSnapshot Callee id: %v term: %v Caller id: %v term: %v\n",
		rf.me, rf.currentTerm, args.LeaderId, args.Term)

	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	if rf.lastExcludedIndex >= args.LastIncludedIndex { // invalid rpc req
		rf.mu.Unlock()
		return
	}

	if index := rf.Index(args.LastIncludedIndex); index >= 0 &&
		index < len(rf.log) && rf.log[index].Term == args.LastIncludedTerm {
		rf.log = rf.log[index+1:]
	} else {
		rf.log = make([]LogEntry, 0)
	}
	rf.lastApplied = args.LastIncludedIndex
	rf.lastExcludedIndex = args.LastIncludedIndex
	rf.lastExcludedTerm = args.LastIncludedTerm
	rf.persister.SaveSnapshot(args.Data)
	rf.persist()
	if rf.state == LEADER || rf.state == CANDIDATE {
		rf.state = FOLLOWER
	}
	if rf.app != nil {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      args,
			CommandIndex: -1,
			IsLeader:     rf.state == LEADER,
		}
		rf.app.Apply(&applyMsg)
	}
	refresh := rf.state != LEADER
	rf.mu.Unlock()
	if refresh {
		rf.refreshElectionTimeout <- struct{}{}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// sendAppendEntries
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// sendInstallSnapshot
//
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	index, term, isLeader := -1, -1, false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		if e := recover(); e != nil {
			index, term, isLeader = -1, -1, false
		}
	}()
	if rf.state == LEADER {
		term = rf.currentTerm
		index = rf.nextIndex[rf.me]
		isLeader = true
		rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
		rf.nextIndex[rf.me] += 1
		rf.matchIndex[rf.me] = rf.Len()
		DPrintf("Start at %v, term %v, index %v, command %#v\n", rf.me, term, index, command)
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.killed = true
}

func (rf *Raft) isKilled() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.killed
}

// Hold rf.mu
func (rf *Raft) Len() int {
	return len(rf.log) + rf.lastExcludedIndex
}

// Hold rf.mu
func (rf *Raft) Index(in int) int {
	return in - rf.lastExcludedIndex - 1
}

// Start timer for follower or candidate
func (rf *Raft) startTimer(timeOut chan struct{}) {
	var raftElectionTimeout int = ElectionTimeOut + rand.Intn(ElectionTimeOutLength)
loop:
	for {
		select {
		case <-rf.refreshElectionTimeout:
			raftElectionTimeout = ElectionTimeOut + rand.Intn(ElectionTimeOutLength)
		case <-time.After(time.Duration(raftElectionTimeout) * time.Millisecond):
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			if state != LEADER {
				select {
				case timeOut <- struct{}{}:
				default:
				}
			}
			break loop
		}
	}
}

//
// Follower loop and timer for candidate
//
func (rf *Raft) followerLoop(timeOut chan struct{}) {
	go rf.startTimer(timeOut) // timer for follower or candidate
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	if state == FOLLOWER {
		<-timeOut                 // waiting for follower to become candidate
		go rf.startTimer(timeOut) // timer for candidate
	}
}

//
// Candidate loop
//
func (rf *Raft) candidateLoop(timeOut chan struct{}) int {
	// Prepare RequestVote RPC parameters
	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist() // persist state
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	if rf.Len() == 0 {
		args.LastLogTerm = 0
		args.LastLogIndex = 0
	} else if rf.Len() != 0 {
		args.LastLogTerm = rf.lastExcludedTerm
		args.LastLogIndex = rf.Len()
		if rf.Index(rf.Len()) != -1 {
			args.LastLogTerm = rf.log[rf.Index(rf.Len())].Term
		}
	} else {
		panic("Can't happen!")
	}
	rf.mu.Unlock()

	currentTerm := args.Term
	grant := make(chan struct{}, 1)
	reject := make(chan struct{}, 1)
	candidateToFollower := make(chan struct{}, len(rf.peers))
	done := make([]bool, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me { // vote self
			continue
		}
		go func(i int) {
			shouldLeaveCandidateLoop := func() bool {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				return rf.state != CANDIDATE || rf.currentTerm != currentTerm || done[i]
			}
			// send request vote to other server
			for {
				if shouldLeaveCandidateLoop() {
					break
				}

				go func() {
					reply := RequestVoteReply{}
					ok := rf.sendRequestVote(i, &args, &reply)

					if shouldLeaveCandidateLoop() {
						return
					}
					rf.mu.Lock()
					if ok && reply.VoteGranted && currentTerm == rf.currentTerm {
						done[i] = true
						rf.mu.Unlock()
						grant <- struct{}{}
					} else if ok && !reply.VoteGranted && currentTerm == rf.currentTerm {
						done[i] = true
						if reply.Term > rf.currentTerm { // discover newer term
							rf.currentTerm = reply.Term
							rf.state = FOLLOWER
							rf.mu.Unlock()
						} else { // server i voted to other candidate at this term
							rf.mu.Unlock()
							reject <- struct{}{}
						}
					} else {
						// invalid response
						rf.mu.Unlock()
					}
				}()

				time.Sleep(time.Duration(RpcDue+rand.Intn(RpcDueLength)) * time.Millisecond)
			}

			rf.mu.Lock()
			if rf.state == FOLLOWER && rf.currentTerm == currentTerm {
				// transit to follower state
				rf.mu.Unlock()
				candidateToFollower <- struct{}{}
			} else if rf.state == LEADER {
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
			}
		}(i)
	}

	var (
		yes                   int  = 1
		no                    int  = 0
	)
	for {
		select {
		case <-grant:
			yes += 1
			if yes*2 > len(rf.peers) {
				return LEADER
			}
		case <-reject:
			no += 1
			if no*2 > len(rf.peers) {
				return FOLLOWER
			}
		case <-candidateToFollower:
			return FOLLOWER
		case <-timeOut: // waiting for candidate to become new candidate
			return CANDIDATE
		}
	}
}

//
// Leader loop
// send heartbeat infinitely under certain period interval,
// until rf.me transited to other state
//
func (rf *Raft) leaderLoop() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.Len() + 1
		rf.matchIndex[i] = 0
	}
	type LeaveLeaderLoopChan struct {
		leaderToFollower chan struct{}
		isFollower       int32
	}
	leaveLeaderLoopChan := LeaveLeaderLoopChan{
		leaderToFollower: make(chan struct{}),
		isFollower:       0,
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			shouldLeaveLeaderLoop := func() bool {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				return rf.killed || rf.state != LEADER || rf.currentTerm != currentTerm
			}

			for {
				if shouldLeaveLeaderLoop() {
					break
				}

				// send heartbeat periodically in async mode
				go func() {
					if shouldLeaveLeaderLoop() {
						return
					}

					rf.mu.Lock()
					tmpNextIndex := rf.Index(rf.nextIndex[i])
					if tmpNextIndex < 0 {
						// I don't have the log you want, install snapshot
						installSnapshotArgs := &InstallSnapshotArgs{}
						installSnapshotReply := &InstallSnapshotReply{}
						installSnapshotArgs.Term = currentTerm
						installSnapshotArgs.LeaderId = rf.me
						installSnapshotArgs.LastIncludedIndex = rf.lastExcludedIndex
						installSnapshotArgs.LastIncludedTerm = rf.lastExcludedTerm
						installSnapshotArgs.ReqId = nrand()
						installSnapshotArgs.Data = rf.persister.ReadSnapshot()
						rf.mu.Unlock()
						rf.sendInstallSnapshot(i, installSnapshotArgs, installSnapshotReply)
						rf.mu.Lock()
						if installSnapshotReply.Term > currentTerm {
							rf.currentTerm = installSnapshotReply.Term
							rf.state = FOLLOWER
						} else if installSnapshotReply.Term == currentTerm &&
							rf.Index(rf.nextIndex[i]) == tmpNextIndex {
							rf.nextIndex[i] = installSnapshotArgs.LastIncludedIndex + 1
						}
						rf.mu.Unlock()
						return
					}

					args := &AppendEntryArgs{
						Term:         currentTerm,
						LeaderId:     rf.me,
						LeaderCommit: rf.commitIndex,
						ReqId:        nrand(),
					}
					if rf.nextIndex[i] == 1 {
						args.PrevLogIndex = 0
						args.PrevLogTerm = 0
					} else if rf.nextIndex[i] >= 2 {
						if rf.nextIndex[i] > rf.Len()+1 {
							rf.nextIndex[i] = rf.Len() + 1
						}
						args.PrevLogIndex = rf.nextIndex[i] - 1
						args.PrevLogTerm = rf.lastExcludedTerm
						prevLogIndex := rf.Index(args.PrevLogIndex)
						if prevLogIndex >= 0 {
							args.PrevLogTerm = rf.log[prevLogIndex].Term
						}
					} else {
						panic("Can't happen!")
					}
					// each RPC send multiple log entries
					if rf.nextIndex[i] < rf.nextIndex[rf.me] {
						args.Entries = append(args.Entries, rf.log[rf.Index(rf.nextIndex[i]):]...)
					}
					rf.mu.Unlock()

					reply := &AppendEntryReply{}
					rf.sendAppendEntries(i, args, reply)

					if shouldLeaveLeaderLoop() {
						return
					}
					rf.mu.Lock()
					if !reply.Success && reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = FOLLOWER
					} else {
						// consistency check
						if reply.Success && rf.nextIndex[i]-1 == args.PrevLogIndex {
							rf.nextIndex[i] += len(args.Entries)
							if len(args.Entries) != 0 {
								rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
							}
							// update commitIndex
							N, has := rf.matchIndex[i], 0
							if N > rf.commitIndex && rf.log[rf.Index(N)].Term == rf.currentTerm {
								for j := range rf.peers {
									if rf.matchIndex[j] >= N {
										has += 1
									}
								}
								if has*2 > len(rf.peers) {
									for k := rf.commitIndex + 1; k <= N; k++ {
										if rf.Index(k) < 0 {
											continue
										}
										applyMsg := ApplyMsg{
											CommandValid: true,
											Command:      rf.log[rf.Index(k)].Command,
											CommandIndex: k,
											IsLeader:     true,
										}
										DPrintf("Apply at leader loop %v, %#v\n", rf.me, applyMsg)
										rf.applyCh <- applyMsg
										if rf.app != nil {
											rf.app.Apply(&applyMsg) // apply
										}
										rf.lastApplied = k
									}
									if N >= rf.commitIndex+1 {
										DPrintf("Raft %#v apply range: [%#v, %#v], role: %#v\n", rf.me, rf.commitIndex+1, N, rf.state)
									}
									rf.commitIndex = N
								}
							}
						} else if !reply.Success &&
							rf.nextIndex[i]-1 == args.PrevLogIndex && reply.ConflictIndex != 0 {
							rf.nextIndex[i] = reply.ConflictIndex
						} else {
							// {0, false}, or duplicated replies
						}
					}
					rf.mu.Unlock()
				}()

				time.Sleep(time.Duration(Heartbeat+rand.Intn(HeatbeatLength)) * time.Millisecond)
			}

			if shouldLeaveLeaderLoop() {
				if atomic.CompareAndSwapInt32(&leaveLeaderLoopChan.isFollower, 0, 1) {
					leaveLeaderLoopChan.leaderToFollower <- struct{}{}
				}
			}
		}(i)
	}

	<-leaveLeaderLoopChan.leaderToFollower
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	// log.SetFlags(log.LstdFlags | log.Llongfile | log.Lmicroseconds)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.refreshElectionTimeout = make(chan struct{}, 1)
	rf.applyCh = applyCh
	rf.killed = false
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rand.Seed(time.Now().UnixNano())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			if rf.isKilled() {
				break
			}
			timeOut := make(chan struct{}, 1)
			rf.followerLoop(timeOut)

			if rf.isKilled() {
				break
			}
			state := rf.candidateLoop(timeOut)

			rf.mu.Lock()
			rf.state = state
			rf.mu.Unlock()
			if state != LEADER {
				continue
			}
			if rf.app != nil {
				DPrintf("Raft leader at %#v is %#v, %#v\n", rf.currentTerm, rf.me, rf.app.Name())
			} else {
				DPrintf("Raft leader at %#v is %#v\n", rf.currentTerm, rf.me)
			}
			rf.leaderLoop()
		}
	}()

	return rf
}
