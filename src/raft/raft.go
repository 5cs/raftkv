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

import "sync"
import "labrpc"

import "math/rand"
import "time"
import "log"

// import "bytes"
// import "labgob"

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
	START           = 300
	LENGTH          = 100
	RPCDUE          = 50
	RPCDULENGTH     = 10
	HEARTBEAT       = 50
	HEARTBEATLENGTH = 10
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
	timeOut                chan struct{}
	applyCh                chan ApplyMsg
	killed                 bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = false
	if rf.state == LEADER {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
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
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
func moreUpdateLog(args *RequestVoteArgs, rf *Raft) bool {
	res := false
	if len(rf.log) == 0 {
		res = true
	} else if len(rf.log) != 0 {
		iLastLogIndex := len(rf.log) - 1
		last := rf.log[iLastLogIndex]
		if args.LastLogTerm > last.Term ||
			(args.LastLogTerm == last.Term && args.LastLogIndex >= iLastLogIndex+1) {
			res = true
		}
	} else {
		log.Fatal("Can not happend!")
	}
	return res
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	log.Println("RPC handler: Raft.RequestVote",
		"\nCallee", "id:", rf.me, "term:", rf.currentTerm, "votedFor:", rf.votedFor,
		"\nCaller", "id:", args.CandidateId, "term:", args.Term)

	// < means outdated candidate, = && -1 means rf has seen this term
	// but has voted for this term
	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.votedFor != -1) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		// grant it when request has more update logs
		// currentTerm := rf.currentTerm
		rf.currentTerm = args.Term
		reply.Term = args.Term
		if moreUpdateLog(args, rf) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			if rf.state == LEADER || rf.state == CANDIDATE {
				rf.state = FOLLOWER
			}
		} else {
			rf.votedFor = -1
			reply.VoteGranted = false
		}
	}

	log.Println("=====RPC handler: Raft.RequestVote",
		"\nCallee", "id:", rf.me, "term:", rf.currentTerm, "votedFor:", rf.votedFor,
		"\nCaller", "id:", args.CandidateId, "term:", args.Term)

	// reset timer
	if reply.VoteGranted && (rf.state == FOLLOWER || rf.state == CANDIDATE) {
		rf.mu.Unlock()
		rf.refreshElectionTimeout <- struct{}{}
	} else {
		rf.mu.Unlock()
	}
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC handler, given a term has at most one leader!
func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()

	log.Println("RPC handler: Raft.AppendEntries",
		"\nCallee", "id:", rf.me, "term:", rf.currentTerm,
		"\nCaller", "id:", args.LeaderId, "term:", args.Term, "entries:", args.Entries,
		"prevLogTerm", args.PrevLogTerm, "prevLogIndex", args.PrevLogIndex, "commitIndex", args.LeaderCommit)

	refresh := false

	if args.Term < rf.currentTerm { // caller is outdated leader!
		reply.Term = rf.currentTerm
		reply.Success = false
	} else { // args.Term == rf.currentTerm (may candidate) || args.Term > rf.currentTerm
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		if len(rf.log) == 0 {
			reply.Success = true
		} else if len(rf.log) != 0 && len(rf.log) < args.PrevLogIndex {
			reply.Success = false
		} else if len(rf.log) != 0 && len(rf.log) >= args.PrevLogIndex {
			iPrevLogIndex := args.PrevLogIndex - 1
			prevLog := rf.log[iPrevLogIndex]
			if prevLog.Term != args.PrevLogTerm {
				reply.Success = false
			} else if prevLog.Term == args.PrevLogTerm {
				reply.Success = true
				// truncate unmatch log entries
				iCurLogIndex := args.PrevLogIndex
				if len(args.Entries) != 0 &&
					iCurLogIndex < len(rf.log) &&
					args.Entries[0].Term != rf.log[iCurLogIndex].Term {
					rf.log = rf.log[:iCurLogIndex]
				}
			} else {
				log.Fatal("Can not happend!")
			}
		} else {
			log.Fatal("Can not happend!")
		}

		// update log state
		if reply.Success {
			// append entries
			i, c := args.PrevLogIndex, 0
			for i < len(rf.log) && c < len(args.Entries) {
				i, c = i+1, c+1
			}
			rf.log = append(rf.log, args.Entries[c:]...)
			// update commitIndex
			iLastLogIndex, N := len(rf.log)-1, rf.commitIndex
			if args.LeaderCommit < iLastLogIndex+1 {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = iLastLogIndex + 1
			}
			for k := N + 1; k <= rf.commitIndex; k++ {
				applyMsg := ApplyMsg{CommandValid: true,
					Command:      rf.log[k-1].Command,
					CommandIndex: k,
				}
				rf.applyCh <- applyMsg
			}
		}

		if rf.state == LEADER || rf.state == CANDIDATE {
			rf.state = FOLLOWER
		}
		if rf.state != LEADER {
			refresh = true
		}
	}

	log.Println("=====RPC handler: Raft.AppendEntries",
		"\nCallee", "id:", rf.me, "term:", rf.currentTerm,
		"\nCaller", "id:", args.LeaderId, "term:", args.Term, "entries:", args.Entries)

	if refresh {
		rf.mu.Unlock()
		rf.refreshElectionTimeout <- struct{}{}
	} else {
		rf.mu.Unlock()
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
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	if rf.state == LEADER {
		term = rf.currentTerm
		index = rf.nextIndex[rf.me]
		isLeader = true
		rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
		rf.nextIndex[rf.me] += 1
		rf.matchIndex[rf.me] = len(rf.log)
	}
	rf.mu.Unlock()

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
	rf.killed = true
	rf.mu.Unlock()
}

func stateName(state int) string {
	if state == FOLLOWER {
		return "follower"
	} else if state == CANDIDATE {
		return "candidate"
	} else if state == LEADER {
		return "leader"
	} else {
		return "?????"
	}
}

//
// Follower loop and timer for candidate
//
func (rf *Raft) followerLoop() {
	// reset election timeout
	var raftElectionTimeout int = START + rand.Intn(LENGTH)
	go func() {
	loopTimeOut:
		for {
			select {
			case <-rf.refreshElectionTimeout:
				raftElectionTimeout = START + rand.Intn(LENGTH)
			case <-time.After(time.Duration(raftElectionTimeout) * time.Millisecond):
				rf.mu.Lock()
				if rf.state != LEADER {
					rf.mu.Unlock()
					rf.timeOut <- struct{}{}
				} else {
					rf.mu.Unlock()
				}
				break loopTimeOut
			}
		}
	}()
	rf.mu.Lock()
	if rf.state == FOLLOWER {
		rf.mu.Unlock()
		<-rf.timeOut // drain channel, let timeOut kick again
		var raftElectionTimeout1 int = START + rand.Intn(LENGTH)
		go func() { // timer for candidate
		loopTimeOut1:
			for {
				select {
				case <-rf.refreshElectionTimeout:
					raftElectionTimeout1 = START + rand.Intn(LENGTH)
				case <-time.After(time.Duration(raftElectionTimeout1) * time.Millisecond):
					rf.mu.Lock()
					if rf.state != LEADER {
						rf.mu.Unlock()
						rf.timeOut <- struct{}{}
					} else {
						rf.mu.Unlock()
					}
					break loopTimeOut1
				}
			}
		}()
	} else {
		rf.mu.Unlock()
	}
}

//
// Candidate loop
//
func (rf *Raft) candidateLoop(args RequestVoteArgs) int {
	votes := make(chan int, 1)
	granted := make(chan struct{}, 1)
	rejected := make(chan struct{}, 1)
	candidateToFollower := make(chan struct{}, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me { // vote self
			continue
		}
		go func(i int) {
			// send request vote to other server
			reply := RequestVoteReply{}
			for {
				ok := rf.sendRequestVote(i, &args, &reply)
				if ok && reply.VoteGranted {
					granted <- struct{}{}
					votes <- i
					break
				} else if ok && !reply.VoteGranted {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm { // discover newer term
						rf.currentTerm = reply.Term
						rf.state = FOLLOWER
						rf.mu.Unlock()
					} else { // server i voted to other candidate at this term
						rf.mu.Unlock()
						rejected <- struct{}{}
					}
					break
				} else {
					// resend this request vote until ok == true or discover current leader or new term
					rf.mu.Lock()
					if rf.state != CANDIDATE {
						rf.mu.Unlock()
						break
					} else {
						rf.mu.Unlock()
					}

					time.Sleep(time.Duration(RPCDUE+rand.Intn(RPCDULENGTH)) * time.Millisecond)
					rf.mu.Lock()
					if rf.state != CANDIDATE {
						rf.mu.Unlock()
						break
					} else {
						rf.mu.Unlock()
					}
				}
			}

			rf.mu.Lock()
			if rf.state == FOLLOWER { // transit to follower state
				rf.mu.Unlock()
				candidateToFollower <- struct{}{}
			} else if rf.state == LEADER {
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
			}
		}(i)
	}

	var state, pros, cons int = -1, 1, 0
	ll := make([]int, 0)
	ll = append(ll, rf.me)
	for {
		select {
		case <-granted:
			pros += 1
			v := <-votes
			ll = append(ll, v)
		case <-rejected:
			cons += 1
		case <-candidateToFollower:
			state = FOLLOWER
		case <-rf.timeOut:
			state = CANDIDATE
		}
		if pros*2 > len(rf.peers) || cons*2 > len(rf.peers) ||
			state == FOLLOWER || state == CANDIDATE {
			break
		}
	}
	if pros*2 > len(rf.peers) {
		state = LEADER
	} else if cons*2 > len(rf.peers) {
		state = FOLLOWER
	}

	return state
}

//
// Leader loop
// send heartbeat infinitely under certain period interval,
// until rf.me transited to other state
//
func (rf *Raft) leaderLoop() bool {

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}

	leaderToFollower := make(chan struct{})

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			rf.mu.Lock()
			currentTerm := rf.currentTerm
			rf.mu.Unlock()

			for {
				// killed, transit to follower
				rf.mu.Lock()
				if rf.killed == true {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
				// sent heartbeat periodically in async mode
				go func() {

					rf.mu.Lock()

					args := AppendEntryArgs{}
					args.Term = currentTerm
					args.LeaderId = rf.me
					args.LeaderCommit = rf.commitIndex

					if rf.nextIndex[i] == 1 {
						args.PrevLogTerm = 0
						args.PrevLogIndex = 0
					} else if rf.nextIndex[i] >= 2 {
						iPrevLogIndex := rf.nextIndex[i] - 2
						prevLog := rf.log[iPrevLogIndex]
						args.PrevLogTerm = prevLog.Term
						args.PrevLogIndex = iPrevLogIndex + 1
					} else {
						log.Fatal("Can not happend!")
					}
					// each RPC send 1 log entry
					if rf.nextIndex[i] < rf.nextIndex[rf.me] {
						args.Entries = make([]LogEntry, 0)
						args.Entries = append(args.Entries, rf.log[rf.nextIndex[i]-1])
					}

					rf.mu.Unlock()

					reply := AppendEntryReply{}
					rf.sendAppendEntries(i, &args, &reply)
					// ok := rf.sendAppendEntries(i, &args, &reply)
					if !reply.Success && reply.Term > currentTerm { // find higher term, transits to follower
						rf.mu.Lock()
						if rf.state == LEADER && rf.currentTerm < reply.Term {
							rf.currentTerm = reply.Term
							rf.state = FOLLOWER
						}
						rf.mu.Unlock()
					} else {
						// consistency check
						rf.mu.Lock()
						if reply.Success &&
							rf.nextIndex[i]-1 == args.PrevLogIndex {
							rf.nextIndex[i] += len(args.Entries)
							// rf.matchIndex[i] += len(args.Entries)
							if len(args.Entries) != 0 {
								rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
							}
							// update commitIndex
							N, has := rf.matchIndex[i], 0
							if N > rf.commitIndex && rf.log[N-1].Term == rf.currentTerm {
								for j := range rf.peers {
									if rf.matchIndex[j] >= N {
										has += 1
									}
								}
								if has*2 > len(rf.peers) {
									for k := rf.commitIndex + 1; k <= N; k++ {
										applyMsg := ApplyMsg{CommandValid: true,
											Command:      rf.log[k-1].Command,
											CommandIndex: k,
										}
										rf.applyCh <- applyMsg
									}
									rf.commitIndex = N
								}
							}
						} else if reply.Term == currentTerm &&
							rf.nextIndex[i]-1 == args.PrevLogIndex {
							rf.nextIndex[i] -= 1
						} else {
							// {0, false}
						}
						rf.mu.Unlock()
					}
				}()

				rf.mu.Lock()
				if rf.state != LEADER {
					rf.mu.Unlock()
					break
				} else {
					rf.mu.Unlock()
				}
				time.Sleep(time.Duration(HEARTBEAT+rand.Intn(HEARTBEATLENGTH)) * time.Millisecond)
				rf.mu.Lock()
				if rf.state != LEADER {
					rf.mu.Unlock()
					break
				} else {
					rf.mu.Unlock()
				}
			}

			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				leaderToFollower <- struct{}{}
			} else {
				rf.mu.Unlock()
			}
		}(i)

	}

	<-leaderToFollower

	return true
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
	rf.timeOut = make(chan struct{}, 0)
	go func() {
		for {

			rf.followerLoop()

			rf.mu.Lock()
			rf.state = CANDIDATE
			rf.currentTerm += 1
			rf.votedFor = rf.me
			// Prepare RPC call parameters
			args := RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			if len(rf.log) == 0 {
				args.LastLogTerm = 0
				args.LastLogIndex = 0
			} else if len(rf.log) != 0 {
				iLastLogIndex := len(rf.log) - 1
				last := rf.log[iLastLogIndex]
				args.LastLogTerm = last.Term
				args.LastLogIndex = iLastLogIndex + 1
			} else {
				log.Fatal("Can not happend!")
			}

			// killed
			if rf.killed == true {
				rf.mu.Unlock()
				break
			}

			rf.mu.Unlock()

			state := rf.candidateLoop(args)

			rf.mu.Lock()
			rf.state = state
			if rf.state == LEADER {
				rf.mu.Unlock()
				rf.leaderLoop()
				// ok := rf.leaderLoop()
			} else if rf.state == FOLLOWER {
				rf.mu.Unlock()
			} else if state == CANDIDATE {
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
				log.Fatal("Can not happend")
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
