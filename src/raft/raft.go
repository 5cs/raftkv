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
	candidateToFollower    chan struct{}
	heartBeat              chan struct{}
	leaderToFollower       chan struct{}
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
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Println("RPC handler: Raft.RequestVote",
		"\nCallee", "id:", rf.me, "term:", rf.currentTerm, "votedFor:", rf.votedFor,
		"\nCaller", "id:", args.CandidateId, "term:", args.Term)
	if rf.state == FOLLOWER || rf.state == CANDIDATE {
		rf.refreshElectionTimeout <- struct{}{} // reset timer
	}
	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if rf.votedFor == -1 {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		// grant it when request has more update logs
		reply.Term = args.Term
		rf.currentTerm = args.Term
		if rf.nextIndex[rf.me] == 1 {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			if rf.state == LEADER { // transit to follower
				rf.state = FOLLOWER
			} else if rf.state == CANDIDATE {
				rf.state = FOLLOWER
			}
		} else if rf.nextIndex[rf.me] >= 2 {
			iLastLogIndex := rf.nextIndex[rf.me] - 2
			last := rf.log[iLastLogIndex]
			if args.LastLogTerm > last.Term ||
				(args.LastLogTerm == last.Term && args.LastLogIndex >= iLastLogIndex+1) {
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				if rf.state == LEADER { // transits to follower
					rf.state = FOLLOWER
				} else if rf.state == CANDIDATE {
					rf.state = FOLLOWER
				}
			} else {
				reply.VoteGranted = false
			}
		} else {
			log.Fatal("Can not happend!")
		}
	}
	log.Println("=====RPC handler: Raft.RequestVote",
		"\nCallee", "id:", rf.me, "term:", rf.currentTerm, "votedFor:", rf.votedFor,
		"\nCaller", "id:", args.CandidateId, "term:", args.Term)
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

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Println("RPC handler: Raft.AppendEntries",
		"\nCallee", "id:", rf.me, "term:", rf.currentTerm,
		"\nCaller", "id:", args.LeaderId, "term:", args.Term, "entries:", args.Entries)

	if rf.state == FOLLOWER || rf.state == CANDIDATE {
		rf.refreshElectionTimeout <- struct{}{} // reset timer
	}
	if args.Term < rf.currentTerm { // newer leader
		reply.Term = rf.currentTerm
		reply.Success = false
	} else if rf.nextIndex[rf.me] == 1 { // this raft instance does not have log entry
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.Success = true

		if rf.state == LEADER { // transit to follower passively
			rf.state = FOLLOWER
		} else if rf.state == CANDIDATE {
			rf.state = FOLLOWER
		}
	} else if rf.nextIndex[rf.me] >= 2 {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		iLastLogIndex := rf.nextIndex[rf.me] - 2
		last := rf.log[iLastLogIndex]
		if iLastLogIndex != args.PrevLogIndex {
			reply.Success = false
		} else if iLastLogIndex == args.PrevLogIndex &&
			last.Term != args.PrevLogTerm {
			reply.Success = false
		} else if iLastLogIndex == args.PrevLogIndex &&
			last.Term == args.PrevLogTerm {
			reply.Success = true
		} else {
			log.Fatal("Can not happend!")
		}

		if rf.state == LEADER { // transit to follower passively
			rf.state = FOLLOWER
		} else if rf.state == CANDIDATE {
			rf.state = FOLLOWER
		}
	} else {
		log.Fatal("Can not happend!")
	}
	log.Println("=====RPC handler: Raft.AppendEntries",
		"\nCallee", "id:", rf.me, "term:", rf.currentTerm,
		"\nCaller", "id:", args.LeaderId, "term:", args.Term, "entries:", args.Entries)
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
	isLeader := true

	// Your code here (2B).

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
	// TODO: read persisted log entries
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.refreshElectionTimeout = make(chan struct{}, 1)

	const (
		START           = 300
		LENGTH          = 100
		RPCDUE          = 50
		RPCDULENGTH     = 10
		HEARTBEAT       = 50
		HEARTBEATLENGTH = 10
	)

	go func() {
		reElection := false
		for {
			rf.state = FOLLOWER
			// reset election timeout
			timeOut := make(chan struct{}, 1)
			var raftElectionTimeout int = START + rand.Intn(LENGTH)
			go func() {
			loopTimeOut:
				for {
					select {
					case <-rf.refreshElectionTimeout:
						raftElectionTimeout = START + rand.Intn(LENGTH)
					case <-time.After(time.Duration(raftElectionTimeout) * time.Millisecond):
						timeOut <- struct{}{}
						break loopTimeOut
					}
				}
			}()
			if !reElection { // transited from candidate state
				<-timeOut
				go func() { // timer for candidate
				loopTimeOut1:
					for {
						select {
						case <-rf.refreshElectionTimeout:
							raftElectionTimeout = START + rand.Intn(LENGTH)
						case <-time.After(time.Duration(raftElectionTimeout) * time.Millisecond):
							timeOut <- struct{}{}
							break loopTimeOut1
						}
					}
				}()
			}
			reElection = false

			rf.mu.Lock()

			rf.state = CANDIDATE
			rf.currentTerm += 1
			// Prepare RPC call parameters
			args := RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			if rf.nextIndex[rf.me] == 1 {
				args.LastLogTerm = 0
				args.LastLogIndex = 0
			} else if rf.nextIndex[rf.me] >= 2 {
				iLastLogIndex := rf.nextIndex[rf.me] - 2
				last := rf.log[iLastLogIndex]
				args.LastLogTerm = last.Term
				args.LastLogIndex = iLastLogIndex + 1
			} else {
				log.Fatal("Can not happend!")
			}

			rf.candidateToFollower = make(chan struct{}, len(peers)) // new channel, avoid effects from prev rounds

			rf.mu.Unlock()

			granted := make(chan struct{}, 1)
			rejected := make(chan struct{}, 1)
			for i := range rf.peers {
				go func(i int) {
					if i != rf.me { // send request vote to other server
						reply := RequestVoteReply{}
						for {
							ok := rf.sendRequestVote(i, &args, &reply)
							if ok && reply.VoteGranted {
								granted <- struct{}{}
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
								// resend this request vote until ok == true, discover newer leader or new term
								if rf.state != CANDIDATE {
									break
								}

								time.Sleep(time.Duration(RPCDUE+rand.Intn(RPCDULENGTH)) * time.Millisecond)

								if rf.state != CANDIDATE {
									break
								}
							}
						}
						// transit to follower state
						if rf.state != CANDIDATE {
							rf.candidateToFollower <- struct{}{}
						}
					} else { // vote self
						// TODO: double vote?
						rf.mu.Lock()
						rf.votedFor = rf.me
						rf.mu.Unlock()
						granted <- struct{}{}
					}
				}(i)
			}

			// candidate harvest events at this point
			var positive, negative int = 0, 0
			toFollower := false
			for {
				select {
				case <-granted:
					positive += 1
				case <-rejected:
					negative += 1
				case <-rf.candidateToFollower:
					toFollower = true
				case <-timeOut:
					reElection = true
				}
				if positive*2 > len(rf.peers) ||
					negative*2 > len(rf.peers) ||
					toFollower || reElection {
					break
				}
			}

			if positive*2 > len(rf.peers) { // leader
				rf.mu.Lock()
				rf.state = LEADER
				rf.mu.Unlock()
				// send heartbeat infinitely under certain period interval, until rf.me transited to other state
				for {
					if rf.state != LEADER {
						break
					}

					time.Sleep(time.Duration(HEARTBEAT+rand.Intn(HEARTBEATLENGTH)) * time.Millisecond)

					if rf.state != LEADER {
						break
					}

					for i := range rf.peers {
						if i == rf.me {
							continue
						}
						go func(i int) {
							currentTerm := rf.currentTerm

							args := AppendEntryArgs{}
							args.Term = currentTerm
							args.LeaderId = rf.me
							if rf.nextIndex[rf.me] == 1 {
								args.PrevLogTerm = 0
								args.PrevLogIndex = 0
							} else {
								iLastLogIndex := rf.nextIndex[rf.me] - 2
								last := rf.log[iLastLogIndex]
								args.PrevLogTerm = last.Term
								args.PrevLogIndex = iLastLogIndex + 1
							}
							// args.LeaderCommit = args.PrevLogIndex
							reply := AppendEntryReply{}
							ok := rf.sendAppendEntries(i, &args, &reply)
							// find higher term, transit from leader to follower
							if !reply.Success && reply.Term > currentTerm {
								rf.mu.Lock()
								if rf.state == LEADER && rf.currentTerm < reply.Term {
									rf.currentTerm = reply.Term
									rf.state = FOLLOWER
								}
								rf.mu.Unlock()
							} else if ok || !ok {
								// TODO: 2B/2C
							}
						}(i)
					}
				}
			} else if toFollower {
				rf.mu.Lock()
				rf.state = FOLLOWER
				rf.mu.Unlock()
			} else if negative*2 > len(rf.peers) {

			} else if reElection {

			} else {
				log.Fatal("Can not happend")
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
