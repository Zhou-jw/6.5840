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

import (
	//	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term  int
	Index int
}

func (rf *Raft) LastLogEntry() LogEntry {
	if len(rf.log) == 0 {
		return LogEntry{
			Term:  0,
			Index: 0,
		}
	}
	return rf.log[len(rf.log)-1]
}

type State int

const (
	Leader State = iota
	Candidate
	Follower
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state        State
	electionTime time.Time

	// Persistent state
	currentTerm int
	votedFor    int // candidateId that received vote in current term(or null if none)
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	LastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	// candidate's term
	Term int
	// candidate requesting vote
	CandidateID int
	// index of candidate's last log entry
	LastLogIndex int
	// term of candidate's last log entry
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	// current term, for candidate to update itself
	Term int
	//true means received vote
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		mylastlog := rf.LastLogEntry()
		uptodate := args.LastLogTerm > mylastlog.Term || (args.LastLogTerm == mylastlog.Term && args.LastLogIndex >= mylastlog.Index)
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && uptodate {
			reply.VoteGranted = true
		}
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) reset_ele_time() {
	// pause for a random amount of time between 1000ms and 2000s
	ms := 1000 + (rand.Int63() % 1000)
	rf.electionTime = time.Now().Add(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("server %d received heartbeats", rf.me)

	// 5.1 if a server If a server receives a request with a stale term number,
	// it rejects the request.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.state == Candidate {
			rf.state = Follower
		}
		rf.votedFor = -1
	}

	rf.reset_ele_time()
	reply.Term = args.Term
	reply.Success = true
}

func (rf *Raft) send_heartbeats_nolock() {

	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}

		reply := AppendEntriesReply{}
		ok := rf.peers[id].Call("Raft.AppendEntries", &args, &reply)

		if !ok {
			log.Printf("server %d fail to send heartbeat to %d", rf.me, id)
		}
	}
}

func (rf *Raft) append_entries() {
}

func (rf *Raft) start_election() {
	rf.mu.Lock()

	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.reset_ele_time()
	votecnt := 1
	term := rf.currentTerm

	rf.mu.Unlock()

	// use channel to collect votes
	vote_ch := make(chan bool, len(rf.peers)-1)

	for id := range rf.peers {
		if id == rf.me {
			// vote for itself
			continue
		}
		args := &RequestVoteArgs{}
		args.CandidateID = rf.me
		args.Term = term

		// request votes from other servers
		go rf.ask_votes(id, args, vote_ch)
	}

	// handle the votes result
	for i := 0; i < len(rf.peers)-1; i++ {
		granted := <-vote_ch
		if granted {
			votecnt++
		}

		rf.mu.Lock()
		// state change when election, because received valid heartbeats from leader
		if rf.state != Candidate {
			rf.mu.Unlock()
			return
		}

		if votecnt > len(rf.peers)/2 {
			log.Printf("server %d become leader", rf.me)
			rf.state = Leader
			// return will sleep for at most 350ms, then others may become leaders, so send heartbeats at once
			rf.send_heartbeats_nolock()
			rf.mu.Unlock()
			return
		} else {
			rf.votedFor = -1
		}

		rf.mu.Unlock()
	}
	electionTimeMS := rf.electionTime.Format("01:02.000")
	log.Printf("server %d fail to be leader, reset ele time %s", rf.me, electionTimeMS)
}

func (rf *Raft) ask_votes(serverId int, args *RequestVoteArgs, voteCh chan<- bool) {

	reply := &RequestVoteReply{}
	ok := rf.peers[serverId].Call("Raft.RequestVote", args, reply)

	if !ok {
		voteCh <- false
		log.Printf("server %d RequestVote to %d discarded", rf.me, serverId)
		return
	}

	voteCh <- reply.VoteGranted
	if reply.VoteGranted {
		log.Printf("server %d received vote from %d", rf.me, serverId)
	}

}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == Leader {
			rf.send_heartbeats_nolock()
			rf.mu.Unlock()
		} else if time.Now().After(rf.electionTime) {
			// election timeout, start election
			log.Printf("server %d starts election", rf.me)
			rf.mu.Unlock()
			rf.start_election()
		} else {
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.reset_ele_time()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
