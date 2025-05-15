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
	Term int
	// Index   int
	Command interface{}
}

func (rf *Raft) lastLogEntry() (int, LogEntry) {
	if len(rf.log) == 0 {
		return 0, LogEntry{
			Term: 0,
		}
	}
	index := len(rf.log) - 1
	return index, rf.log[index]
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
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	applyCh   chan ApplyMsg
	applyCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
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

func (rf *Raft) convert_to_follower() {
	rf.state = Follower
	rf.votedFor = -1
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

		rf.currentTerm = args.Term
		rf.convert_to_follower()

		lastlogidx, mylastlog := rf.lastLogEntry()
		uptodate := args.LastLogTerm > mylastlog.Term || (args.LastLogTerm == mylastlog.Term && args.LastLogIndex >= lastlogidx)
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && uptodate {
			rf.votedFor = args.CandidateID
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
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return index, term, false
	}

	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	lastidx, _ := rf.lastLogEntry()
	// rf.matchIndex[rf.me] = lastidx
	rf.append_entry_nolock()
	rf.mu.Unlock()

	return lastidx, term, isLeader
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
	electionTimeMS := rf.electionTime.Format("15:04:05.000")
	DPrintf("server %d reset ele time %s", rf.me, electionTimeMS)
}

func (rf *Raft) append_entry_nolock() {
	// send heartbeats in parallel
	lastidx, _ := rf.lastLogEntry()
	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		nextidx := rf.nextIndex[id]
		prevlog := rf.log[nextidx-1]

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextidx - 1,
			PrevLogTerm:  prevlog.Term,
			LeaderCommit: rf.commitIndex,
		}

		if lastidx-nextidx >= 0 {
			args.Entries = make([]LogEntry, lastidx-nextidx+1)
			copy(args.Entries, rf.log[nextidx:lastidx+1])
		}

		go rf.send_entry_to_peer(id, args)
	}

	// check if entry is committed
	if rf.commitIndex == lastidx {
		return
	}

	cnt := 0
	for _, idx := range rf.matchIndex {
		if idx >= rf.commitIndex + 1{
			cnt += 1
		}
		// if an entry is committed, then apply
		if cnt + 1 > len(rf.peers)/2 {
			rf.commitIndex += 1
			rf.matchIndex[rf.me] = rf.commitIndex
			rf.apply()
			break
		}
	}
}

func (rf *Raft) send_entry_to_peer(peerid int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	DPrintf("server %d try to send heartbeat to %d, prev_index %d, term %d", rf.me, peerid, args.PrevLogIndex, args.Term)
	ok := rf.peers[peerid].Call("Raft.AppendEntries", args, &reply)

	if !ok {
		DPrintf("server %d fail to send heartbeat to %d", rf.me, peerid)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// handle reply
	if reply.Success {
		// if heartbeats rpc, skip update nextindex
		if len(args.Entries) == 0 {
			return
		}
		matchidx := args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peerid] = matchidx + 1
		rf.matchIndex[peerid] = matchidx
		return
	}

	// if rpc reply.term > currentTerm, step down immediately
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		if rf.state != Follower {
			DPrintf("server %d convert to follower with reply term %d > currentTerm %d", rf.me, reply.Term, rf.currentTerm)
		}
		rf.convert_to_follower()
		return
	}

	// reply fails because of log inconsistency, decrease nextindex of peerid
	if rf.state == Leader {
		rf.nextIndex[peerid] -= 1
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server %d received heartbeats from %d, prev_index %d, term %d, leadercommit %d", rf.me, args.LeaderId, args.PrevLogIndex, args.Term, args.LeaderCommit)

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term > rf.currentTerm {
		if rf.state != Follower {
			DPrintf("server %d convert to follower", rf.me)
		}
		rf.currentTerm = args.Term
		rf.convert_to_follower()
	}

	// 5.1 if a server If a server receives a request with a stale term number,
	// it rejects the request.
	if args.Term < rf.currentTerm {
		DPrintf("server %d refuse %d with args.term %d < %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	// 5.3 doesn't contain an entry at preLogIndex whose term matches preLogIndex
	if args.PrevLogIndex >= len(rf.log) {
		DPrintf("server %d refuse %d with index %d >= log_len %d", rf.me, args.LeaderId, args.PrevLogIndex, len(rf.log))
		return
	}

	// 5.3 at prevLogindex with different term, fail because of inconsistency
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("server %d refuse %d with index %d's term %d mismatches preterm %d", rf.me, args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}

	//truncate the inconsistent logs
	rf.log = rf.log[:args.PrevLogIndex+1]
	//append entries to it
	rf.log = append(rf.log, args.Entries...)

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		lastidx, _ := rf.lastLogEntry()
		rf.commitIndex = min(args.LeaderCommit, lastidx)
		DPrintf("rpc trigger :server %d will apply", rf.me)
		rf.apply()
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

		heartbeats := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}

		// send heartbeats in parallel
		go func(leader int, peerid int, args *AppendEntriesArgs) {
			reply := AppendEntriesReply{}
			ok := rf.peers[peerid].Call("Raft.AppendEntries", args, &reply)

			if !ok {
				DPrintf("server %d fail to send heartbeat to %d", leader, id)
			}
		}(rf.me, id, heartbeats)
	}
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
		if rf.state != Candidate || time.Now().After(rf.electionTime) {
			rf.mu.Unlock()
			return
		}

		if votecnt + 1 > len(rf.peers)/2 {
			DPrintf("server %d become leader", rf.me)
			rf.state = Leader
			// return will sleep for at most 350ms, then others may become leaders, so send heartbeats at once
			rf.append_entry_nolock()
			rf.mu.Unlock()
			return
		} else {
			rf.votedFor = -1
		}

		rf.mu.Unlock()
	}
	electionTimeMS := rf.electionTime.Format("15:04:05.000")
	DPrintf("server %d fail to be leader, reset ele time %s", rf.me, electionTimeMS)
}

func (rf *Raft) ask_votes(serverId int, args *RequestVoteArgs, voteCh chan<- bool) {

	reply := &RequestVoteReply{}
	ok := rf.peers[serverId].Call("Raft.RequestVote", args, reply)

	if !ok {
		voteCh <- false
		DPrintf("server %d RequestVote to %d discarded", rf.me, serverId)
		return
	}

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.convert_to_follower()
	}
	rf.mu.Unlock()

	voteCh <- reply.VoteGranted
	if reply.VoteGranted {
		DPrintf("server %d received vote from %d", rf.me, serverId)
	}

}

func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			applymsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.commitIndex,
			}
			DPrintf("server %d apply index %d, commitIndex %d", rf.me, rf.lastApplied, rf.commitIndex)
			rf.applyCh <- applymsg
		} else {
			rf.applyCond.Wait()
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == Leader {
			// send heartbeats
			rf.append_entry_nolock()
			rf.mu.Unlock()
		} else if time.Now().After(rf.electionTime) {
			// election timeout, start election
			DPrintf("server %d starts election", rf.me)
			rf.mu.Unlock()
			rf.start_election()
		} else {
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		// log.Printf("server %d will sleep for %d ms", rf.me, ms)
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

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0}}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
