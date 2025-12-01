package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

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
	log         Log

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	applyCh   chan raftapi.ApplyMsg
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log.Entries)
	rfstate := w.Bytes()
	rf.persister.Save(rfstate, rf.log.Snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var (
		currentTerm int
		votedFor    int
		logEntries  []LogEntry
	)

	if err := d.Decode(&currentTerm); err != nil {
		log.Fatalf("Raft %d readPersist error: %v", rf.me, err)
	}
	if err := d.Decode(&votedFor); err != nil {
		log.Fatalf("Raft %d readPersist error: %v", rf.me, err)
	}
	if err := d.Decode(&logEntries); err != nil {
		log.Fatalf("Raft %d readPersist error: %v", rf.me, err)
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log.Entries = logEntries
	rf.log.Snapshot = rf.persister.ReadSnapshot()
	// rf.matchIndex[rf.me] = len(logEntries) - 1
	DPrintf("server %d restart, current term: %d, votedFor: %d, log last index: %d", rf.me, rf.currentTerm, rf.votedFor, rf.log.lastIndex())
	DPrintf("server %d 's log: %v", rf.me, rf.log)
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf("try to compact to index %d, server %d 's log: %v", index, rf.me, rf.log)
	rf.log.compact_to(index, snapshot)
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
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {

		rf.currentTerm = args.Term
		rf.convert_to_follower()

		lastlogidx := rf.log.lastIndex()
		lastlogterm, _ := rf.log.term(lastlogidx)

		uptodate := args.LastLogTerm > lastlogterm || (args.LastLogTerm == lastlogterm && args.LastLogIndex >= lastlogidx)
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && uptodate {
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
			rf.reset_ele_time()
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
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return index, term, false
	}

	defer rf.persist()
	DPrintf("leader %d will start cmd %d", rf.me, command)

	lastidx := rf.log.lastIndex() + 1
	entry := LogEntry{
		Index:   lastidx,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log.append([]LogEntry{entry})
	term = rf.currentTerm
	isLeader = (rf.state == Leader)
	// rf.matchIndex[rf.me] = lastidx
	// rf.append_entry_nolock()

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
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) reset_ele_time() {
	// pause for a random amount of time between 500ms and 1000ms
	ms := 500 + (rand.Int63() % 500)
	rf.electionTime = time.Now().Add(time.Duration(ms) * time.Millisecond)
	electionTimeMS := rf.electionTime.Format("15:04:05.000")
	DPrintf("server %d reset ele time %s", rf.me, electionTimeMS)
}

// func (rf *Raft) append_entry_nolock() {
// 	// send heartbeats in parallel
// 	lastidx := rf.log.lastIndex()
// 	for id := range rf.peers {
// 		if id == rf.me {
// 			continue
// 		}
// 		nextidx := rf.nextIndex[id]
// 		prevlog := rf.log[nextidx-1]

// 		args := &AppendEntriesArgs{
// 			Term:         rf.currentTerm,
// 			LeaderId:     rf.me,
// 			PrevLogIndex: nextidx - 1,
// 			PrevLogTerm:  prevlog.Term,
// 			LeaderCommit: rf.commitIndex,
// 		}

// 		if lastidx-nextidx >= 0 {
// 			args.Entries = make([]LogEntry, lastidx-nextidx+1)
// 			copy(args.Entries, rf.log[nextidx:lastidx+1])
// 		}

// 		go rf.send_entry_to_peer(id, args)
// 	}

// 	// check if entry is committed
// 	if rf.commitIndex == lastidx {
// 		return
// 	}

// 	cnt := 0
// 	for _, idx := range rf.matchIndex {
// 		if idx >= rf.commitIndex+1 {
// 			cnt += 1
// 		}
// 		// if an entry is committed, then apply
// 		if cnt > len(rf.peers)/2 {
// 			rf.commitIndex += 1
// 			rf.matchIndex[rf.me] = rf.commitIndex
// 			rf.apply()
// 			break
// 		}
// 	}
// }

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
		// update matchIdx
		matchidx := args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peerid] = matchidx + 1
		rf.matchIndex[peerid] = matchidx

		// update leader commitIdx
		go rf.update_commitidx()
		return
	}

	// if rpc reply.term > currentTerm, step down immediately
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		if rf.state != Follower {
			DPrintf("server %d convert to follower with reply term %d > currentTerm %d", rf.me, reply.Term, rf.currentTerm)
		}
		rf.convert_to_follower()
		defer rf.persist()
		return
	}

	// reply fails because of log inconsistency, decrease nextindex of peerid
	if rf.state == Leader {
		origin_idx := rf.nextIndex[peerid]
		if reply.XTerm != 0 {
			is_xterm_existed := false
			idx := rf.log.lastIndex()
			mterm, _ := rf.log.term(idx)
			for idx > 0 && mterm > reply.XTerm {
				idx--
			}
			if idx > 0 && mterm == reply.Term {
				is_xterm_existed = true
			}
			//case1: leader have XTerm
			if is_xterm_existed {
				rf.nextIndex[peerid] = idx + 1
			} else {
				//case2: leader doesn't have XTerm
				rf.nextIndex[peerid] = reply.XIndex
			}
		} else {
			//case3: follower's log is too short
			rf.nextIndex[peerid] = reply.XLen + 1
		}
		DPrintf("leader %d update nextidx[%d] from %d to %d", rf.me, peerid, origin_idx, rf.nextIndex[peerid])
	}
}

func (rf *Raft) update_commitidx() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastlogidx := rf.log.lastIndex()
	if rf.commitIndex == lastlogidx {
		return
	}

	initCommitIndex := rf.commitIndex
	left := rf.commitIndex + 1
	majority := len(rf.peers)/2 + 1
	right := 0
	for _, idx := range rf.matchIndex {
		if idx > right {
			right = idx
		}
	}
	for left <= right {
		mid := (left + right) / 2
		cnt := 0
		// cond1: count the majority match log[mid]
		for _, idx := range rf.matchIndex {
			if idx >= mid {
				cnt += 1
			}
		}
		if cnt >= majority {
			left = mid + 1
			if mterm, _ := rf.log.term(mid); mterm == rf.currentTerm {
				rf.commitIndex = mid
			}
		} else {
			right = mid - 1
		}
	}
	DPrintf("matchindex of leader %d is %d", rf.me, rf.matchIndex)
	if initCommitIndex != rf.commitIndex {
		DPrintf("leader %d update commitIndex from %d to %d", rf.me, initCommitIndex, rf.commitIndex)
		DPrintf("server %d 's log: %v", rf.me, rf.log)
		rf.apply()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
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
		DPrintf("server %d reject %d with args.term %d < %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	defer rf.reset_ele_time()
	// 5.3 doesn't contain an entry at preLogIndex whose term matches preLogIndex
	if args.PrevLogIndex > rf.log.lastIndex() {
		DPrintf("server %d refuse %d with index %d >= log_len %d, xlen is %d, xterm is %d", rf.me, args.LeaderId, args.PrevLogIndex, rf.log.lastIndex(), reply.XLen, reply.XTerm)
		// if rf.log.lastIndex() == 0 {
		// 	DPrintf("server %d 's log len 0, logs:%v", rf.me, rf.log)
		// }
		reply.XLen = rf.log.lastIndex()
		return
	}

	// 5.3 at prevLogindex with different term, fail because of inconsistency
	myprevlogterm, _ := rf.log.term(args.PrevLogIndex)
	if myprevlogterm != args.PrevLogTerm {
		DPrintf("server %d refuse %d with index %d's term %d mismatches leader's prelogterm %d", rf.me, args.LeaderId, args.PrevLogIndex, myprevlogterm, args.PrevLogTerm)
		// seek to the first index for the conflicting term
		xindex := args.PrevLogIndex
		xterm, _ := rf.log.term(args.PrevLogIndex)
		for xindex > 0 {
			prevterm, _ := rf.log.term(xindex - 1)
			if prevterm != xterm {
				break
			}
			xindex--
		}
		// for prevlogterm,_ := rf.log.term(xindex-1); xindex > 0 && prevlogterm == xterm {
		// 	xindex--
		// }
		reply.XIndex = xindex
		reply.XTerm = xterm
		return
	}

	//truncate the inconsistent logs
	rf.log.truncate(args.PrevLogIndex + 1)
	//append entries to it
	rf.log.append(args.Entries)

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		lastidx := rf.log.lastIndex()
		rf.commitIndex = min(args.LeaderCommit, lastidx)
		DPrintf("rpc trigger :server %d will apply", rf.me)
		rf.apply()
	}

	reply.Term = args.Term
	reply.Success = true
}

func (rf *Raft) start_election() {
	rf.mu.Lock()

	rf.currentTerm += 1
	rf.state = Candidate
	rf.reset_ele_time()
	votecnt := 1
	term := rf.currentTerm

	rf.mu.Unlock()

	// use channel to collect votes
	vote_ch := make(chan bool, len(rf.peers)-1)
	// timeout_ch := time.After(time.Until(rf.electionTime))

	for id := range rf.peers {
		if id == rf.me {
			// vote for itself
			continue
		}
		args := &RequestVoteArgs{}
		args.CandidateID = rf.me
		args.Term = term
		args.LastLogIndex = rf.log.lastIndex()
		args.LastLogTerm, _ = rf.log.term(args.LastLogIndex)

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
			DPrintf("server %d state change when election", rf.me)
			rf.mu.Unlock()
			return
		}

		DPrintf("server %d get votecnt %d", rf.me, votecnt)
		if votecnt > len(rf.peers)/2 {
			DPrintf("server %d become leader, currentTerm %d", rf.me, rf.currentTerm)
			rf.state = Leader
			rf.votedFor = rf.me

			// reinitialize next log index
			nextlog_idx := rf.log.lastIndex() + 1
			for i := range rf.nextIndex {
				rf.nextIndex[i] = nextlog_idx
				rf.matchIndex[i] = 0
			}
			// return will sleep for at most 350ms, then others may become leaders, so send heartbeats at once
			go rf.send_heartbeats()
			rf.persist()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.state == Candidate {
		rf.votedFor = -1
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
		rf.persist()
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
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// for !rf.killed() {
	// 	if rf.commitIndex > rf.lastApplied {
	// 		rf.lastApplied += 1
	// 		applymsg := raftapi.ApplyMsg{
	// 			CommandValid: true,
	// 			Command:      rf.log[rf.lastApplied].Command,
	// 			CommandIndex: rf.lastApplied,
	// 		}
	// 		DPrintf("server %d apply index %d, commitIndex %d", rf.me, rf.lastApplied, rf.commitIndex)
	// 		rf.applyCh <- applymsg
	// 	} else {
	// 		rf.applyCond.Wait()
	// 	}
	// }
	for !rf.killed() {
		rf.mu.Lock()
		// 1. 等待直到有新的日志可应用（循环防止虚假唤醒）
		for rf.commitIndex <= rf.lastApplied && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		// 2. 批量获取需要应用的日志范围（减少锁操作）
		if rf.log.HasPendingSnapshot {
			applyMsg := raftapi.ApplyMsg{
				CommandValid: false,
				SnapshotValid: true,
				Snapshot:     rf.log.cloneSnapshot(),
				SnapshotTerm: rf.log.LastIncludedTerm(),
				SnapshotIndex: rf.log.LastIncludedIndex(),
			}
			rf.applyCh <- applyMsg
			rf.log.HasPendingSnapshot = false
			rf.lastApplied = rf.log.LastIncludedIndex()
			DPrintf("server %d applied snapshot index %d (commitIndex=%d)", rf.me, rf.lastApplied, rf.commitIndex)
			rf.mu.Unlock()
		} else {
			start := rf.lastApplied + 1
			end := rf.commitIndex
			// 3. 提前更新lastApplied（避免重复应用）
			rf.lastApplied = end
			DPrintf("server %d commit index is %d, last applied index changed from %d to %d", rf.me, rf.commitIndex, start-1, rf.lastApplied)
			rf.mu.Unlock() // 释放锁，避免阻塞其他操作

			// 4. 发送日志到applyCh（不持有锁，防止阻塞）
			new_committed_logs := rf.log.cloneslice(start, end+1)
			for _, entry := range new_committed_logs {
				applyMsg := raftapi.ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
				DPrintf("server %d applied index %d (commitIndex=%d), cmd(%d)", rf.me, entry.Index, rf.commitIndex, applyMsg.Command)
				// 注意：若applyCh是无缓冲的，此处可能阻塞，但不影响锁持有
				rf.applyCh <- applyMsg
			}
		}
	}
}

func (rf *Raft) send_heartbeats() {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// send heartbeats in parallel
	lastidx := rf.log.lastIndex()
	rf.matchIndex[rf.me] = lastidx
	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		nextidx := rf.nextIndex[id]

		// if leader has discarded the logs to be sent, send snapshot
		if nextidx <= rf.log.LastIncludedIndex() {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.log.LastIncludedIndex(),
				LastIncludedTerm:  rf.log.LastIncludedTerm(),
				Offset:            0,
				Data:              rf.log.Snapshot,
				Done:              true,
			}
			DPrintf("leader %d send snapshot to server %d", rf.me, id)
			go rf.send_snapshot_to_peer(id, args)
			continue
		}

		prevlogterm, _ := rf.log.term(nextidx - 1)
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextidx - 1,
			PrevLogTerm:  prevlogterm,
			LeaderCommit: rf.commitIndex,
		}

		if lastidx-nextidx >= 0 {
			// args.Entries = make([]LogEntry, lastidx-nextidx+1)
			// copy(args.Entries, rf.log[nextidx:lastidx+1])
			DPrintf("send heartbeat to server %d: clone entries from %d to %d", id, nextidx, lastidx)
			args.Entries = rf.log.cloneslice(nextidx, lastidx+1)
		}

		go rf.send_entry_to_peer(id, args)
	}
	// rf.mu.Unlock()

	// ms := 5+ (rand.Int() % 5)
	// // ms = ms * len(rf.peers)
	// time.Sleep(time.Duration(ms) * time.Millisecond)

	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// // check if entry is committed
	// if rf.commitIndex == lastidx {
	// 	return
	// }

	// cnt := 0
	// for _, idx := range rf.matchIndex {
	// 	if idx >= rf.commitIndex + 1{
	// 		cnt += 1
	// 	}
	// 	// if an entry is committed, then apply
	// 	if cnt > len(rf.peers)/2 {
	// 		rf.commitIndex += 1
	// 		rf.matchIndex[rf.me] = rf.commitIndex
	// 		rf.apply()
	// 		break
	// 	}
	// }
	// update commitIndex
	// initCommitIndex := rf.commitIndex
	// left := rf.commitIndex + 1
	// majority := len(rf.peers)/2 + 1
	// right := 0
	// for _, idx := range rf.matchIndex {
	//  	if idx > right{
	//  		right = idx
	//  	}
	// }
	// for left <= right{
	// 	mid := (left + right) /2
	// 	cnt :=0
	// 	// cond1: count the majority match log[mid]
	// 	for _, idx := range rf.matchIndex {
	// 		if idx >= mid {
	// 			cnt +=1
	// 		}
	// 	}
	// 	if cnt >= majority && rf.log[mid].Term == rf.currentTerm {
	// 		rf.commitIndex = mid
	// 		left = mid + 1
	// 	} else {
	// 		right = mid - 1
	// 	}
	// }
	// if initCommitIndex != rf.commitIndex {
	// 	DPrintf("leader %d update commitIndex from %d to %d", rf.me, initCommitIndex, rf.commitIndex)
	// 	rf.apply()
	// }
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == Leader {
			// send heartbeats
			go rf.send_heartbeats()
			rf.mu.Unlock()
		} else if time.Now().After(rf.electionTime) {
			// election timeout, start election
			DPrintf("server %d starts election", rf.me)
			rf.mu.Unlock()
			rf.start_election()
		} else {
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 150 and 300
		// milliseconds.
		ms := 150 + (rand.Int() % 150)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.reset_ele_time()

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = makeLog()

	rf.commitIndex = 0
	rf.lastApplied = 0

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
