package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	// reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// check if logs already have included this snapshot
	term, _ := rf.log.term(args.LastIncludedIndex)
	if term == args.Term {
		rf.log.compact_to(args.LastIncludedIndex)
		return
	}

	// create new snapshot if offset == 0
	if args.Offset == 0 {
		rf.log.TempSnapshotBuf = make([]byte, len(args.Data))
	}

	// check offset
	if args.Offset != len(rf.log.TempSnapshotBuf) {
		// ignore out-of-order snapshot chunk
		return
	}

	// append data
	rf.log.TempSnapshotBuf = append(rf.log.TempSnapshotBuf, args.Data...)

	// if done, install snapshot
	if args.Done {
		rf.log.Snapshot = rf.log.TempSnapshotBuf
		rf.log.TempSnapshotBuf = nil

		// retain logs after LastIncludedIndex
		var retained_entries []LogEntry
		for _, entry := range rf.log.Entries {
			if entry.Index > args.LastIncludedIndex {
				retained_entries = rf.log.Entries[rf.log.toArrayIndex(entry.Index):]
				break
			}
		}
		rf.log.Entries = make([]LogEntry,1)
		rf.log.Entries = append(rf.log.Entries, retained_entries...)
		rf.log.Entries[0].Index = args.LastIncludedIndex
		rf.log.Entries[0].Term = args.LastIncludedTerm
	}
}

func (rf *Raft) send_snapshot_to_peer(peerid int, args *InstallSnapshotArgs) {

	reply := &InstallSnapshotReply{}
	ok := rf.peers[peerid].Call("Raft.InstallSnapshot", args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		DPrintf("server %d fail to send snapshot to peer %d", rf.me, peerid)
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.convert_to_follower()
		defer rf.persist()
		return
	}

	rf.nextIndex[peerid] = rf.log.LastIncludedIndex() + 1
	rf.matchIndex[peerid] = rf.log.LastIncludedIndex()
}
