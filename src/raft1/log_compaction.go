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
	DPrintf("server %d receive snapshot from %d", rf.me, args.LeaderId)

	reply.Term = rf.currentTerm
	// 1. reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// 2. create new snapshot if offset == 0
	if args.Offset == 0 {
		rf.log.TempSnapshotBuf = make([]byte, len(args.Data))
	} else if args.Offset != 0 && args.Offset != len(rf.log.TempSnapshotBuf) {
		// invalid offset
		return
	}

	defer rf.persist()

	// 3. copy data into TempSnapshotBuf at offset
	requiredLen := args.Offset + len(args.Data)
	// expand buffer if needed
    if cap(rf.log.TempSnapshotBuf) < requiredLen {
        newBuf := make([]byte, len(rf.log.TempSnapshotBuf), requiredLen*2)
        copy(newBuf, rf.log.TempSnapshotBuf)
        rf.log.TempSnapshotBuf = newBuf
    }
	// resize slice and copy data
	rf.log.TempSnapshotBuf = rf.log.TempSnapshotBuf[:requiredLen]
    copy(rf.log.TempSnapshotBuf[args.Offset:], args.Data)

	if !args.Done {
		return
	}

	// (TODO):refuse to install old snapshot ?
	if args.LastIncludedIndex <= rf.log.LastIncludedIndex() {
		rf.log.TempSnapshotBuf = nil
		return
	}

	// 4. if done, install snapshot
	rf.log.Snapshot = rf.log.TempSnapshotBuf
	rf.log.TempSnapshotBuf = nil
	rf.log.HasPendingSnapshot = true

	// retain logs after LastIncludedIndex
	var retained_entries []LogEntry
	for _, entry := range rf.log.Entries {
		if entry.Index > args.LastIncludedIndex {
			retained_entries = rf.log.Entries[rf.log.toArrayIndex(entry.Index):]
			break
		}
	}
	rf.log.Entries = make([]LogEntry, 1)
	rf.log.Entries = append(rf.log.Entries, retained_entries...)

	// update snapshot metadata
	rf.log.Entries[0].Index = args.LastIncludedIndex
	rf.log.Entries[0].Term = args.LastIncludedTerm
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
