package raft

import "errors"

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type Log struct {
	Entries         []LogEntry
	Snapshot        []byte
	TempSnapshotBuf []byte

	// indicate there is a snapshot to be applied
	HasPendingSnapshot bool
}

func makeLog() Log {
	log := Log{
		Entries:  []LogEntry{{0, 0, nil}},
		Snapshot: make([]byte, 0),
		HasPendingSnapshot: false,
	}
	return log
}

func (log *Log) firstIndex() int {
	return log.Entries[0].Index
}

func (log *Log) toArrayIndex(index int) int {
	// warning: an unstable implementation may incur integer underflow. (my implementation is stable now)
	return index - log.firstIndex()
}

func (log *Log) lastIndex() int {
	if len(log.Entries) == 0 {
		return 0
	}
	index := len(log.Entries) - 1
	return log.Entries[index].Index
}

func (log *Log) LastIncludedIndex() int {
	return log.Entries[0].Index
}

func (log *Log) LastIncludedTerm() int {
	return log.Entries[0].Term
}

var ErrOutOfBound = errors.New("index out of bound")

func (log *Log) term(index int) (int, error) {
	if index < log.firstIndex() || index > log.lastIndex() {
		return 0, ErrOutOfBound
	}
	index = log.toArrayIndex(index)
	return log.Entries[index].Term, nil
}

func (log *Log) clone(entries []LogEntry) []LogEntry {
	cloned := make([]LogEntry, len(entries))
	copy(cloned, entries)
	return cloned
}

func (log *Log) cloneslice(start int, end int) []LogEntry {
	if start == end {
		return nil
	}
	start = log.toArrayIndex(start)
	end = log.toArrayIndex(end)
	DPrintf("cloneslice: start is %d, end is %d", start, end)
	return log.clone(log.Entries[start:end])
}

func (log *Log) append(new_entries []LogEntry) {
	log.Entries = append(log.Entries, new_entries...)
}

func (log *Log) truncate(index int) {
	index = log.toArrayIndex(index)
	log.Entries = log.Entries[:index]
}

func (log *Log) compact_to(index int, snapshot []byte) {
	suffix := make([]LogEntry, 0)
	suffix_idx := index + 1
	if suffix_idx < log.lastIndex() {
		suffix_idx = log.toArrayIndex(suffix_idx)
		suffix = log.Entries[suffix_idx:]
	}
	term0, _ := log.term(index)
	log.Entries = append(make([]LogEntry, 1), suffix...)
	log.Entries[0].Index = index
	log.Entries[0].Term = term0
	log.Snapshot = snapshot
}

func (log *Log) cloneSnapshot() []byte {
	cloned := make([]byte, len(log.Snapshot))
	DPrintf("clone snapshot: len %d", len(log.Snapshot))
	copy(cloned, log.Snapshot)
	return cloned
}