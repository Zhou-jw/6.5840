package raft

import "errors"

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type Log struct {
	entries []LogEntry
}

func makeLog() Log {
	log := Log {
		entries: []LogEntry{{Index: 0, Term: 0}},
	}
	return log
}

func (log *Log) firstIndex() int {
	return log.entries[0].Index
}

func (log *Log) toArrayIndex(index int) int{
	// warning: an unstable implementation may incur integer underflow. (my implementation is stable now)
	return index - log.firstIndex()
}

func (log *Log) lastIndex() int {
	if len(log.entries) == 0 {
		return 0
	}
	index := len(log.entries) - 1
	return log.entries[index].Index
}

var ErrOutOfBound = errors.New("index out of bound")

func (log *Log) term(index int) (int , error) {
	if index < log.firstIndex() || index > log.lastIndex(){
		return 0, ErrOutOfBound
	}
	index = log.toArrayIndex(index)
	return log.entries[index].Term, nil
}

func (log *Log) at(index int) LogEntry{
	index = log.toArrayIndex(index)
	return log.entries[index]
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
	return log.clone(log.entries[start:end])
}

func (log *Log) append(new_entries []LogEntry) {
	log.entries = append(log.entries, new_entries...)
}

func (log *Log) truncate(index int) {
	index = log.toArrayIndex(index)
	log.entries = log.entries[:index]
}

func (log *Log) compact_to(index int) {
		suffix := make([]LogEntry, 0)
	suffix_idx := index + 1
	if suffix_idx < log.lastIndex() {
		suffix = log.entries[suffix_idx:]
	}

	log.entries = append(make([]LogEntry, 1), suffix...)
	term0, _ := log.term(index)
	log.entries[0] = LogEntry{Index: index, Term: term0}
}
// func (rf *Raft) lastLogIndexTerm() (int, int) {
// 	if len(rf.log) == 0 {
// 		return 0, 0
// 	}
// 	index := len(rf.log) - 1
// 	return index, rf.log[index].Term
// }
