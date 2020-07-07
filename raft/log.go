package raft

//
// Entry in a Raft server.
//
type Entry struct {
	Term    int
	Command interface{}
}

//
// Log in a Raft server.
//
type Log struct {
	Entries []Entry
}

//
// Creates a new log.
// Raft logs are 1-indexed, so a sentinel entry acts as the 0-th entry.
//
func NewLog() *Log {
	l := Log{}
	l.Entries = append(l.Entries, Entry{-1, nil})
	return &l
}

//
// Returns the index of the last log.
//
func (l *Log) GetLastLogIndex() int {
	return len(l.Entries) - 1
}

//
// Returns the term of the last log.
//
func (l *Log) GetLastLogTerm() int {
	return l.Entries[len(l.Entries)-1].Term
}

//
// Returns true if there is a log entry with the given index and term.
//
func (l *Log) Contains(index int, term int) bool {
	return index < len(l.Entries) && l.Entries[index].Term == term
}

//
// Returns
// * 1 if this log > given log
// * 0 if this log = given log
// * -1 if this log = given log
//
func (l *Log) Compare(lastLogIndex int, lastLogTerm int) int {
	if l.GetLastLogTerm() == lastLogTerm && l.GetLastLogIndex() == lastLogIndex {
		return 0
	} else if l.GetLastLogTerm() > lastLogTerm ||
		(l.GetLastLogTerm() == lastLogTerm && l.GetLastLogIndex() > lastLogIndex) {
		return 1
	}

	return -1
}
