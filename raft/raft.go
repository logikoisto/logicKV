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
	"bytes"
	"encoding/gob"
	"logicKV/labrpc"
	"sync"
)

const (
	ElectionTimeoutMin = 150
	ElectionTimeoutMax = 300
	HeartbeatTimeout   = 50
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu sync.Mutex
	// peers     []*labrpc.ClientEnd
	peers     []labrpc.Client
	persister *Persister
	me        int // index into peers[]

	currentTerm int
	votedFor    int
	log         *Log
	commitIndex int
	lastApplied int

	state State
	apply chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	_, isLeader := rf.state.(*Leader)
	return rf.currentTerm, isLeader
}

func (rf *Raft) SetState(state State) {
	rf.state.Kill(rf)
	rf.state = state
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	DPrintf("%d (unknown)  (term %d): persist: %d, %d, %v", rf.me, rf.currentTerm, rf.currentTerm, rf.votedFor, rf.log)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	DPrintf("%d (unknown)  (term %d): read persist: %d, %d, %v", rf.me, rf.currentTerm, rf.currentTerm, rf.votedFor, rf.log)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state.RequestVote(rf, args, reply)
	rf.persist()
	return nil
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state.AppendEntries(rf, args, reply)
	rf.persist()
	return nil
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state.Start(rf, command)
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state.Kill(rf)
}

func (rf *Raft) Commit() {
	if rf.commitIndex <= rf.lastApplied {
		return
	}

	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.apply <- ApplyMsg{
			Index:       rf.lastApplied,
			Command:     rf.log.Entries[rf.lastApplied].Command,
			UseSnapshot: false,
			Snapshot:    nil,
		}
		DPrintf("%d (unknown)  (term %d): applied commit %d", rf.me, rf.currentTerm, rf.lastApplied)
	}
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
func Make(peers []labrpc.Client, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = NewLog()
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.state = NewFollower(rf)
	rf.apply = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
