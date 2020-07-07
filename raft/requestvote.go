package raft

type RequestVoteArgs struct {
	Term         int
	CandidateId  int // index of candidate in `peers`
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}
