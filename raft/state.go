package raft

type State interface {
	Start(*Raft, interface{}) (int, int, bool)
	Kill(*Raft)

	AppendEntries(*Raft, AppendEntriesArgs, *AppendEntriesReply)
	RequestVote(*Raft, RequestVoteArgs, *RequestVoteReply)
}
