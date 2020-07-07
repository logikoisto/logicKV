package raft

import (
	"math/rand"
	"time"
)

type Candidate struct {
	votes   chan bool
	done    chan struct{}
	timeout time.Duration
}

func (c *Candidate) Start(rf *Raft, command interface{}) (int, int, bool) {
	DPrintf("%d (candidate) (term %d): Start(%v) called", rf.me, rf.currentTerm, command)
	return rf.log.GetLastLogIndex() + 1, rf.currentTerm, false
}

func (c *Candidate) Kill(rf *Raft) {
	close(c.done)
	DPrintf("%d (candidate) (term %d): killed", rf.me, rf.currentTerm)
	DPrintf("%d (candidate) (term %d): killed: log: %v", rf.me, rf.currentTerm, rf.log.Entries)
}

func (c *Candidate) AppendEntries(rf *Raft, args AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term >= rf.currentTerm {
		DPrintf("%d (candidate) (term %d): found AppendEntries with equal or higher term from %d: converting to follower", rf.me, rf.currentTerm, args.LeaderId)
		rf.SetState(NewFollower(rf))
		rf.state.AppendEntries(rf, args, reply)
	} else {
		DPrintf("%d (candidate) (term %d): rejected AppendEntries request from %d with term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
	}
}

func (c *Candidate) RequestVote(rf *Raft, args RequestVoteArgs, reply *RequestVoteReply) {
	if args.Term > rf.currentTerm {
		DPrintf("%d (candidate) (term %d): found RequestVote with higher term from %d: converting to follower", rf.me, rf.currentTerm, args.CandidateId)
		rf.SetState(NewFollower(rf))
		rf.state.RequestVote(rf, args, reply)
	} else {
		DPrintf("%d (candidate) (term %d): rejected AppendEntries request from %d with term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

func (c *Candidate) HandleRequestVote(rf *Raft, server int, args RequestVoteArgs) {
	var reply RequestVoteReply
	DPrintf("%d (candidate) (term %d): sending RequestVote to %d: %+v", rf.me, rf.currentTerm, server, args)
	ok := rf.sendRequestVote(server, args, &reply)
	select {
	case <-c.done:
		return
	default:
	}

	if ok && reply.VoteGranted {
		DPrintf("%d (candidate) (term %d): received yes RequestVote from %d: %+v", rf.me, rf.currentTerm, server, reply)
		c.votes <- true
	} else if ok && !reply.VoteGranted {
		DPrintf("%d (candidate) (term %d): received no RequestVote from %d: %+v", rf.me, rf.currentTerm, server, reply)
	} else {
		DPrintf("%d (candidate) (term %d): could not send RequestVote to %d", rf.me, rf.currentTerm, server)
	}
}

//
// Wait for either the election to be won or the timeout to go off.
//
func (c *Candidate) Wait(rf *Raft) {
	timeout := make(chan struct{})
	go func() {
		<-time.After(c.timeout)
		close(timeout)
	}()

	for {
		select {
		case <-timeout:
			DPrintf("%d (candidate) (term %d): timed out", rf.me, rf.currentTerm)
			rf.mu.Lock()
			rf.SetState(NewCandidate(rf))
			rf.mu.Unlock()
			return
		case <-c.done:
			DPrintf("%d (candidate) (term %d): stopped waiting", rf.me, rf.currentTerm)
			return
		default:
			if len(c.votes) == cap(c.votes) {
				DPrintf("%d (candidate) (term %d): won election", rf.me, rf.currentTerm)
				rf.mu.Lock()
				rf.SetState(NewLeader(rf))
				rf.mu.Unlock()
				return
			}
		}
	}
}

func NewCandidate(rf *Raft) State {
	c := Candidate{}
	c.done = make(chan struct{})
	c.votes = make(chan bool, len(rf.peers)/2+1)
	c.votes <- true
	c.timeout = time.Duration(rand.Intn(ElectionTimeoutMin)+(ElectionTimeoutMax-ElectionTimeoutMin)) * time.Millisecond

	// Send RequestVote RPCs to all peers
	rf.votedFor = rf.me
	rf.currentTerm++
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.GetLastLogIndex(),
		LastLogTerm:  rf.log.GetLastLogTerm(),
	}

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go c.HandleRequestVote(rf, server, args)
	}

	go c.Wait(rf)

	DPrintf("%d (candidate) (term %d): created new candidate", rf.me, rf.currentTerm)
	return &c
}
