package raft

import (
	"sync"
	"time"
)

type AppendEntriesMessage struct {
	Args  AppendEntriesArgs
	Reply AppendEntriesReply
}

type Leader struct {
	mu         sync.Mutex
	nextIndex  []int
	matchIndex []int
	done       chan struct{}
}

func (l *Leader) Start(rf *Raft, command interface{}) (int, int, bool) {
	DPrintf("%d (leader)    (term %d): Start(%v) called", rf.me, rf.currentTerm, command)
	entry := Entry{rf.currentTerm, command}
	rf.log.Entries = append(rf.log.Entries, entry)
	//l.nextIndex[rf.me]++
	//l.matchIndex[rf.me]++
	rf.persist()

	return rf.log.GetLastLogIndex(), rf.currentTerm, true
}

func (l *Leader) Kill(rf *Raft) {
	close(l.done)
	DPrintf("%d (leader)    (term %d): killed", rf.me, rf.currentTerm)
	DPrintf("%d (leader)    (term %d): killed: log: %v", rf.me, rf.currentTerm, rf.log.Entries)
}

func (l *Leader) AppendEntries(rf *Raft, args AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term > rf.currentTerm {
		DPrintf("%d (leader)    (term %d): found AppendEntries with higher term from %d: converting to follower", rf.me, rf.currentTerm, args.LeaderId)
		rf.SetState(NewFollower(rf))
		rf.state.AppendEntries(rf, args, reply)
	} else {
		DPrintf("%d (leader)    (term %d): rejected AppendEntries request from %d with term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
	}
}

func (l *Leader) RequestVote(rf *Raft, args RequestVoteArgs, reply *RequestVoteReply) {
	if args.Term > rf.currentTerm {
		DPrintf("%d (leader)    (term %d): found RequestVote with higher term from %d: converting to follower", rf.me, rf.currentTerm, args.CandidateId)
		rf.SetState(NewFollower(rf))
		rf.state.RequestVote(rf, args, reply)
	} else {
		DPrintf("%d (leader)    (term %d): rejected AppendEntries request from %d with term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

func (l *Leader) HandleOneAppendEntries(rf *Raft, server int, args AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(server, args, &reply)
	select {
	case <-l.done:
		return
	default:
	}
	if ok {
		DPrintf("%d (leader)    (term %d): received %v AppendEntries reply from %d: %+v", rf.me, rf.currentTerm, reply.Success, server, reply)

		rf.mu.Lock()
		l.mu.Lock()
		defer rf.mu.Unlock()
		defer l.mu.Unlock()

		if rf.currentTerm != args.Term {
			return
		} else if reply.Term > args.Term {
			rf.SetState(NewFollower(rf))
		} else if reply.Success {
			// update nextIndex and matchIndex for follower at `server`
			numAdded := len(args.Entries)
			l.nextIndex[server] = args.PrevLogIndex + numAdded + 1
			l.matchIndex[server] = args.PrevLogIndex + numAdded

			for N := rf.commitIndex + 1; N < len(rf.log.Entries); N++ {
				// Check if a majority of matchIndex >= N
				count := 0
				for _, m := range l.matchIndex {
					if m >= N {
						count++
					}
				}

				if count >= len(rf.peers)/2 && rf.log.Entries[N].Term == rf.currentTerm {
					DPrintf("%d (leader)    (term %d): setting commit index from %d to %d", rf.me, rf.currentTerm, rf.commitIndex, N)
					rf.commitIndex = N
					rf.Commit()
					return
				}
			}
		} else {
			if reply.Conflict {
				// search log for last term with `ConflictTerm` as its term
				index := -1
				for i := range rf.log.Entries {
					if rf.log.Entries[i].Term == reply.ConflictTerm {
						index = i
					}
				}

				// if not found, set nextIndex to `ConflictIndex`
				if index != -1 {
					l.nextIndex[server] = index + 1
				} else {
					l.nextIndex[server] = reply.ConflictIndex
				}
			} else {
				// decrement nextIndex for `server`
				l.nextIndex[server]--
				if l.nextIndex[server] < 1 {
					l.nextIndex[server] = 1
				}
			}
		}
	} else {
		DPrintf("%d (leader)    (term %d): could not send AppendEntries to %d: %+v", rf.me, rf.currentTerm, server, args)
	}
}

func (l *Leader) HandleAppendEntries(rf *Raft, server int) {

	for {
		// Send append entries
		rf.mu.Lock()
		l.mu.Lock()

		args := AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex

		prevLogIndex := l.nextIndex[server] - 1
		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = rf.log.Entries[prevLogIndex].Term
		args.Entries = rf.log.Entries[prevLogIndex+1:]

		l.mu.Unlock()
		rf.mu.Unlock()

		DPrintf("%d (leader)    (term %d): sending AppendEntries to %d: %+v", rf.me, rf.currentTerm, server, args)
		go l.HandleOneAppendEntries(rf, server, args)
		select {
		case <-l.done:
			return
		case <-time.After(time.Millisecond * HeartbeatTimeout):
		}
	}
}

func NewLeader(rf *Raft) State {
	l := Leader{}

	l.nextIndex = make([]int, len(rf.peers))
	l.matchIndex = make([]int, len(rf.peers))
	for server := range rf.peers {
		l.nextIndex[server] = rf.log.GetLastLogIndex() + 1
		l.matchIndex[server] = 0
	}
	l.done = make(chan struct{})

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go l.HandleAppendEntries(rf, server)
	}

	DPrintf("%d (leader)    (term %d): created new leader", rf.me, rf.currentTerm)
	return &l
}
