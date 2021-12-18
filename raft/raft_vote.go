package raft

import (
	"sync/atomic"
	"time"
)

const VoteTimeout = 1000
const VoteInterval time.Duration = time.Millisecond * 50
const HeartBeatTimeout = 500
const HeartBeatInterval time.Duration = time.Millisecond * 50

type Vote struct {
	Req    RequestVoteArgs
	ReplyC chan RequestVoteReply
}
type Entry struct {
	Req    AppendEntriesArgs
	ReplyC chan AppendEntriesReply
}

func (rf *Raft) handleEntry(req AppendEntriesArgs) AppendEntriesReply {
	currentTerm := rf.getTerm()
	if currentTerm <= req.Term {
		rf.setTerm(req.Term)
		return AppendEntriesReply{
			Term:    req.Term,
			Success: true,
		}
	} else {
		return AppendEntriesReply{
			Term:    currentTerm,
			Success: false,
		}
	}
}

func (rf *Raft) handleVote(req RequestVoteArgs) RequestVoteReply {
	currentTerm := rf.getTerm()
	// currentTerm < req.Term && (rf.votedFor == -1 || rf.votedFor == req.CandidateId)
	if currentTerm < req.Term {
		rf.Log("Granted Vote to ", req.CandidateId, " currentTerm: ", currentTerm, " req.Term ", req.Term)
		rf.setTerm(req.Term)
		rf.votedFor = req.CandidateId
		return RequestVoteReply{
			Term:        req.Term,
			VoteGranted: true,
		}
	} else {
		return RequestVoteReply{
			Term:        currentTerm,
			VoteGranted: false,
		}
	}
}

func (rf *Raft) setTerm(term int64) {
	atomic.StoreInt64(&rf.currentTerm, term)
}

func (rf *Raft) getTerm() int64 {
	return atomic.LoadInt64(&rf.currentTerm)
}

func (rf *Raft) addTerm() {
	atomic.AddInt64(&rf.currentTerm, 1)
}
