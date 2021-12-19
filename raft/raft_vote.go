package raft

import (
	"sync/atomic"
)

// const VoteInterval time.Duration = time.Millisecond * 5
// const HeartBeatInterval time.Duration = time.Millisecond * 5
var VoteTimeout = 100
var HeartBeatTimeout = 100

type Vote struct {
	Req    RequestVoteArgs
	ReplyC chan RequestVoteReply
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}
	replyC := make(chan RequestVoteReply)
	rf.voteC <- Vote{
		Req:    *args,
		ReplyC: replyC,
	}
	*reply = <-replyC
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

func (rf *Raft) SetRole(role int32) {
	atomic.StoreInt32(&rf.role, role)
}
func (rf *Raft) GetRole() int32 {
	return atomic.LoadInt32(&rf.role)
}

// role 0:Leader 1:Candidate 2:Follower
// type RaftState struct {
// 	role int32
// }
func (rf *Raft) Turn(role int32) {
	rf.SetRole(role)
	// rf.stateC <- role
	switch role {
	case 0:
		go rf.runLeader()
	case 1:
		go rf.runCandidate()
	case 2:
		go rf.runFollower()
	}
}
