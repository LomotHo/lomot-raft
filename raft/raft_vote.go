package raft

import (
	"sync/atomic"
	"time"
)

const VoteTimeout time.Duration = time.Millisecond * 1000
const VoteInterval time.Duration = time.Millisecond * 50
const HeartBeatTimeout = 500
const HeartBeatInterval time.Duration = time.Millisecond * 50

// var HeartBeatC = make(chan bool)
var entryC = make(chan Entry)
var voteC = make(chan Vote)

type Vote struct {
	Req    RequestVoteArgs
	ReplyC chan RequestVoteReply
}
type Entry struct {
	Req    AppendEntriesArgs
	ReplyC chan AppendEntriesReply
}

func (rf *Raft) runLeader() {
	var leaderClosed int32 = 0
	heartBeatNum := 0
	heartBeatCommitNum := 0
	heartBeatFailedNum := 0
	peerNum := len(rf.peers)
	heartbeatTicker := time.NewTicker(HeartBeatInterval)
	defer heartbeatTicker.Stop()
	for !rf.killed() {
		select {
		case <-heartbeatTicker.C:
			for i := 0; i < peerNum; i++ {
				if i == rf.me {
					heartBeatNum++
					continue
				}
				go func(index int, leaderClosed *int32) {
					term := rf.getTerm()
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(index, &AppendEntriesArgs{
						Term: term,
					}, &reply)
					if atomic.LoadInt32(leaderClosed) == 1 {
						rf.Log("close old leader heartbeatTicker func")
						return
					}
					if ok {
						heartBeatNum++
						if heartBeatNum > (peerNum/2)+1 {
							heartBeatNum = 0
							heartBeatCommitNum++
							if heartBeatCommitNum%20 == 0 {
								rf.Log("heartbeat AppendEntries commit", heartBeatCommitNum)
							}
						}
					} else {
						heartBeatFailedNum++
						if heartBeatFailedNum%10 == 0 {
							rf.Log("heartbeat not ok ", heartBeatFailedNum)
						}
					}
				}(i, &leaderClosed)
			}
		case entry := <-entryC:
			rf.Log("!!!Leader get entry!!!, Term:", entry.Req.Term)
			reply := rf.handleEntry(entry.Req)
			entry.ReplyC <- reply
			if reply.Success {
				rf.Log("Leader get Vote, go Follower")
				atomic.StoreInt32(&leaderClosed, 1)
				rf.state.Turn(2)
				return
			}
		case vote := <-voteC:
			reply := rf.handleVote(vote.Req)
			vote.ReplyC <- reply
			if reply.VoteGranted {
				rf.Log("Leader get Vote, go Follower, vote.Req.Term:", vote.Req.Term)
				atomic.StoreInt32(&leaderClosed, 1)
				rf.state.Turn(2)
				return
			}
		}
	}
}

func (rf *Raft) runCandidate() {
	rf.votedFor = -1
	rf.ticker()
}

func (rf *Raft) runFollower() {
	// rf.votedFor = -1
	// randOffset := rand.Intn(HeartBeatTimeout / 5)
	randOffset := 10 * rf.me
	heartBeatTimeout := time.Duration(HeartBeatTimeout+randOffset) * time.Millisecond
	timer := time.NewTimer(heartBeatTimeout)
	for !rf.killed() {
		select {
		case entry := <-entryC:
			reply := rf.handleEntry(entry.Req)
			entry.ReplyC <- reply
			if reply.Success {
				timer.Reset(heartBeatTimeout)
			} else {
				// rf.Log("Follower get old Leader, Term:", entry.Req.Term)
			}
		case vote := <-voteC:
			reply := rf.handleVote(vote.Req)
			vote.ReplyC <- reply
			if reply.VoteGranted {
				timer.Reset(heartBeatTimeout)
			}
		case <-timer.C:
			rf.Log("Follower heartbeat timeout, go Candidate")
			go rf.state.Turn(1)
			return
		}
	}
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
