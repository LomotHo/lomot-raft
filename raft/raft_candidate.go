package raft

import (
	"sync/atomic"
	"time"
)

func (rf *Raft) runCandidate() {
	rf.votedFor = -1
	rf.ticker()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.addTerm()
	peerNum := len(rf.peers)
	var voteFinished int32 = 0
	rf.votedFor = rf.me
	// Candidate vote to self
	voteNum := 1
	voteTimeoutTimer := time.NewTimer(GetRandTime(rf.me, VoteTimeout))
	defer voteTimeoutTimer.Stop()
	tickerVoteC := make(chan bool)

	sendRequestVoteRPC := func(index int, tickerVoteC chan bool, voteFinished *int32) {
		reply := RequestVoteReply{}
		if ok := rf.sendRequestVote(index, &RequestVoteArgs{
			Term:        rf.getTerm(),
			CandidateId: rf.me,
		}, &reply); ok {
			if atomic.LoadInt32(voteFinished) == 1 {
				return
			}
			rf.Log("vote reply from ", index, reply)
			if reply.VoteGranted {
				tickerVoteC <- true
			}
		}
	}
	for i := 0; i < peerNum; i++ {
		if i == rf.me {
			continue
		}
		go sendRequestVoteRPC(i, tickerVoteC, &voteFinished)
	}

	for !rf.killed() {
		select {
		case <-tickerVoteC:
			voteNum++
			rf.Log("has voteNum ", voteNum)
			if voteNum >= (peerNum/2)+1 {
				rf.Log("get enougth vote , go Leader")
				atomic.StoreInt32(&voteFinished, 1)
				rf.votedFor = -1
				rf.Turn(0)
				return
			}
		case entry := <-rf.entryC:
			reply := rf.handleEntry(entry.Req)
			entry.ReplyC <- reply
			if reply.Success {
				rf.Log("get entry, go Follower, entry.Req.Term:", entry.Req.Term)
				rf.votedFor = -1
				rf.Turn(2)
				return
			} else {
				rf.Log("Candidate get older Leader, Term:", entry.Req.Term)
			}
		case vote := <-rf.voteC:
			reply := rf.handleVote(vote.Req)
			vote.ReplyC <- reply
			if reply.VoteGranted {
				rf.Log("get Vote, go Follower, vote.Req.Term:", vote.Req.Term)
				rf.votedFor = -1
				rf.Turn(2)
				return
			}
		case <-voteTimeoutTimer.C:
			atomic.StoreInt32(&voteFinished, 1)
			rf.Log("vote timeout , go Candidate again")
			rf.votedFor = -1
			rf.Turn(1)
			return
		}
	}
	rf.Log("receive rf.killed!")
	atomic.StoreInt32(&voteFinished, 1)
}
