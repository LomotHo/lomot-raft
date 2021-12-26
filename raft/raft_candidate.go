package raft

import (
	"sync/atomic"
	"time"
)

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// GetRandTime(rf.me, VoteTimeout)
	// rf.runCandidate()
	rf.runFollower()
}

func (rf *Raft) runCandidate() {
	rf.addTerm()
	rf.votedFor = rf.me
	// Candidate vote to self
	peerNum := len(rf.peers)
	var voteFinished int32 = 0
	voteNum := 1
	voteTimeoutTimer := time.NewTimer(GetRandTime(rf.me, VoteTimeout))
	defer voteTimeoutTimer.Stop()
	tickerVoteC := make(chan bool)

	sendRequestVoteRpc := func(index int, voteFinished *int32, voteCountC chan bool, arg *RequestVoteArgs) {
		reply := RequestVoteReply{}
		if ok := rf.sendRequestVote(index, arg, &reply); ok {
			if atomic.LoadInt32(voteFinished) == 1 {
				return
			}
			rf.Log("vote reply from ", index, reply)
			if reply.VoteGranted {
				voteCountC <- true
			}
		}
	}
	for i := 0; i < peerNum; i++ {
		if i == rf.me {
			continue
		}
		lastLogIndex := atomic.LoadInt64(&rf.lastApplied)
		arg := RequestVoteArgs{
			Term:         rf.getTerm(),
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  atomic.LoadInt64(&rf.logs[lastLogIndex].Term),
		}
		go sendRequestVoteRpc(i, &voteFinished, tickerVoteC, &arg)
	}

	for !rf.killed() {
		select {
		case <-tickerVoteC:
			voteNum++
			rf.Log("has voteNum ", voteNum)
			if voteNum >= (peerNum/2)+1 {
				rf.Log("get enougth vote , go Leader")
				atomic.StoreInt32(&voteFinished, 1)
				// rf.votedFor = -1
				rf.Turn(0)
				return
			}
		case entry := <-rf.entryC:
			reply := rf.handleEntry(entry.Req)
			entry.ReplyC <- reply
			if reply.Success || reply.Term < rf.currentTerm {
				rf.Log("get entry, go Follower, entry.Req.Term:", entry.Req.Term)
				rf.Turn(2)
				return
			} else {
				rf.Log("Candidate get older Leader, Term:", entry.Req.Term)
			}
		case vote := <-rf.voteC:
			reply := rf.handleVote(vote.Req)
			vote.ReplyC <- reply
			if reply.VoteGranted || reply.Term < rf.currentTerm {
				rf.Log("get Vote, go Follower, vote.Req.Term:", vote.Req.Term)
				rf.Turn(2)
				return
			}
		case <-voteTimeoutTimer.C:
			atomic.StoreInt32(&voteFinished, 1)
			rf.Log("vote timeout , go Candidate again")
			rf.Turn(1)
			return
		}
	}
	rf.Log("receive rf.killed!")
	atomic.StoreInt32(&voteFinished, 1)
}
