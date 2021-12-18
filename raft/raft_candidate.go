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
	rf.votedFor = rf.me
	peerNum := len(rf.peers)
	voteTimeoutTimer := time.NewTimer(VoteTimeout)
	var voteFinished int32 = 0
	voteNum := 0
	tickerVoteC := make(chan bool)
	for i := 0; i < peerNum; i++ {
		if i == rf.me {
			voteNum++
			// rf.Log("vote to self, has voteNum ", voteNum)
			continue
		}
		go func(index int, voteFinished *int32) {
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(index, &RequestVoteArgs{
				Term:        rf.getTerm(),
				CandidateId: rf.me,
			}, &reply); ok {
				rf.Log("vote reply from ", index, reply)
				if reply.VoteGranted {
					if atomic.LoadInt32(voteFinished) == 1 {
						// rf.Log("recv vote from ", index, " drop")
						return
					}
					tickerVoteC <- true
					// rf.Log("recv vote from ", index)
				}
			}
		}(i, &voteFinished)
	}

	for !rf.killed() {
		select {
		case <-tickerVoteC:
			voteNum++
			rf.Log("has voteNum ", voteNum)
			if voteNum >= (peerNum/2)+1 {
				// became leader
				rf.Log("get enougth vote , go Leader")
				atomic.StoreInt32(&voteFinished, 1)
				rf.votedFor = -1
				rf.state.Turn(0)
				return
			}
		case entry := <-entryC:
			reply := rf.handleEntry(entry.Req)
			entry.ReplyC <- reply
			if reply.Success {
				rf.Log("get entry, go Follower, entry.Req.Term:", entry.Req.Term)
				rf.votedFor = -1
				rf.state.Turn(2)
				return
			} else {
				rf.Log("Candidate get older Leader, Term:", entry.Req.Term)
			}
		case vote := <-voteC:
			reply := rf.handleVote(vote.Req)
			vote.ReplyC <- reply
			if reply.VoteGranted {
				rf.Log("get Vote, go Follower, vote.Req.Term:", vote.Req.Term)
				rf.votedFor = -1
				rf.state.Turn(2)
				return
			}
		case <-voteTimeoutTimer.C:
			atomic.StoreInt32(&voteFinished, 1)
			rf.Log("vote timeout , go Candidate again")
			rf.votedFor = -1
			rf.state.Turn(1)
			return
		}
	}
	rf.Log("receive rf.killed!")
	atomic.StoreInt32(&voteFinished, 1)
}
