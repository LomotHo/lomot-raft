package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) runFollower() {
	// rf.votedFor = -1
	randOffset := rand.Intn(HeartBeatTimeout / 5)
	// randOffset := 10 * rf.me
	heartbeatTimeout := time.Duration(HeartBeatTimeout+randOffset) * time.Millisecond
	timer := time.NewTimer(heartbeatTimeout)
	defer timer.Stop()
	for !rf.killed() {
		select {
		case entry := <-entryC:
			reply := rf.handleEntry(entry.Req)
			entry.ReplyC <- reply
			if reply.Success {
				timer.Reset(heartbeatTimeout)
			}
		case vote := <-voteC:
			reply := rf.handleVote(vote.Req)
			vote.ReplyC <- reply
			if reply.VoteGranted {
				timer.Reset(heartbeatTimeout)
			}
		case <-timer.C:
			rf.Log("Follower heartbeat timeout, go Candidate")
			go rf.state.Turn(1)
			return
		}
	}
	rf.Log("receive rf.killed!")
}
