package raft

import (
	"time"
)

func (rf *Raft) runFollower() {
	// rf.votedFor = -1
	// heartbeatTimeout := time.Duration(HeartBeatTimeout+randOffset) * time.Millisecond
	heartbeatTimeout := GetRandTime(rf.me, HeartBeatTimeout)
	timer := time.NewTimer(heartbeatTimeout)
	defer timer.Stop()
	for !rf.killed() {
		select {
		case entry := <-rf.entryC:
			reply := rf.handleEntry(entry.Req)
			entry.ReplyC <- reply
			if reply.Success {
				timer.Reset(heartbeatTimeout)
			}
		case vote := <-rf.voteC:
			reply := rf.handleVote(vote.Req)
			vote.ReplyC <- reply
			if reply.VoteGranted {
				timer.Reset(heartbeatTimeout)
			}
		case <-timer.C:
			rf.Log("Follower heartbeat timeout, go Candidate")
			go rf.Turn(1)
			return
		}
	}
	rf.Log("receive rf.killed!")
}
