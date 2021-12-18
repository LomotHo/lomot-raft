package raft

import (
	"sync/atomic"
	"time"
)

func (rf *Raft) runLeader() {
	type countType int
	type count struct {
		Kind countType
		Peer int
	}
	const (
		HEARTBEAT_FAILED countType = 0
		HEARTBEAT_OK     countType = 1
	)
	var countC = make(chan count, 1024)
	heartbeatTicker := time.NewTicker(HeartBeatInterval)
	defer heartbeatTicker.Stop()
	var leaderClosed int32 = 0
	term := rf.getTerm()
	me := rf.me
	peerNum := len(rf.peers)
	var heartbeatOkArr []bool
	var heartbeatFailedNum int = 0
	var heartbeatCommitNum int = 0

	sendHeartbeat := func(index int, term int64, countC chan count, leaderClosed *int32) {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(index, &AppendEntriesArgs{
			Term: term,
		}, &reply)
		if atomic.LoadInt32(leaderClosed) == 1 {
			// rf.Log("close old leader heartbeatTicker func")
			return
		} else if ok {
			countC <- count{Kind: HEARTBEAT_OK, Peer: index}
		} else {
			countC <- count{Kind: HEARTBEAT_FAILED, Peer: index}
		}
	}

	for !rf.killed() {
		select {
		case <-heartbeatTicker.C:
			// 每次发心跳前清空上一次的心跳记录
			heartbeatOkArr = make([]bool, peerNum)
			countC <- count{Kind: HEARTBEAT_OK, Peer: me}
			for i := 0; i < peerNum; i++ {
				if i == me {
					continue
				}
				go sendHeartbeat(i, term, countC, &leaderClosed)
			}
		case cnt := <-countC:
			switch cnt.Kind {
			case HEARTBEAT_OK:
				heartbeatOkArr[cnt.Peer] = true
				heartbeatOkNum := 0
				for _, v := range heartbeatOkArr {
					if v {
						heartbeatOkNum++
					}
				}
				if heartbeatOkNum == (peerNum/2)+1 {
					heartbeatCommitNum++
					if heartbeatCommitNum%20 == 0 {
						rf.Log("heartbeat AppendEntries commit", heartbeatCommitNum)
					}
				}
			case HEARTBEAT_FAILED:
				heartbeatFailedNum++
				if heartbeatFailedNum%10 == 0 {
					rf.Log("heartbeat not ok ", heartbeatFailedNum)
				}
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
	rf.Log("receive rf.killed!")
	atomic.StoreInt32(&leaderClosed, 1)
	return
}
