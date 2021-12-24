package raft

import (
	"sync/atomic"
	"time"
)

type appendEntriesCountType int
type appendEntriesCount struct {
	Kind         appendEntriesCountType
	Peer         int
	AppliedIndex int64
	DebugInfo    string
}

const (
	// appendEntriesCountType
	// HEARTBEAT_FAILED      appendEntriesCountType = 0
	HEARTBEAT_OK          appendEntriesCountType = 1
	APPEND_ENTRIES_FAILED appendEntriesCountType = 2
	APPEND_ENTRIES_OK     appendEntriesCountType = 3

	// other
	SEND_APPEND_ENTRIES_RETRY_NUM = 20
)

func (rf *Raft) sendAppendEntriesRpc(serverId int, leaderClosed *int32, countC chan appendEntriesCount, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverId, args, &reply)
	if atomic.LoadInt32(leaderClosed) == 1 {
		return
	}
	if ok && reply.Success {
		if len(args.Entries) != 0 {
			countC <- appendEntriesCount{Kind: APPEND_ENTRIES_OK, Peer: serverId, AppliedIndex: args.PrevLogIndex + int64(len(args.Entries))}
		} else {
			countC <- appendEntriesCount{Kind: HEARTBEAT_OK, Peer: serverId}
		}
	} else {
		if len(args.Entries) != 0 {
			countC <- appendEntriesCount{Kind: APPEND_ENTRIES_FAILED, Peer: serverId, DebugInfo: reply.DebugInfo}
		}
	}
}

func (rf *Raft) runLeader() {
	term := rf.getTerm()
	me := rf.me
	peerNum := len(rf.peers)
	sendAppendEntriesRetryNum := make([]int, peerNum)
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		if rf.commitIndex > 1 {
			rf.nextIndex[i] = rf.commitIndex
		}
	}
	var appendEntriesCountC = make(chan appendEntriesCount, 1024)
	heartbeatTicker := time.NewTicker(GetTimeInterval(HeartBeatTimeout))
	defer heartbeatTicker.Stop()
	var leaderClosed int32 = 0
	var heartbeatOkArr []bool
	// var heartbeatFailedNum int = 0
	var heartbeatCommitNum int = 0
	var appendEntriesFailedNum int = 0
	// var appendEntriesCommitNum int = 0

	sendAppendEntriesRpcWarp := func(serverId int) {
		nextIndex := rf.nextIndex[serverId]
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     me,
			Entries:      rf.logs[nextIndex:],
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.logs[nextIndex-1].Term,
		}
		go rf.sendAppendEntriesRpc(serverId, &leaderClosed, appendEntriesCountC, &args)
		// sendAppendEntriesRetryNum[serverId] = 0
	}

	for !rf.killed() {
		select {
		case <-heartbeatTicker.C:
			// 每次发心跳前清空上一次的心跳记录
			heartbeatOkArr = make([]bool, peerNum)
			appendEntriesCountC <- appendEntriesCount{Kind: HEARTBEAT_OK, Peer: me}
			for i := 0; i < peerNum; i++ {
				if i != me && sendAppendEntriesRetryNum[i] == 0 {
					sendAppendEntriesRpcWarp(i)
				}
			}
			sendAppendEntriesRetryNum = make([]int, peerNum)
		case cnt := <-appendEntriesCountC:
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
			// case HEARTBEAT_FAILED:
			// 	heartbeatFailedNum++
			// 	if heartbeatFailedNum%10 == 0 {
			// 		rf.Log("heartbeat not ok ", heartbeatFailedNum)
			// 	}
			case APPEND_ENTRIES_OK:
				// atomic.AddInt64(&rf.nextIndex[cnt.Peer], int64(cnt.Size))
				atomic.StoreInt64(&rf.matchIndex[cnt.Peer], cnt.AppliedIndex)
				atomic.StoreInt64(&rf.nextIndex[cnt.Peer], cnt.AppliedIndex+1)
				// rf.Log("APPEND_ENTRIES_OK", rf.commitIndex)
				for i := rf.commitIndex; i < rf.lastApplied; i++ {
					entriesOkCnt := 0
					for _, peerIndex := range rf.matchIndex {
						if i < peerIndex {
							entriesOkCnt++
							if entriesOkCnt >= (peerNum/2)+1 {
								// rf.commitIndex++
								atomic.AddInt64(&rf.commitIndex, 1)
								rf.applyCh <- ApplyMsg{
									CommandValid: true,
									Command:      rf.logs[rf.commitIndex].Command,
									CommandIndex: int(rf.commitIndex),
								}
								rf.Log("leader commited", rf.logs[rf.commitIndex])
								break
							}
						}
					}
				}
			case APPEND_ENTRIES_FAILED:
				// rf.Log("appendEntries FAILED, ID: ", cnt.Peer, cnt.DebugInfo)
				appendEntriesFailedNum++
				if appendEntriesFailedNum%10 == 0 {
					rf.Log("appendEntries FAILED ", appendEntriesFailedNum)
				}
				nextIndex := atomic.LoadInt64(&rf.nextIndex[cnt.Peer])
				matchIndex := atomic.LoadInt64(&rf.matchIndex[cnt.Peer])
				if nextIndex > 1 {
					atomic.StoreInt64(&rf.nextIndex[cnt.Peer], nextIndex/2)
					if nextIndex/2 <= matchIndex {
						atomic.StoreInt64(&rf.matchIndex[cnt.Peer], nextIndex/2-1)
					}
				}
				if sendAppendEntriesRetryNum[cnt.Peer] < SEND_APPEND_ENTRIES_RETRY_NUM {
					sendAppendEntriesRpcWarp(cnt.Peer)
					sendAppendEntriesRetryNum[cnt.Peer]++
				}
			}
		case entry := <-rf.entryC:
			rf.Log("!!!Leader get entry!!!, Term:", entry.Req.Term)
			reply := rf.handleEntry(entry.Req)
			entry.ReplyC <- reply
			if reply.Success || reply.Term < rf.currentTerm {
				rf.Log("Leader get Vote, go Follower")
				atomic.StoreInt32(&leaderClosed, 1)
				rf.Turn(2)
				return
			}
		case vote := <-rf.voteC:
			reply := rf.handleVote(vote.Req)
			vote.ReplyC <- reply
			if reply.VoteGranted || reply.Term < rf.currentTerm {
				rf.Log("Leader get Vote, go Follower, vote.Req.Term:", vote.Req.Term)
				atomic.StoreInt32(&leaderClosed, 1)
				rf.Turn(2)
				return
			}
		case startCommand := <-rf.commandC:
			// rf.Log("command type: ", reflect.TypeOf(command))
			// var entry Entry
			// if v, ok := command.(int); ok {
			// 	entry = Entry{Term: term, Command: v}
			// } else {
			// 	log.Panic("err type of command")
			// }
			entry := Entry{Term: term, Command: startCommand.Command}
			atomic.AddInt64(&rf.lastApplied, 1)
			startCommand.ReqC <- int(atomic.LoadInt64(&rf.lastApplied))
			rf.logs = append(rf.logs, entry)
			index := len(rf.logs)
			atomic.StoreInt64(&rf.nextIndex[rf.me], int64(index+1))
			atomic.StoreInt64(&rf.matchIndex[rf.me], int64(index))
			// rf.Log("new log: ", rf.logs[1:])
			rf.Log("new log: ", entry)
		}
	}
	rf.Log("receive rf.killed!")
	atomic.StoreInt32(&leaderClosed, 1)
}
