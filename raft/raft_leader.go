package raft

import (
	"sync/atomic"
	"time"
)

type appendEntriesCountType int
type appendEntriesCount struct {
	Kind appendEntriesCountType
	Peer int
	// Index int
	Size int
}

const (
	HEARTBEAT_FAILED      appendEntriesCountType = 0
	HEARTBEAT_OK          appendEntriesCountType = 1
	APPEND_ENTRIES_FAILED appendEntriesCountType = 2
	APPEND_ENTRIES_OK     appendEntriesCountType = 3
)

func (rf *Raft) runLeader() {
	term := rf.getTerm()
	me := rf.me
	peerNum := len(rf.peers)

	sendHeartbeatRPC := func(serverId int, term int64, countC chan appendEntriesCount, leaderClosed *int32) {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(serverId, &AppendEntriesArgs{
			Term:         term,
			LeaderId:     me,
			LeaderCommit: rf.commitIndex,
		}, &reply)
		if atomic.LoadInt32(leaderClosed) == 1 {
			// rf.Log("close old leader heartbeatTicker func")
			return
		} else if ok {
			countC <- appendEntriesCount{Kind: HEARTBEAT_OK, Peer: serverId}
		} else {
			countC <- appendEntriesCount{Kind: HEARTBEAT_FAILED, Peer: serverId}
		}
	}
	var appendEntriesCountC = make(chan appendEntriesCount, 1024)
	heartbeatTicker := time.NewTicker(GetTimeInterval(HeartBeatTimeout))
	defer heartbeatTicker.Stop()
	var leaderClosed int32 = 0
	var heartbeatOkArr []bool
	var heartbeatFailedNum int = 0
	var heartbeatCommitNum int = 0
	var appendEntriesFailedNum int = 0
	// var appendEntriesCommitNum int = 0

	for !rf.killed() {
		select {
		case <-heartbeatTicker.C:
			// 每次发心跳前清空上一次的心跳记录
			heartbeatOkArr = make([]bool, peerNum)
			appendEntriesCountC <- appendEntriesCount{Kind: HEARTBEAT_OK, Peer: me}
			for i := 0; i < peerNum; i++ {
				if i == me {
					continue
				}
				go sendHeartbeatRPC(i, term, appendEntriesCountC, &leaderClosed)
			}
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
			case HEARTBEAT_FAILED:
				heartbeatFailedNum++
				if heartbeatFailedNum%10 == 0 {
					rf.Log("heartbeat not ok ", heartbeatFailedNum)
				}
			case APPEND_ENTRIES_OK:
				// rf.matchIndex[cnt.Peer] += cnt.Size
				atomic.AddInt64(&rf.matchIndex[cnt.Peer], int64(cnt.Size))
				rf.Log("APPEND_ENTRIES_OK", rf.commitIndex)
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
								rf.Log("log commited", rf.logs)
								break
							}
						}
					}
				}
				rf.Log("master log", rf.logs)

			case APPEND_ENTRIES_FAILED:
				appendEntriesFailedNum++
				if appendEntriesFailedNum%10 == 0 {
					rf.Log("appendEntries not ok ", appendEntriesFailedNum)
				}
			}
		case entry := <-rf.entryC:
			rf.Log("!!!Leader get entry!!!, Term:", entry.Req.Term)
			reply := rf.handleEntry(entry.Req)
			entry.ReplyC <- reply
			if reply.Success {
				rf.Log("Leader get Vote, go Follower")
				atomic.StoreInt32(&leaderClosed, 1)
				rf.Turn(2)
				return
			}
		case vote := <-rf.voteC:
			reply := rf.handleVote(vote.Req)
			vote.ReplyC <- reply
			if reply.VoteGranted {
				rf.Log("Leader get Vote, go Follower, vote.Req.Term:", vote.Req.Term)
				atomic.StoreInt32(&leaderClosed, 1)
				rf.Turn(2)
				return
			}
		case command := <-rf.commandC:
			// rf.Log("command type: ", reflect.TypeOf(command))
			// var entry Entry
			// if v, ok := command.(int); ok {
			// 	entry = Entry{Term: term, Command: v}
			// } else {
			// 	log.Panic("err type of command")
			// }
			entry := Entry{Term: term, Command: command}

			atomic.AddInt64(&rf.lastApplied, 1)
			// rf.lastApplied++
			rf.logs = append(rf.logs, entry)
			index := len(rf.logs)
			// rf.nextIndex[rf.me] = index + 1
			// rf.matchIndex[rf.me] = index
			atomic.StoreInt64(&rf.nextIndex[rf.me], int64(index+1))
			atomic.StoreInt64(&rf.matchIndex[rf.me], int64(index))

			rf.Log(rf.logs)
			for i := 0; i < peerNum; i++ {
				if i == me {
					continue
				}
				peerIndex := rf.matchIndex[i]
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     me,
					Entries:      rf.logs[peerIndex+1:],
					LeaderCommit: rf.commitIndex,
					PrevLogIndex: peerIndex,
					PrevLogTerm:  rf.logs[peerIndex].Term,
				}
				go rf.sendAppendEntriesRPC(i, &leaderClosed, appendEntriesCountC, &args)
			}
		}
	}
	rf.Log("receive rf.killed!")
	atomic.StoreInt32(&leaderClosed, 1)
}

// []Entry{entry}
func (rf *Raft) sendAppendEntriesRPC(serverId int, leaderClosed *int32, countC chan appendEntriesCount, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverId, args, &reply)
	if atomic.LoadInt32(leaderClosed) == 1 {
		return
	}
	if ok && reply.Success {
		countC <- appendEntriesCount{Kind: APPEND_ENTRIES_OK, Peer: serverId, Size: len(args.Entries)}
	} else {
		countC <- appendEntriesCount{Kind: APPEND_ENTRIES_FAILED, Peer: serverId}
	}
}
