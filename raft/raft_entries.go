package raft

import (
	"sync/atomic"
)

type Entry struct {
	Term int64
	// Command int
	Command interface{}
}
type AppendEntriesArgs struct {
	Term         int64
	LeaderId     int
	Entries      []Entry
	LeaderCommit int64
	PrevLogIndex int64
	PrevLogTerm  int64
}
type AppendEntriesReply struct {
	Term      int64
	Success   bool
	DebugInfo string
}
type AppendEntryWarp struct {
	Req    AppendEntriesArgs
	ReplyC chan AppendEntriesReply
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}
	replyC := make(chan AppendEntriesReply)
	rf.entryC <- AppendEntryWarp{
		Req:    *args,
		ReplyC: replyC,
	}
	*reply = <-replyC
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) handleEntry(req AppendEntriesArgs) AppendEntriesReply {
	currentTerm := rf.getTerm()

	if currentTerm <= req.Term {
		rf.setTerm(req.Term)
		rf.votedFor = -1
	} else {
		return AppendEntriesReply{
			Term:      currentTerm,
			Success:   false,
			DebugInfo: "old term",
		}
	}
	if rf.GetRole() != 0 {
		rf.leaderId = req.LeaderId
		// rf.Log("req: ", req)
		if rf.lastApplied >= req.PrevLogIndex && rf.logs[req.PrevLogIndex].Term == req.PrevLogTerm {
			if len(req.Entries) != 0 {
				// race
				rf.logs = append(rf.logs[:req.PrevLogIndex+1], req.Entries...)
				// rf.lastApplied += len(req.Entries)
				// rf.commitIndex = req.LeaderCommit
				atomic.StoreInt64(&rf.lastApplied, req.PrevLogIndex+int64(len(req.Entries)))
				// atomic.StoreInt64(&rf.commitIndex, req.LeaderCommit)
				// rf.Log("log appended", rf.logs[1:])
				rf.Log("log appended", req.Entries)
			}
			// check commit
			if req.LeaderCommit > rf.commitIndex && rf.lastApplied > rf.commitIndex {
				oldCommitIndex := rf.commitIndex
				if req.LeaderCommit > rf.lastApplied {
					rf.commitIndex = rf.lastApplied
				} else {
					rf.commitIndex = req.LeaderCommit
				}
				for i := oldCommitIndex + 1; i <= rf.commitIndex; i++ {
					rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.logs[i].Command,
						CommandIndex: int(i),
					}
					rf.Log("log commited", rf.logs[i])
				}
			}
			rf.persist()
			return AppendEntriesReply{
				Term:    req.Term,
				Success: true,
			}
		}
		// rf.Log("PrevLogIndex err req: ", req, " log: ", rf.logs)
		return AppendEntriesReply{
			Term:      currentTerm,
			Success:   false,
			DebugInfo: "PrevLogIndex err",
		}
	} else {
		return AppendEntriesReply{
			Term:      currentTerm,
			Success:   false,
			DebugInfo: "master get newer entries",
		}
	}
}
