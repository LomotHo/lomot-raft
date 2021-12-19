package raft

import "sync/atomic"

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
	Term    int64
	Success bool
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
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) handleEntry(req AppendEntriesArgs) AppendEntriesReply {
	currentTerm := rf.getTerm()
	if currentTerm <= req.Term {
		rf.setTerm(req.Term)
		rf.leaderId = req.LeaderId
		if req.LeaderCommit > rf.commitIndex {
			if rf.lastApplied < req.LeaderCommit {
				rf.commitIndex = rf.lastApplied
			} else {
				rf.commitIndex = req.LeaderCommit
			}
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.commitIndex].Command,
				CommandIndex: int(rf.commitIndex),
			}
			rf.Log("log commited", rf.logs)
		}
		if len(req.Entries) != 0 {
			// if req.PrevLogIndex ==-1 {}
			// rf.Log(req.PrevLogIndex, rf.lastApplied, req.PrevLogTerm, rf.logs[rf.lastApplied].Term)
			if req.PrevLogIndex == rf.lastApplied && req.PrevLogTerm == rf.logs[rf.lastApplied].Term {
				rf.logs = append(rf.logs, req.Entries...)
				// rf.lastApplied += len(req.Entries)
				// rf.commitIndex = req.LeaderCommit
				atomic.AddInt64(&rf.lastApplied, int64(len(req.Entries)))
				atomic.StoreInt64(&rf.commitIndex, req.LeaderCommit)
				rf.Log("log appended", rf.logs)
			}
		}
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

// func (rf *Raft) writeLog(entry Entry) {
// 	rf.lastApplied++
// 	rf.logs = append(rf.logs, entry)
// }
