package raft

type Entry struct {
	Req    AppendEntriesArgs
	ReplyC chan AppendEntriesReply
}

type AppendEntriesArgs struct {
	Term         int64
	LeaderId     int
	Entries      []string
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int64
}
type AppendEntriesReply struct {
	Term    int64
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	replyC := make(chan AppendEntriesReply)
	rf.entryC <- Entry{
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
