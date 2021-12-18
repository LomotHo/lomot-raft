package raft

import (
	"fmt"
	"log"
	"sync/atomic"
)

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// if args.Term < rf.currentTerm {
	// 	reply.VoteGranted = false
	// 	return
	// }
	// if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
	// 	rf.currentTerm = args.Term
	// 	rf.Log("Granted Vote to ", args.CandidateId)
	// 	reply.VoteGranted = true
	// 	rf.votedFor = args.CandidateId
	// } else {
	// 	reply.VoteGranted = false
	// 	return
	// }

	replyC := make(chan RequestVoteReply)
	voteC <- Vote{
		Req:    *args,
		ReplyC: replyC,
	}
	*reply = <-replyC
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term int64
}
type AppendEntriesReply struct {
	Term    int64
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if rf.currentTerm > args.Term {
	// 	reply.Success = false
	// 	return
	// } else {
	// 	rf.currentTerm = args.Term
	// 	HeartBeatC <- true
	// 	reply.Success = true
	// 	return
	// }
	// rf.currentTerm = args.Term
	replyC := make(chan AppendEntriesReply)
	entryC <- Entry{
		Req:    *args,
		ReplyC: replyC,
	}
	*reply = <-replyC
	// HeartBeatC <- true
	// reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// role 0:Leader 1:Candidate 2:Follower
type RaftState struct {
	role   int32
	stateC chan int32
}

func (rfs *RaftState) SetState(role int32) {
	atomic.StoreInt32(&rfs.role, role)
}
func (rfs *RaftState) GetState() int32 {
	return atomic.LoadInt32(&rfs.role)
}
func (rfs *RaftState) Turn(role int32) {
	rfs.stateC <- role
}

func (rf *Raft) Run() {
	go rf.runFollower()
	for {
		select {
		case state := <-rf.state.stateC:
			{
				switch state {
				case 0:
					rf.state.SetState(0)
					go rf.runLeader()
				case 1:
					rf.state.SetState(1)
					go rf.runCandidate()
				case 2:
					rf.state.SetState(2)
					go rf.runFollower()
				}
			}
			// case s:= <-x:{}
		}
	}
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func (rf *Raft) Log(v ...interface{}) {
	if Debug {
		// rf.mu.Lock()
		me := rf.me
		// rf.mu.Unlock()
		currentTerm := rf.getTerm()
		state := rf.state.GetState()
		termColor := currentTerm % 7
		nodeColor := me%7 + 2
		log.Println(fmt.Sprintf("\033[4%vm[term%v]\033[0m \033[3%vm[r%vnode%v]\033[0m",
			termColor, currentTerm, nodeColor, state, me), fmt.Sprint(v...))
	}
}
