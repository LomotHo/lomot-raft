package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

const VoteTimeout time.Duration = time.Millisecond * 1000
const VoteInterval time.Duration = time.Millisecond * 50
const HeartBeatTimeout time.Duration = time.Millisecond * 500
const HeartBeatInterval time.Duration = time.Millisecond * 50

var HeartBeatC = make(chan bool)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term <= rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
		return
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else {
		HeartBeatC <- true
		reply.Success = true
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// func (rf *Raft) sendHeartBeat() bool {
// 	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
// 	return ok
// }

func (rf *Raft) checkHeartBeat() {
	timer := time.NewTimer(HeartBeatTimeout)
	for {
		select {
		case <-HeartBeatC:
			timer.Reset(HeartBeatTimeout)
		case <-timer.C:
			go rf.ticker()
			break
		}
	}
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

// type getState struct {

// }
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
			// case s:= <-x:{

			// }
		}
	}

}

func (rfs *RaftState) Turn(role int32) {
	rfs.stateC <- role
}

func (rf *Raft) runLeader() {
	heartBeatNum := 0
	heartBeatCommitNum := 0
	peerNum := len(rf.peers)
	heartbeatTicker := time.NewTicker(HeartBeatInterval)
	defer heartbeatTicker.Stop()
	for {
		select {
		case <-heartbeatTicker.C:
			{
				for i := 0; i < peerNum; i++ {
					go func(index int) {
						reply := AppendEntriesReply{}
						if ok := rf.sendAppendEntries(index, &AppendEntriesArgs{
							Term: rf.currentTerm,
						}, &reply); ok {
							heartBeatNum++
							if heartBeatNum > (peerNum/2)+1 {
								heartBeatNum = 0
								heartBeatCommitNum++
								if heartBeatCommitNum%20 == 0 {
									rf.Log("heartbeat AppendEntries commit", heartBeatCommitNum)
								}
							}
						}
					}(i)
				}
			}

		}

	}

}

func (rf *Raft) runCandidate() {
	// rf.Log("became Candidate")
	rf.ticker()
}

func (rf *Raft) runFollower() {
	ra := rand.Intn(50)
	heartBeatTimeout := HeartBeatTimeout + time.Millisecond*time.Duration(ra)
	timer := time.NewTimer(heartBeatTimeout)
	for {
		select {
		case <-HeartBeatC:
			timer.Reset(heartBeatTimeout)
		case <-timer.C:
			rf.Log("Follower heartbeat timeout, go Candidate")
			go rf.state.Turn(1)
			return
		}
	}
}

func (rf *Raft) Log(v ...interface{}) {
	color := rf.currentTerm % 7
	log.Println(fmt.Sprintf("\033[3%vm[t%vnode%v]\033[0m", color, rf.currentTerm, rf.me), fmt.Sprint(v...))
}
