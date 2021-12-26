package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
)

// Debugging
var Debug bool

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if debug := os.Getenv("GO_DEBUG"); debug == "1" {
		Debug = true
		VoteTimeout = 500
		HeartBeatTimeout = 500
	}
}

func (rf *Raft) Log(v ...interface{}) {
	if Debug {
		// rf.mu.Lock()
		// rf.mu.Unlock()
		commitIndex := atomic.LoadInt64(&rf.commitIndex)
		lastApplied := atomic.LoadInt64(&rf.lastApplied)
		me := rf.me
		currentTerm := rf.getTerm()
		state := rf.GetRole()
		termColor := currentTerm % 7
		nodeColor := me%7 + 2
		log.Println(fmt.Sprintf("\033[4%vm[term%v]\033[0m\033[3%vm[r%vnode%v][%v-%v]\033[0m",
			termColor, currentTerm, nodeColor, state, me, commitIndex, lastApplied), fmt.Sprint(v...))
	}
}

func (rf *Raft) getLogsOneLine(a, b int64) string {
	if b-a <= 4 {
		return fmt.Sprintf(" %v-%v | %v", a, b-1, rf.logs[a:b])
	} else {
		return fmt.Sprintf(" %v-%v | [%v%v--%v%v]", a, b-1, rf.logs[a], rf.logs[a+1], rf.logs[b-2], rf.logs[b-1])
	}
}

// func GetRandTimeOffset(id int, timeoutAmount int) int {
// 	if Debug {
// 		return 10 * id
// 	} else {
// 		return rand.Intn(timeoutAmount / 5)
// 	}
// }

func GetRandTime(id int, timeoutAmount int) time.Duration {
	var randOffset int
	if Debug {
		randOffset = 10 * id
	} else {
		randOffset = rand.Intn(timeoutAmount / 5)
	}
	return time.Duration(timeoutAmount+randOffset) * time.Millisecond
}

func GetTimeInterval(timeoutAmount int) time.Duration {
	return time.Duration(timeoutAmount/10) * time.Millisecond
}
