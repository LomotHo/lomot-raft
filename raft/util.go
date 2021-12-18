package raft

import (
	"fmt"
	"log"
	"os"
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
	}
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
