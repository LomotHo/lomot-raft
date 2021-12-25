package raft

import (
	"bytes"

	"6.824/labgob"
)

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)\
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	rf.persister.SaveRaftState(w.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []Entry
	var currentTerm int64
	var votedFor int

	if err := d.Decode(&logs); err != nil {
		rf.Log("decode logs error ", err.Error())
	} else {
		rf.logs = logs
	}
	if err := d.Decode(&currentTerm); err != nil {
		rf.Log("decode currentTerm error ", err.Error())
	} else {
		rf.currentTerm = currentTerm
	}
	if err := d.Decode(&votedFor); err != nil {
		rf.Log("decode votedFor error ", err.Error())
	} else {
		rf.votedFor = votedFor
	}
}
