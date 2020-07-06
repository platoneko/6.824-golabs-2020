package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf("Server %d (Term %d): ", rf.me, rf.term)
		log.Printf(format, a...)
	}
	return
}
