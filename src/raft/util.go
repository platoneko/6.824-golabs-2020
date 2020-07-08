package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		var role string
		switch rf.role {
		case Follower:
			role = "Follower"
		case Candidate:
			role = "Candidate"
		case Leader:
			role = "Leader"
		default:
			panic("Unknow role")
		}
		msg := fmt.Sprintf(format, a...)
		log.Printf("Server %d (Term %d, Role %s):\n%s", rf.me, rf.term, role, msg)
	}
	return
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
