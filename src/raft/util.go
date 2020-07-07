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
		log.Printf("Server %d (Term %d, Role %s): ", rf.me, rf.term, role)
		log.Printf(format, a...)
	}
	return
}
