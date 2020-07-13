package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const Debug = 1
const DeadlockCheck = 0

func (rf *Raft) lock(m string) {
	rf.mu.Lock()
	if DeadlockCheck > 0 {
		go func() {
			select {
			case <-time.After(MutexTimeout):
				log.Printf("Server %d: lock timeout (%s)", rf.me, m)
			case <-rf.unlockCh:
				return
			}
		}()
	}
}

func (rf *Raft) unlock() {
	if DeadlockCheck > 0 {
		rf.unlockCh <- struct{}{}
	}
	rf.mu.Unlock()
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		var state string
		switch rf.state {
		case Follower:
			state = "Follower"
		case Candidate:
			state = "Candidate"
		case Leader:
			state = "Leader"
		default:
			panic("Unknow state")
		}
		msg := fmt.Sprintf(format, a...)
		log.Printf("Server %d (Term %d, Role %s):\n%s", rf.me, rf.term, state, msg)
	}
	return
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
