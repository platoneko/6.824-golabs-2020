package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const Debug = 0
const DeadlockCheck = 1

func (rf *Raft) lock(m string) {
	rf.mu.Lock()
	if DeadlockCheck > 0 {
		go func() {
			select {
			case <-time.After(MutexTimeout):
				log.Printf("Server %d: lock timeout (%s)", rf.me, m)
			case <-rf.unlockCh:
				return
			case <-rf.killCh:
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

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func (rf *Raft) getEntry(idx int) LogEntry {
	return rf.logEntries[idx-rf.lastIncludedIndex]
}

func (rf *Raft) getEntryTerm(idx int) int {
	return rf.logEntries[idx-rf.lastIncludedIndex].Term
}

func (rf *Raft) toSliceIndex(idx int) int {
	return idx - rf.lastIncludedIndex
}

func (rf *Raft) toEntryIndex(idx int) int {
	return idx + rf.lastIncludedIndex
}

func (rf *Raft) getLastIndex() int {
	return rf.lastIncludedIndex + len(rf.logEntries) - 1
}

func (rf *Raft) getLastTerm() int {
	return rf.logEntries[len(rf.logEntries)-1].Term
}

func (rf *Raft) getEntriesLength() int {
	return rf.getLastIndex() + 1
}