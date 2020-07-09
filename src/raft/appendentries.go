package raft

import (
	"time"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("AppendEntries")
	defer rf.unlock()
	reply.Term = rf.term
	reply.Success = false
	if args.Term < rf.term {
		rf.DPrintf("leader expired")
		return
	}
	if args.Term > rf.term {
		rf.term = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randElectionTimeout())

	lastIndex := len(rf.logEntries) - 1
	if args.PrevLogIndex > lastIndex {
		rf.DPrintf("missing log (%d < %d)", lastIndex, args.PrevLogIndex)
		reply.NextIndex = lastIndex + 1
		return
	}
	if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.DPrintf("log doesn't contain match term (%d != %d)",
			rf.logEntries[args.PrevLogIndex].Term, args.PrevLogTerm)
		reply.NextIndex = args.PrevLogIndex
		return
	}
	lastNewIndex := args.PrevLogIndex
	for i, entry := range args.Entries {
		cur := args.PrevLogIndex + 1 + i
		if cur < len(rf.logEntries) {
			if rf.logEntries[cur].Term != entry.Term {
				rf.DPrintf("term conflict log[%d].Term %d != %d",
					cur, rf.logEntries[cur].Term, entry.Term)
				rf.logEntries[cur] = entry
				rf.logEntries = rf.logEntries[:cur+1]
				lastNewIndex = cur
			}
		} else {
			rf.logEntries = append(rf.logEntries, entry)
			lastNewIndex = cur
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, lastNewIndex)
		// fmt.Printf("Server %d applyNotifyCh 0\n", rf.me)
		rf.applyNotifyCh <- struct{}{}
		// fmt.Printf("Server %d applyNotifyCh 1\n", rf.me)
	}
	reply.NextIndex = lastNewIndex + 1
	reply.Success = true
	rf.DPrintf("append success, next index %d", reply.NextIndex)
	rf.DPrintf("log entries num: %d, commit index: %d", len(rf.logEntries), rf.commitIndex)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	okCh := make(chan bool)
	go func() {
		okCh <- rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}()
	select {
	case <-time.After(RPCTimeout):
		rf.DPrintf("server %d AppendEntries RPC timeout", server)
		reply.Success = false
		reply.Term = args.Term
		return false
	case ok := <-okCh:
		if !ok {
			reply.Success = false
			reply.Term = args.Term
		}
		return ok
	}
}

func (rf *Raft) heartbeat() {
	rf.DPrintf("start heartbeat")
	timerCh := time.Tick(HeartbeatInterval)
	for {
		rf.lock("heartbeat 0")
		if rf.state != Leader || rf.killed() {
			rf.unlock()
			return
		}
		rf.DPrintf("send heartbeat")
		rf.DPrintf("log entries num: %d, commit index: %d", len(rf.logEntries), rf.commitIndex)
		rf.unlock()
		for i := range rf.peers {
			if i == rf.me {
				rf.electionTimer.Stop()
				rf.electionTimer.Reset(randElectionTimeout())
				continue
			}
			go func(server int) {
				rf.lock("heartbeat 1")
				next := rf.nextIndex[server]
				args := AppendEntriesArgs{
					Term:         rf.term,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
					Entries:      nil,
					PrevLogIndex: next - 1,
					PrevLogTerm:  rf.logEntries[next-1].Term,
				}
				if next < len(rf.logEntries) {
					args.Entries = append(args.Entries, rf.logEntries[next:]...)
				}
				rf.unlock()
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)
				rf.DPrintf("server %d append reply: %#v", server, reply)
				rf.lock("heartbeat 2")
				if !reply.Success {
					if reply.Term > rf.term {
						rf.term = reply.Term
						rf.state = Follower
						rf.votedFor = -1
						rf.electionTimer.Stop()
						rf.electionTimer.Reset(randElectionTimeout())
					} else if ok {
						rf.nextIndex[server] = reply.NextIndex
						rf.unlock()
						return
					}
					rf.unlock()
					return
				}
				rf.nextIndex[server] = reply.NextIndex
				rf.matchIndex[server] = reply.NextIndex - 1
				rf.unlock()
			}(i)
		}
		<-timerCh
	}
}

func (rf *Raft) leaderCommit() {
	rf.DPrintf("start leader commit check")
	timerCh := time.Tick(CommitInterval)
	for {
		<-timerCh
		rf.lock("leaderCommit")
		if rf.state != Leader || rf.killed() {
			rf.unlock()
			return
		}
		rf.DPrintf("leader check commit")
		rf.DPrintf("nextIndex: %#v, matchIndex: %#v", rf.nextIndex, rf.matchIndex)
		n := len(rf.logEntries) - 1
		majority := len(rf.peers)/2 + 1
		for i := n; i > rf.commitIndex; i-- {
			replicated := 0
			if rf.logEntries[i].Term != rf.term {
				break
			}
			for server := range rf.peers {
				if rf.matchIndex[server] >= i {
					replicated++
				}
			}
			// rf.DPrintf("replicated:peers=%d:%d", replicated, len(rf.peers))
			if replicated >= majority {
				rf.commitIndex = i
				// fmt.Printf("Server %d applyNotifyCh 0\n", rf.me)
				rf.applyNotifyCh <- struct{}{}
				// fmt.Printf("Server %d applyNotifyCh 1\n", rf.me)
				break
			}
		}
		rf.DPrintf("after apply commit index %d", rf.commitIndex)
		rf.unlock()
	}
}

func (rf *Raft) doApply() {
	rf.lock("doApply")
	defer rf.unlock()
	rf.DPrintf("start apply")
	applied := rf.lastApplied
	committed := rf.commitIndex
	if applied < committed {
		for i, entry := range rf.logEntries[applied+1 : committed+1] {
			msg := ApplyMsg{
				Command:      entry.Command,
				CommandIndex: applied + 1 + i,
				CommandValid: true,
			}
			// fmt.Printf("Server %d applyCh 0\n", rf.me)
			rf.applyCh <- msg
			// fmt.Printf("Server %d applyCh 1\n", rf.me)
		}
		rf.lastApplied = committed
	}
}
