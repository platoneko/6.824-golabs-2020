package raft

import (
	"sort"
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
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
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

	defer rf.persist()

	if args.Term > rf.term || rf.state == Candidate {
		rf.term = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randElectionTimeout())

	lastIndex := rf.getLastIndex()
	if args.PrevLogIndex > lastIndex {
		rf.DPrintf("(leader %d) missing log (%d < %d)", args.LeaderId, lastIndex, args.PrevLogIndex)
		reply.ConflictIndex = lastIndex + 1
		reply.ConflictTerm = 0
		return
	}

	if rf.toSliceIndex(args.PrevLogIndex) > 0 &&
		rf.getEntryTerm(args.PrevLogIndex) != args.PrevLogTerm {
		rf.DPrintf("(leader %d) log doesn't contain match term (%d != %d)",
			args.LeaderId, rf.getEntryTerm(args.PrevLogIndex), args.PrevLogTerm)
		reply.ConflictTerm = rf.getEntryTerm(args.PrevLogIndex)
		reply.ConflictIndex = rf.toEntryIndex(max(sort.Search(rf.toSliceIndex(args.PrevLogIndex+1),
			func(i int) bool { return rf.logEntries[i].Term == reply.ConflictTerm }), 1))
		return
	}

	for i, entry := range args.Entries {
		cur := rf.toSliceIndex(args.PrevLogIndex + 1 + i)
		if cur <= 0 {
			continue
		}
		if cur < len(rf.logEntries) {
			if rf.logEntries[cur].Term != entry.Term {
				rf.DPrintf("(leader %d) term conflict log[%d].Term %d != %d",
					args.LeaderId, cur, rf.logEntries[cur].Term, entry.Term)
				rf.logEntries[cur] = entry
				rf.logEntries = rf.logEntries[:cur+1]
			}
		} else {
			rf.logEntries = append(rf.logEntries, args.Entries[i:]...)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		// fmt.Printf("Server %d applyNotifyCh 0\n", rf.me)
		rf.applyNotifyCh <- struct{}{}
		// fmt.Printf("Server %d applyNotifyCh 1\n", rf.me)
	}
	reply.Success = true
	rf.DPrintf("(leader %d) log entries num: %d, commit index: %d, included index: %d",
		args.LeaderId, rf.getEntriesLength(), rf.commitIndex, rf.lastIncludedIndex)
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
		rf.DPrintf("log entries num: %d, commit index: %d, included index: %d",
			rf.getEntriesLength(), rf.commitIndex, rf.lastIncludedIndex)
		rf.DPrintf("heartbeat: %#v, %d\n", rf.nextIndex, rf.lastIncludedIndex)
		rf.unlock()
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(server int) {
				rf.lock("heartbeat 1")
				next := rf.nextIndex[server]
				if next <= rf.lastIncludedIndex {
					rf.unlock()
					rf.syncSnapshot(i)
					return
				}
				args := AppendEntriesArgs{
					Term:         rf.term,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
					Entries:      nil,
					PrevLogIndex: next - 1,
					PrevLogTerm:  rf.getEntryTerm(next - 1),
				}
				if next < rf.getEntriesLength() {
					args.Entries = append(args.Entries, rf.logEntries[rf.toSliceIndex(next):]...)
				}
				rf.unlock()
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)
				// rf.DPrintf("server %d append reply: %#v", server, reply)
				rf.lock("heartbeat 2")
				defer rf.unlock()
				if !reply.Success {
					if reply.Term > rf.term {
						rf.term = reply.Term
						rf.state = Follower
						rf.votedFor = -1
						rf.persist()
					} else if ok {
						conflictIndex := reply.ConflictIndex
						if reply.ConflictTerm != 0 {
							index := max(sort.Search(len(rf.logEntries),
								func(i int) bool { return rf.logEntries[i].Term > reply.ConflictTerm }), 1)
							if rf.logEntries[index-1].Term == reply.ConflictTerm {
								conflictIndex = rf.toEntryIndex(index)
							}
						}
						rf.DPrintf("server %d conflict index %d", server, conflictIndex)
						rf.nextIndex[server] = conflictIndex
					}
					return
				}
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			}(i)
		}
		<-timerCh
	}
}

func (rf *Raft) leaderCommit() {
	// rf.DPrintf("start leader commit check")
	timerCh := time.Tick(CommitInterval)
	for {
		<-timerCh
		rf.lock("leaderCommit")
		if rf.state != Leader || rf.killed() {
			rf.unlock()
			return
		}
		// rf.DPrintf("leader check commit")
		// rf.DPrintf("nextIndex: %#v, matchIndex: %#v", rf.nextIndex, rf.matchIndex)
		matched := make([]int, len(rf.peers))
		copy(matched, rf.matchIndex)
		sort.Ints(matched)
		rf.DPrintf("%#v, before apply commit index %d", matched, rf.commitIndex)
		if n := matched[(len(rf.peers)-1)/2]; n > rf.commitIndex && rf.getEntryTerm(n) == rf.term {
			rf.commitIndex = n
			rf.applyNotifyCh <- struct{}{}
		}
		rf.DPrintf("%#v, after apply commit index %d", rf.matchIndex, rf.commitIndex)
		rf.unlock()
	}
}

func (rf *Raft) doApply() {
	rf.lock("doApply")
	rf.DPrintf("start apply")
	applied := rf.lastApplied
	committed := rf.commitIndex
	if applied < committed {
		rf.lastApplied = committed
		entries := make([]LogEntry, 0)
		entries = append(entries, rf.logEntries[rf.toSliceIndex(applied+1):rf.toSliceIndex(committed+1)]...)
		rf.unlock()
		for i, entry := range entries {
			msg := ApplyMsg{
				Command:      entry.Command,
				CommandIndex: applied + 1 + i,
				CommandValid: true,
			}
			// log.Printf("Server %d send apply %d to chan", rf.me, msg.CommandIndex)
			rf.applyCh <- msg
			// fmt.Printf("Server %d applyCh 1\n", rf.me)
		}
		rf.DPrintf("apply entries from index %d to %d", applied+1, committed)
	} else {
		rf.unlock()
	}
}
