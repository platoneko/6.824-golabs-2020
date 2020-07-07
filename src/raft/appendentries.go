package raft

import "time"

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
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	reply.Success = false
	if args.Term < rf.term {
		return
	}
	rf.term = args.Term
	rf.role = Follower
	rf.votedFor = -1
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randElectionTimeout())
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
		rf.mu.Lock()
		if rf.role != Leader || rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.DPrintf("send heartbeat")
		args := AppendEntriesArgs{
			Term:     rf.term,
			LeaderId: rf.me,
		}
		rf.mu.Unlock()
		for i := range rf.peers {
			if i == rf.me {
				rf.electionTimer.Stop()
				rf.electionTimer.Reset(randElectionTimeout())
				continue
			}
			go func(server int) {
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(server, &args, &reply)
				rf.mu.Lock()
				if reply.Term > rf.term {
					rf.term = reply.Term
					rf.role = Follower
					rf.votedFor = -1
				}
				rf.mu.Unlock()
			}(i)
		}
		<-timerCh
	}

}
