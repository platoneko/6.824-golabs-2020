package raft

import (
	"time"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock("InstallSnapshot")

	reply.Term = rf.term
	if args.Term < rf.term {
		rf.DPrintf("leader expired")
		rf.unlock()
		return
	}

	rf.term = args.Term
	rf.state = Follower
	rf.votedFor = args.LeaderId
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randElectionTimeout())

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.persist()
		rf.DPrintf("snapshot rpc expired(%d <= %d)", args.LastIncludedIndex, rf.lastIncludedIndex)
		rf.unlock()
		return
	}

	if args.LastIncludedIndex <= rf.getLastIndex() &&
		args.LastIncludedTerm == rf.getEntryTerm(args.LastIncludedIndex) {
		rf.logEntries = rf.logEntries[rf.toSliceIndex(args.LastIncludedIndex):]
	} else {
		rf.logEntries = rf.logEntries[:1]
		rf.logEntries[0] = LogEntry{
			Term:    args.LastIncludedTerm,
			Command: nil,
		}
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persistAndSnapshot(args.Data)

	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
	rf.unlock()

	rf.applyCh <- ApplyMsg{
		CommandValid: false,
		Command:      args.Data,
		CommandIndex: args.LastIncludedIndex,
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	okCh := make(chan bool)
	go func() {
		okCh <- rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	}()
	select {
	case <-time.After(RPCTimeout):
		rf.DPrintf("server %d InstallSnapshot RPC timeout", server)
		reply.Term = args.Term
		return false
	case ok := <-okCh:
		if !ok {
			reply.Term = args.Term
		}
		return ok
	}
}

func (rf *Raft) syncSnapshot(server int) {
	rf.lock("syncSnapshot 1")
	args := InstallSnapshotArgs{
		Term:              rf.term,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.unlock()
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if !ok {
		return
	}
	rf.lock("syncSnapshot 2")
	defer rf.unlock()
	if reply.Term > rf.term {
		rf.DPrintf("receive reply with higher term %d", reply.Term)
		rf.term = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		return
	}
	rf.matchIndex[server] = rf.lastIncludedIndex
	rf.nextIndex[server] = rf.lastIncludedIndex + 1
}

func (rf *Raft) DoSnapshot(idx int, data []byte) {
	// log.Printf("server %d DoSnapshot %d, raftstate size %d", rf.me, idx, rf.persister.RaftStateSize())
	rf.lock("DoSnapshot")
	defer rf.unlock()

	if idx <= rf.lastIncludedIndex {
		rf.DPrintf("delayed snapshot(%d <= %d)", idx, rf.lastIncludedIndex)
		return
	}
	rf.logEntries = rf.logEntries[rf.toSliceIndex(idx):]
	rf.lastIncludedIndex = idx
	rf.lastIncludedTerm = rf.logEntries[0].Term
	rf.persistAndSnapshot(data)
	rf.DPrintf("DoSnapshot: %d", rf.lastIncludedIndex)
	// log.Printf("server %d DoSnapshot %d done, raftstate size %d", rf.me, idx, rf.persister.RaftStateSize())
}

func (rf *Raft) persistAndSnapshot(snapshot []byte) {
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
}
