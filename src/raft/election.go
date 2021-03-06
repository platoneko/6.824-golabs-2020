package raft

import (
	"math/rand"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock("RequestVote")
	defer rf.unlock()

	reply.Term = rf.term
	reply.VoteGranted = false

	if rf.term > args.Term {
		rf.DPrintf("candidate expired")
		return
	}

	defer rf.persist()

	if rf.term < args.Term {
		rf.term = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	if lastTerm > args.LastLogTerm ||
		lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex {
		rf.DPrintf("log newer than candidate's log (%d, %d) > (%d, %d)",
			lastTerm, lastIndex, args.LastLogTerm, args.LastLogIndex)
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.state = Follower
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.electionTimer.Stop()
		rf.electionTimer.Reset(randElectionTimeout())
	}
	rf.DPrintf("vote for: %d", rf.votedFor)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	okCh := make(chan bool)
	go func() {
		okCh <- rf.peers[server].Call("Raft.RequestVote", args, reply)
	}()
	select {
	case <-time.After(RPCTimeout):
		rf.DPrintf("server %d RequestVote RPC timeout", server)
		reply.VoteGranted = false
		reply.Term = args.Term
		return false
	case ok := <-okCh:
		if !ok {
			reply.VoteGranted = false
			reply.Term = args.Term
		}
		return ok
	}
}

func randElectionTimeout() time.Duration {
	return ElectionTimeout + time.Duration(rand.Int63())%ElectionTimeout
}

func (rf *Raft) doElection() {
	rf.lock("doElection 0")
	rf.electionTimer.Reset(randElectionTimeout())
	if rf.state == Leader {
		rf.unlock()
		return
	}
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.term++
	rf.persist()
	rf.unlock()
	rf.DPrintf("start election")
	if !rf.sendRequestVoteToAll() {
		rf.lock("doElection 3")
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		rf.unlock()
		return
	}
	rf.lock("doElection 4")
	if rf.state == Candidate {
		rf.DPrintf("change state to Leader")
		rf.state = Leader
		nextIndex := rf.getEntriesLength()
		for i := range rf.peers {
			rf.nextIndex[i] = nextIndex
		}
		rf.unlock()
		go rf.heartbeat()
		go rf.leaderCommit()
	} else {
		rf.unlock()
	}
}

func (rf *Raft) sendRequestVoteToAll() bool {
	replyCh := make(chan RequestVoteReply)
	rf.lock("sendRequestVoteToAll 0")
	args := RequestVoteArgs{
		CandidateId:  rf.me,
		Term:         rf.term,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}
	rf.unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(server, &args, &reply)
			replyCh <- reply
		}(i)
	}

	grantedCount := 1
	replyCount := 1
	for {
		reply := <-replyCh
		rf.lock("sendRequestVoteToAll 1")
		if reply.Term > rf.term {
			rf.DPrintf("receive reply with higher term %d", reply.Term)
			rf.term = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.unlock()
			return false
		}
		rf.unlock()
		replyCount++
		if reply.VoteGranted {
			grantedCount++
		}

		if replyCount == len(rf.peers) || grantedCount > len(rf.peers)/2 || replyCount-grantedCount > len(rf.peers)/2 {
			break
		}
	}
	if grantedCount > len(rf.peers)/2 {
		return true
	} else {
		return false
	}
}
