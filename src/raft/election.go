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
	defer rf.DPrintf("vote for: %d\n", rf.votedFor)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	reply.VoteGranted = false

	if rf.term > args.Term {
		return
	}
	if rf.term < args.Term {
		rf.term = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.role = Follower
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}

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
	timer := time.NewTimer(RPCTimeout)
	okCh := make(chan bool)
	go func() {
		okCh <- rf.peers[server].Call("Raft.RequestVote", args, reply)
	}()
	select {
	case <-timer.C:
		rf.DPrintf("server %d RequestVote RPC timeout\n", server)
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
	rf.DPrintf("doElection\n")
	rf.mu.Lock()
	rf.role = Candidate
	rf.term++
	rf.votedFor = rf.me
	rf.electionTimer.Reset(randElectionTimeout())
	args := RequestVoteArgs{
		CandidateId: rf.me,
		Term:        rf.term,
		LastLogTerm: rf.logEntries[len(rf.logEntries)-1].Term,
	}
	rf.mu.Unlock()

	replyCh := make(chan RequestVoteReply)

	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(server, &args, &reply)
			replyCh <- reply
		}(idx)
	}

	grantedCount := 1
	replyCount := 1
	for {
		reply := <-replyCh
		rf.mu.Lock()
		if reply.Term > rf.term {
			rf.DPrintf("receive reply with higher term %d\n", reply.Term)
			rf.term = reply.Term
			rf.role = Follower
			rf.votedFor = -1
			return
		}
		rf.mu.Unlock()
		replyCount++
		if reply.VoteGranted {
			rf.DPrintf("vote granted\n")
			grantedCount++
		}

		if replyCount == len(rf.peers) || grantedCount > len(rf.peers)/2 || replyCount-grantedCount > len(rf.peers)/2 {
			break
		}
	}

	rf.DPrintf("granted:peers = %d:%d\n", grantedCount, len(rf.peers))

	rf.mu.Lock()
	rf.votedFor = -1
	if grantedCount > len(rf.peers)/2 {
		rf.DPrintf("change role to Leader\n")
		rf.role = Leader
		rf.electionTimer.Stop()
		rf.mu.Unlock()
	} else {
		rf.mu.Unlock()
	}
}