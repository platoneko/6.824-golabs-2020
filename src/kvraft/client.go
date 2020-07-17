package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"../labrpc"
)

const RPCTimeout = 400 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	me       int64
	leaderId int
	seqNum   int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = nrand()
	return ck
}

func (ck *Clerk) sendGet(args *GetArgs, reply *GetReply) bool {
	okCh := make(chan bool)
	go func() {
		okCh <- ck.servers[ck.leaderId].Call("KVServer.Get", args, reply)
	}()
	select {
	case <-time.After(RPCTimeout):
		reply.Err = "rpc error"
		return false
	case ok := <-okCh:
		if !ok {
			reply.Err = "rpc error"
		}
		return ok
	}
}

func (ck *Clerk) sendPutAppend(args *PutAppendArgs, reply *PutAppendReply) bool {
	okCh := make(chan bool)
	go func() {
		okCh <- ck.servers[ck.leaderId].Call("KVServer.PutAppend", args, reply)
	}()
	select {
	case <-time.After(RPCTimeout):
		reply.Err = "rpc error"
		return false
	case ok := <-okCh:
		if !ok {
			reply.Err = "rpc error"
		}
		return ok
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key: key,
	}
	for {
		reply := GetReply{}
		ok := ck.sendGet(&args, &reply)
		if !ok || reply.Err != "" {
			ck.changeLeader()
			continue
		}
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	seqNum := atomic.AddInt32(&ck.seqNum, 1)
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.me,
		SeqNum:   seqNum,
	}
	for {
		reply := PutAppendReply{}
		ok := ck.sendPutAppend(&args, &reply)
		if !ok || reply.Err != "" {
			ck.changeLeader()
			continue
		}
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) changeLeader() {
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
}
