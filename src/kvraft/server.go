package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const (
	AgreeTimeout = 300 * time.Millisecond
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqNum   int32
	Op       string
	Key      string
	Value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db      map[string]string
	seqNum  map[int64]int32
	killCh  chan struct{}
	agreeCh map[int]chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Op:  "Get",
		Key: args.Key,
	}
	reply.Err = ""
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = "not Leader"
		return
	}

	ch := make(chan Op)
	kv.mu.Lock()
	kv.agreeCh[index] = ch
	kv.mu.Unlock()
	var agreeOp Op
	select {
	case <-time.After(AgreeTimeout):
		reply.Err = "agree timeout"
		kv.mu.Lock()
		if ch == kv.agreeCh[index] {
			delete(kv.agreeCh, index)
		}
		kv.mu.Unlock()
		return
	case agreeOp = <-ch:
		kv.mu.Lock()
		delete(kv.agreeCh, index)
		kv.mu.Unlock()
	}

	if agreeOp != op {
		reply.Err = "op overwrited"
		return
	}
	kv.mu.Lock()
	reply.Value = kv.db[op.Key]
	kv.mu.Unlock()
	kv.DPrintf("op %#v agreed", op)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		Op:       args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}
	reply.Err = ""
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = "not Leader"
		return
	}

	ch := make(chan Op)
	kv.mu.Lock()
	kv.agreeCh[index] = ch
	kv.mu.Unlock()
	var agreeOp Op
	select {
	case <-time.After(AgreeTimeout):
		reply.Err = "agree timeout"
		kv.mu.Lock()
		if ch == kv.agreeCh[index] {
			delete(kv.agreeCh, index)
		}
		kv.mu.Unlock()
		return
	case agreeOp = <-ch:
		kv.mu.Lock()
		delete(kv.agreeCh, index)
		kv.mu.Unlock()
	}

	if agreeOp != op {
		reply.Err = "op overwrited"
		return
	}
	kv.DPrintf("op %#v agreed", op)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killCh <- struct{}{}
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.seqNum = make(map[int64]int32)
	kv.agreeCh = make(map[int]chan Op)
	kv.killCh = make(chan struct{})

	go kv.waitAgree()

	return kv
}

func (kv *KVServer) waitAgree() {
	for {
		select {
		case <-kv.killCh:
			return
		case msg := <-kv.applyCh:
			op := msg.Command.(Op)
			kv.mu.Lock()
			seqNum, ok := kv.seqNum[op.ClientId]
			if !ok || op.SeqNum > seqNum {
				kv.seqNum[op.ClientId] = op.SeqNum
				switch op.Op {
				case "Put":
					kv.db[op.Key] = op.Value
				case "Append":
					kv.db[op.Key] += op.Value
				}
				kv.DPrintf("apply %#v", msg)
			}
			ch, ok := kv.agreeCh[msg.CommandIndex]
			kv.mu.Unlock()
			if ok {
				ch <- op
			}
		}
	}
}
