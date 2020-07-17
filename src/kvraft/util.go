package kvraft

import (
	"fmt"
	"log"
	"time"
)

const Debug = 0
const DeadlockCheck = 1

func (kv *KVServer) lock(m string) {
	kv.mu.Lock()
	if DeadlockCheck > 0 {
		go func() {
			select {
			case <-time.After(MutexTimeout):
				log.Printf("KVServer %d: lock timeout (%s)", kv.me, m)
			case <-kv.unlockCh:
				return
			case <-kv.killCh:
				return
			}
		}()
	}
}

func (kv *KVServer) unlock() {
	if DeadlockCheck > 0 {
		kv.unlockCh <- struct{}{}
	}
	kv.mu.Unlock()
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (kv *KVServer) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		msg := fmt.Sprintf(format, a...)
		log.Printf("KVServer %d:\n%s", kv.me, msg)
	}
	return
}
