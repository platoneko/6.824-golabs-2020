package kvraft

import (
	"fmt"
	"log"
)

const Debug = 0

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
