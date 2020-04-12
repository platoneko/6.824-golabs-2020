package mr

import "fmt"

type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type Task struct {
	FileName string
	NReduce  int
	NMap     int
	ID       int
	Phase    TaskPhase
}

func intermediateName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func outName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
