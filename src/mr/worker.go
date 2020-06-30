package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type WorkerT struct {
	ID      int
	Mapf    func(string, string) []KeyValue
	Reducef func(string, []string) string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := WorkerT{
		Mapf:    mapf,
		Reducef: reducef,
	}
	w.CallRegister()
	w.run()
}

func (w *WorkerT) run() {
	for {
		t := w.CallGetTask()
		switch t.Phase {
		case MapPhase:
			w.runMapTask(t)
		case ReducePhase:
			w.runReduceTask(t)
		default:
			panic("t.Phase undefined\n")
		}
	}
}

func (w *WorkerT) runMapTask(t *Task) {
	content, err := ioutil.ReadFile(t.FileName)
	if err != nil {
		w.CallReportTask(t, err)
		return
	}
	kvs := w.Mapf(t.FileName, string(content))
	intermediate := make([][]KeyValue, t.NReduce, t.NReduce)
	for _, kv := range kvs {
		index := ihash(kv.Key) % t.NReduce
		intermediate[index] = append(intermediate[index], kv)
	}
	for reduceIdx := 0; reduceIdx < t.NReduce; reduceIdx++ {
		f, err := os.Create(intermediateName(t.ID, reduceIdx))
		if err != nil {
			w.CallReportTask(t, err)
			return
		}
		data, _ := json.Marshal(intermediate[reduceIdx])
		_, err = f.Write(data)
		if err != nil {
			w.CallReportTask(t, err)
			return
		}
		err = f.Close()
		if err != nil {
			w.CallReportTask(t, err)
			return
		}
	}
	w.CallReportTask(t, nil)
}

func (w *WorkerT) runReduceTask(t *Task) {
	kvReduce := make(map[string][]string)
	for mapIdx := 0; mapIdx < t.NMap; mapIdx++ {
		content, err := ioutil.ReadFile(intermediateName(mapIdx, t.ID))
		if err != nil {
			w.CallReportTask(t, err)
			return
		}
		kvs := make([]KeyValue, 0)
		err = json.Unmarshal(content, &kvs)
		if err != nil {
			w.CallReportTask(t, err)
			return
		}
		for _, kv := range kvs {
			_, ok := kvReduce[kv.Key]
			if !ok {
				kvReduce[kv.Key] = make([]string, 0, 100)
			}
			kvReduce[kv.Key] = append(kvReduce[kv.Key], kv.Value)
		}
	}
	data := make([]string, 0, 100)
	for k, v := range kvReduce {
		data = append(data, fmt.Sprintf("%v %v\n", k, w.Reducef(k, v)))
	}
	err := ioutil.WriteFile(outName(t.ID), []byte(strings.Join(data, "")), 0600)
	if err != nil {
		w.CallReportTask(t, err)
		return
	}
	w.CallReportTask(t, nil)
}

/*
//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}
*/

func (w *WorkerT) CallRegister() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	if !call("Master.RegisterWorker", &args, &reply) {
		log.Fatal("Register failed!\n")
	}
	w.ID = reply.WorkerID
}

func (w *WorkerT) CallGetTask() *Task {
	args := GetTaskArgs{w.ID}
	reply := GetTaskReply{}
	if !call("Master.GetTask", &args, &reply) {
		log.Fatal("GetTask failed, worker process exit!\n")
	}
	return reply.Task
}

func (w *WorkerT) CallReportTask(t *Task, err error) {
	args := ReportTaskArgs{
		Done:      true,
		TaskID:    t.ID,
		WorkerID:  w.ID,
		TaskPhase: t.Phase,
	}
	if err != nil {
		args.Done = false
		log.Printf("%+v %v", args, err)
	}
	reply := ReportTaskReply{}
	if !call("Master.ReportTask", &args, &reply) {
		log.Printf("ReportTask failed! %+v\n", args)
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
