package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskTimeout      = time.Second * 5
	ScheduleInterval = time.Millisecond * 500
)

const (
	TaskStatusReady = iota
	TaskStatusQueue
	TaskStatusRunning
	TaskStatusDone
	TaskStatusError
)

type Master struct {
	// Your definitions here.
	Files     []string
	NReduce   int
	NMap      int
	TaskStats []TaskStat
	TaskPhase TaskPhase
	TaskCh    chan *Task
	Lock      sync.Mutex
	NWorker   int
	HasDone   bool
}

type TaskStat struct {
	Status    int
	WorkerID  int
	StartTime time.Time
}

func (m *Master) initMapTasks() {
	m.TaskPhase = MapPhase
	m.TaskStats = make([]TaskStat, m.NMap)
}

func (m *Master) initReduceTasks() {
	m.TaskPhase = ReducePhase
	m.TaskStats = make([]TaskStat, m.NReduce)
}

func (m *Master) makeTask(taskID int) *Task {
	task := Task{
		FileName: "",
		NReduce:  m.NReduce,
		NMap:     m.NMap,
		ID:       taskID,
		Phase:    m.TaskPhase,
	}
	if task.Phase == MapPhase {
		task.FileName = m.Files[taskID]
	}
	return &task
}

// Inspect regularly if some task running out of time
func (m *Master) tickSchedule() {
	for !m.HasDone {
		m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

func (m *Master) schedule() {
	m.Lock.Lock()
	defer m.Lock.Unlock()

	if m.HasDone {
		return
	}
	allDone := true
	for i, t := range m.TaskStats {
		switch t.Status {
		case TaskStatusReady:
			allDone = false
			m.TaskCh <- m.makeTask(i)
			m.TaskStats[i].Status = TaskStatusQueue
		case TaskStatusQueue:
			allDone = false
		case TaskStatusRunning:
			allDone = false
			if time.Now().Sub(t.StartTime) > TaskTimeout {
				m.TaskStats[i].Status = TaskStatusQueue
				m.TaskCh <- m.makeTask(i)
			}
		case TaskStatusDone:
		case TaskStatusError:
			allDone = false
			m.TaskStats[i].Status = TaskStatusQueue
			m.TaskCh <- m.makeTask(i)
		default:
			panic("t.Status undefined\n")
		}
	}
	if allDone {
		if m.TaskPhase == MapPhase {
			m.initReduceTasks()
		} else {
			m.HasDone = true
		}
	}
}

// Your code here -- RPC handlers for the worker to call.

/*
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
*/

func (m *Master) registerTask(args *GetTaskArgs, task *Task) {
	m.Lock.Lock()
	defer m.Lock.Unlock()
	m.TaskStats[task.ID].Status = TaskStatusRunning
	m.TaskStats[task.ID].WorkerID = args.WorkerID
	m.TaskStats[task.ID].StartTime = time.Now()
}

// RPC handler. Get a map/reduce task for workers.
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	task := <-m.TaskCh
	reply.Task = task
	m.registerTask(args, task)
	return nil
}

// RPC handler. Register a worker, reply a WorkerID.
func (m *Master) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.Lock.Lock()
	defer m.Lock.Unlock()
	reply.WorkerID = m.NWorker
	m.NWorker++
	return nil
}

// RPC handler. Report task Done or Error and then schedule.
func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.Lock.Lock()
	// A map task may run more than one time cause redundant running
	if args.TaskPhase != m.TaskPhase || m.TaskStats[args.TaskID].WorkerID != args.WorkerID {
		m.Lock.Unlock()
		return nil
	}
	if args.Done {
		m.TaskStats[args.TaskID].Status = TaskStatusDone
	} else {
		m.TaskStats[args.TaskID].Status = TaskStatusError
	}
	m.Lock.Unlock()
	m.schedule()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.HasDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.NReduce = nReduce
	m.NMap = len(files)
	m.Files = files
	if m.NReduce > m.NMap {
		m.TaskCh = make(chan *Task, m.NReduce)
	} else {
		m.TaskCh = make(chan *Task, m.NMap)
	}
	m.initMapTasks()
	m.server()
	go m.tickSchedule()
	return &m
}
