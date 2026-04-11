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

type Coordinator struct {
	// Your definitions here.
	mu             sync.Mutex
	mapTaskPool    []Task
	reduceTaskPool []Task
	workerCounts   int
	nReduce        int
	Phase          PhaseType
	mapDone        int
	reduceDone     int
}

// Your code here -- RPC handlers for the worker to call.
// =========================[My Code]============================
// Worker Register
// Worker send register key if server need auth
// master server reply worker ID and other info if worker need
// example: if worker request map task, send filename server need
func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Server how to registration worker info?
	reply.WorkerID = c.workerCounts
	c.workerCounts++
	return nil
}

// Set task phase Map, Reduce or Done
// c.mu has been locked in function RequestTask
func (c *Coordinator) advancePhaseLocked() {
	switch c.Phase {
	case MapPhase:
		if c.mapDone == len(c.mapTaskPool) {
			c.Phase = ReducePhase
		}
	case ReducePhase:
		if c.reduceDone == len(c.reduceTaskPool) {
			c.Phase = DonePhase
		}
	}
}

func (c *Coordinator) assignIdleTaskLocked(taskPool []Task, tp TaskType, workerID int, reply *RequestTaskReply) bool {
	for i := range taskPool {
		if taskPool[i].Ref.State != Idle {
			continue
		}
		taskPool[i].Ref.State = Running
		taskPool[i].WorkerID = workerID
		taskPool[i].StartTime = time.Now()

		reply.MapTaskFile = taskPool[i].MapTaskFile
		reply.Ref.TaskID = taskPool[i].Ref.TaskID
		reply.Ref.Attempt = taskPool[i].Ref.Attempt
		reply.Ref.State = Running
		reply.Ref.Type = tp
		reply.NReduce = c.nReduce
		reply.NMap = len(c.mapTaskPool)
		return true
	}
	return false
}

// worker request coordinator to get task
// if have idle map task, send
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.advancePhaseLocked()
	switch c.Phase {
	case MapPhase:
		if c.assignIdleTaskLocked(c.mapTaskPool, Map, args.WorkerID, reply) {
			return nil
		}
		reply.Ref.Type = Wait
	case ReducePhase:
		if c.assignIdleTaskLocked(c.reduceTaskPool, Reduce, args.WorkerID, reply) {
			return nil
		}
		reply.Ref.Type = Wait
	case DonePhase:
		reply.Ref.Type = Exit
	}

	// Check map worker state if all tasks completed
	// Reduce phase
	return nil
}

// Set Completed Task State Running
func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	switch args.Ref.Type {
	case Map:
		task = &c.mapTaskPool[args.Ref.TaskID]
	case Reduce:
		task = &c.reduceTaskPool[args.Ref.TaskID]
	case Wait, Exit:
		return nil
	}

	if task.Ref.State != Running {
		return nil
	}

	// Check if the task crashed
	if task.Ref.Attempt != args.Ref.Attempt {
		return nil
	}

	task.Ref.State = Completed

	switch args.Ref.Type {
	case Map:
		c.mapDone++
	case Reduce:
		c.reduceDone++
	}

	c.advancePhaseLocked()
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	defer c.mu.Unlock()
	// Your code here.
	ret = c.Phase == DonePhase
	return ret
}

// handler crash worker
func (c *Coordinator) timeoutChecker() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()

		if c.Phase == DonePhase {
			c.mu.Unlock()
			return
		}
		c.checkTimeoutLocked(c.mapTaskPool)
		c.checkTimeoutLocked(c.reduceTaskPool)
		c.mu.Unlock()
	}
}

func (c *Coordinator) checkTimeoutLocked(tasks []Task) {
	for i := range tasks {
		t := &tasks[i]
		if t.Ref.State == Running && time.Since(t.StartTime) > 10*time.Second {
			t.Ref.State = Idle
			t.WorkerID = -1
			t.StartTime = time.Time{}
			t.Ref.Attempt++
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	// init map task pool
	c.mapTaskPool = make([]Task, len(files))
	for i, file := range files {
		c.mapTaskPool[i] = Task{
			Ref: TaskRef{
				Type:   Map,
				State:  Idle,
				TaskID: i,
			},
			MapTaskFile: file,
			WorkerID:    -1,
		}
	}

	// init reduce task pool
	for i := 0; i < nReduce; i++ {
		c.reduceTaskPool = append(c.reduceTaskPool, Task{
			Ref: TaskRef{
				Type:   Reduce,
				State:  Idle,
				TaskID: i,
			},
			WorkerID: -1,
		})
	}

	c.server(sockname)
	go c.timeoutChecker()

	return &c
}
