package mr

import "time"

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// task have three states
// if is idle
type TaskState int

const (
	Idle TaskState = iota
	Running
	Completed
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
	Exit
)

type PhaseType int

const (
	MapPhase PhaseType = iota
	ReducePhase
	DonePhase
)

type TaskRef struct {
	Type    TaskType
	State   TaskState
	TaskID  int
	Attempt int
}
type Task struct {
	Ref         TaskRef
	MapTaskFile string
	StartTime   time.Time
}

// Add your RPC definitions here.
type RegisterArgs struct {
	// if need sec
	Key string
}
type RegisterReply struct {
}

type RequestTaskArgs struct {
}

// MapTaskFile: input file for map task
// NReduce: number of reduce tasks (used for partitioning)
//
// Map tasks generate intermediate files named "mr-X-Y",
// where X is the map task ID and Y is the reduce bucket ID.
//
// NMap: total number of map tasks,
// used by reduce tasks to know how many intermediate files to read.
type RequestTaskReply struct {
	Ref         TaskRef // task identifier
	MapTaskFile string  // input file for map task
	NReduce     int     // number of reduce tasks
	NMap        int     // total number of map tasks
}

type ReportTaskDoneArgs struct {
	Ref TaskRef
}

type ReportTaskDoneReply struct {
}
