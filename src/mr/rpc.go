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

type TaskState int

// task have three states
// if is idle
const (
	Idle TaskState = iota
	Running
	Finished
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
	Exit
)

type Task struct {
	Type      TaskType
	ID        int
	StartTime time.Time
	State     TaskState
}

// Add your RPC definitions here.
type MyArgs struct {
}
