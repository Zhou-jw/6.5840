package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

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

// Add your RPC definitions here.
type WorkerStat int

const (
	Ready WorkerStat = iota
	Waiting
	PleaseExit
)

type WorkerMeta struct {
	Workerid int
	Stat     WorkerStat
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	ExitTask
)

type TaskState int

const (
	Unassigned TaskState = iota
	InProgress
	Completed
)

type TaskInfo struct {
	TaskType  TaskType
	Taskid    int
	State TaskState
	StartTime time.Time
}

type Task struct {
	// type = 0: map, 1: reduce
	TaskType TaskType
	Filename string
	Workerid int // id of all tasks
	Innerid  int // id of Map/Reduce tasks
	NReduce  int
}

type TaskMeta struct {
	Task       Task
	Workermeta WorkerMeta
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
