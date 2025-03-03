package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
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
	Notask
)

type WorkerMeta struct {
	Workerid int
	Stat     WorkerStat
}

type TaskInfo struct {
	Type_id int
	Taskid  int
}

type Task struct {
	// type = 0: map, 1: reduce
	Type_id  int
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
