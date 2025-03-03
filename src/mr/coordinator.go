package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
)

type TaskState int

const (
	Unassigned TaskState = iota
	InProgress
	Completed
)

type Coordinator struct {
	// Your definitions here.
	state_lock  sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	map_chan    chan *Task
	reduce_chan chan *Task
	// workers     []chan *Task
	worker_cnt int64
	taskStatus map[int]TaskState
	isDone     bool
}

func (c *Coordinator) next_worker_id() int {
	return int(atomic.AddInt64(&c.worker_cnt, 1))
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GiveTasks(args *WorkerMeta, reply *TaskMeta) error {
	// hold the big lock
	c.state_lock.Lock()
	if c.isDone {
		// fmt.Println("c.isDone")
		reply.Workermeta.Stat = Notask
		fmt.Printf("args.Stat is %d\n", reply.Workermeta.Stat)
		c.state_lock.Unlock()
		return nil
	}
	defer c.state_lock.Unlock()

	// assign worker id
	if args.Workerid == 0 {
		reply.Workermeta.Workerid = c.next_worker_id()
	}

	// assign map task
	// if len(c.map_chan) > 0 {
	// 	fmt.Printf("len of c.map_chan is %d", len(c.map_chan))
	// 	*reply = *<-c.map_chan
	// 	fmt.Printf(", taskstate[%d] is set\n", reply.Innerid)
	// 	c.taskStatus[reply.Innerid] = InProgress
	// 	return nil
	// }

	// Try to assign a map task
	select {
	case task := <-c.map_chan:
		reply.Task = *task
		c.taskStatus[reply.Task.Innerid] = InProgress
		// fmt.Printf("Assigned map task %d\n", reply.Innerid)
		return nil
	default:
		// No map task available
	}

	// Check if all map tasks are done
	allMapTasksDone := true
	for i := 0; i < len(c.mapTasks); i++ {
		if c.taskStatus[i] != Completed {
			allMapTasksDone = false
			break
		}
	}

	if !allMapTasksDone {
		fmt.Printf("\n=== some map unfinished , worker %d is waiting ===\n", args.Workerid)
		reply.Workermeta.Stat = Waiting
		return nil
	} else {
		fmt.Printf("\n===== all map finished , worker %d is ready ===\n", args.Workerid)
		reply.Workermeta.Stat = Ready
	}

	// assign reduce task
	if len(c.reduce_chan) > 0 && allMapTasksDone {
		fmt.Printf("len of c.recude_chan is %d", len(c.reduce_chan))
		reply.Task = *<-c.reduce_chan
		fmt.Printf(", taskstate[%d] is set, workerid is %d, reduceid is %d\n", reply.Task.Innerid+len(c.mapTasks), args.Workerid, reply.Task.Innerid)
		c.taskStatus[reply.Task.Innerid+len(c.mapTasks)] = InProgress
		return nil
	}

	c.isDone = true
	return nil
}

func (c *Coordinator) TaskiFinish(args *TaskInfo, reply *ExampleReply) error {
	c.state_lock.Lock()
	defer c.state_lock.Unlock()
	c.taskStatus[args.Taskid] = Completed
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
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.state_lock.Lock()
	defer c.state_lock.Unlock()
	ret := true

	// Your code here.
	for _, state := range c.taskStatus {
		if state != Completed {
			ret = false
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		state_lock:  sync.Mutex{},
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
		map_chan:    make(chan *Task, len(files)),
		reduce_chan: make(chan *Task, nReduce),
		taskStatus:  make(map[int]TaskState),
		worker_cnt:  0,
		isDone:      false,
	}

	// Your code here.
	// init map tasks
	for i, filename := range files {
		c.mapTasks[i] = Task{
			Type_id:  0,
			Filename: filename,
			Innerid:  i,
			Workerid: 0,
			NReduce:  nReduce,
		}
		c.taskStatus[i] = Unassigned
		c.map_chan <- &c.mapTasks[i]
	}

	// init reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			Type_id:  1,
			Filename: "",
			Workerid: 0,
			Innerid:  i,
			NReduce:  nReduce,
		}
		c.taskStatus[i+len(c.mapTasks)] = Unassigned
		c.reduce_chan <- &c.reduceTasks[i]
	}

	c.server()
	return &c
}
