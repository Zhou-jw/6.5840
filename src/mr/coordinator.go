package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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
	task_cnt   int
	taskStatus map[int]TaskState
	isDone     bool
}

func (c *Coordinator) next_task_id() int {
	c.task_cnt += 1
	return c.task_cnt - 1
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GiveTasks(args *ExampleArgs, reply *Task) error {
	// hold the big lock
	c.state_lock.Lock()
	if c.isDone {
		c.state_lock.Unlock()
		return nil
	}
	defer c.state_lock.Unlock()

	// assign map task
	if len(c.map_chan) > 0 {
		*reply = *<-c.map_chan
		c.taskStatus[reply.Taskid] = InProgress
		return nil
	}
	// for _, mtask := range c.mapTasks {
	// 	if c.taskStatus[mtask.Taskid] == Unassigned {
	// 		c.taskStatus[mtask.Taskid] = InProgress
	// 		*reply = mtask
	// 	}
	// }

	// assign reduce task
	if len(c.reduce_chan) > 0 {
		*reply = *<-c.reduce_chan
		c.taskStatus[reply.Taskid] = InProgress
		return nil
	}
	// for _, rtask := range c.reduceTasks {
	// 	if c.taskStatus[rtask.Taskid] == Unassigned {
	// 		c.taskStatus[rtask.Taskid] = InProgress
	// 		*reply = rtask
	// 	}
	// }
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
		task_cnt:    0,
		isDone:      false,
	}

	// Your code here.
	// init map tasks
	for i, filename := range files {
		c.mapTasks[i] = Task{
			Type_id:  0,
			Filename: filename,
			Innerid:  i,
			Taskid:   c.next_task_id(),
			NReduce:  nReduce,
		}
		c.taskStatus[c.mapTasks[i].Taskid] = Unassigned
		c.map_chan <- &c.mapTasks[i]
	}

	// init reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			Type_id: 1,
			Taskid:  c.next_task_id(),
			Innerid: i,
			NReduce: nReduce,
		}
		c.taskStatus[c.reduceTasks[i].Taskid] = Unassigned
		c.reduce_chan <- &c.reduceTasks[i]
	}

	c.server()
	return &c
}
