package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	map_chan    chan *Task
	reduce_chan chan *Task
	state_lock  sync.Mutex
	is_done     bool
	task_cnt    int
}

func (c *Coordinator) next_task_id() int {
	c.task_cnt += 1
	return c.task_cnt - 1
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GiveTask(args *ExampleArgs, reply *Task) error {
	// hold the big lock
	c.state_lock.Lock()
	defer c.state_lock.Unlock()

	if len(c.map_chan) > 0 {
		*reply = *<-c.map_chan
	}
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		map_chan:    make(chan *Task, len(files)),
		reduce_chan: make(chan *Task, nReduce),
		state_lock:  sync.Mutex{},
		is_done:     false,
		task_cnt:    0,
	}

	// Your code here.
	for _, filename := range files {
		task := Task{
			Type_id:  0,
			Filename: filename,
			Taskid:   c.next_task_id(),
			NReduce:  nReduce,
		}
		c.map_chan <- &task
	}
	c.server()
	return &c
}
