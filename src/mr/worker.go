package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func DoReduce(reducef func(string, []string) string, task *Task) error {
	var intermediate []KeyValue
	reduceid := task.Innerid
	fmt.Printf("reduce id is %d", reduceid)

	// match files to be reduced
	pattern := fmt.Sprintf("mr-*-%d", reduceid)
	files, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Println("Error:", err)
		return nil
	}

	// read content of each mr-X-reduceid
	for _, ifilename := range files {
		ifile, err := os.Open(ifilename)
		if err != nil {
			log.Fatalf("cannot open %v", ifilename)
		}
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		ifile.Close()
	}

	// reduce phase
	sort.Sort(ByKey(intermediate))
	ofile, _ := os.Create(fmt.Sprintf("mr-out-%d", reduceid))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

	return nil
}

func DoMap(mapf func(string, string) []KeyValue, task *Task) error {
	var kva []KeyValue
	nReduce := task.NReduce
	fmt.Printf("nReduce is %d", nReduce)
	// read content of pg-*
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// map each word in content to (word, "1")
	kva = mapf(filename, string(content))
	total_inter_files := make([][]KeyValue, nReduce)

	// map phase
	for _, kv := range kva {
		reduce_idx := ihash(kv.Key) % nReduce
		total_inter_files[reduce_idx] = append(total_inter_files[reduce_idx], kv)
	}

	// write intermediate output to mr-X-Y
	for i := 0; i < nReduce; i++ {
		oname := "mr-" + strconv.Itoa(task.Taskid) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)

		for kv := range total_inter_files[i] {
			err = enc.Encode(kv)
			if err != nil {
				log.Fatalf("cannot encode kv:%v in file: %v", kv, filename)
			}
		}
		ofile.Close()
	}
	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	new_task := AskForTask()
	// Your worker implementation here.
	if new_task.Type_id == 0 {
		// Map worker
		err := DoMap(mapf, new_task)
		if err != nil {
			log.Printf("fail to map, worker.id is %d, taskid is %d\n", new_task.Innerid, new_task.Taskid)
		}
	} else {
		err := DoReduce(reducef, new_task) 
		if err != nil {
			log.Printf("fail to reduce, worker.id is %d, taskid is %d\n", new_task.Innerid, new_task.Taskid)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func AskForTask() *Task {
	args := ExampleArgs{}
	new_task := Task{}
	// reduce_task := Task{}
	ok := call("Coordinator.GiveTasks", &args, &new_task)

	if ok {
		fmt.Printf("ask for task, task file: %v\n", new_task.Filename)
	} else {
		fmt.Printf("fail to ask for task\n")
	}
	return &new_task
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
