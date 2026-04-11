package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
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

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	workerID, err := Register()
	if err != nil {
		log.Fatalf("Worker Register err: %v", err)
	}

	for {
		reply, err := RequestTask(workerID)
		if err != nil {
			log.Fatalf("[Worker] RequestTask err: %v", err)
		}
		switch reply.Ref.Type {
		case Map:
			err = doMap(reply, mapf)
			if err != nil {
				log.Fatalf("[Worker] doMap err: %v", err)
			}
			ReportTaskDone(reply.Ref)
		case Reduce:
			err = doReduce(reply, reducef)
			if err != nil {
				log.Fatalf("[Worker] doReduce err: %v", err)
			}
			ReportTaskDone(reply.Ref)
		case Wait:
			time.Sleep(1000 * 1)
		case Exit:
			return
		default:
			panic("err")
		}
	}
}

func Register() (int, error) {
	args := RegisterArgs{}
	reply := RegisterReply{}
	ok := call("Coordinator.Register", &args, &reply)
	if ok {
		return reply.WorkerID, nil
	} else {
		return -1, fmt.Errorf("call failed")
	}
}

func RequestTask(workerID int) (RequestTaskReply, error) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}

	args.WorkerID = workerID
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		return reply, nil
	} else {
		return RequestTaskReply{}, fmt.Errorf("RequestTask call failed")
	}
}

func ReportTaskDone(ref TaskRef) error {
	args := ReportTaskDoneArgs{}
	reply := ReportTaskDoneReply{}

	args.Ref = ref
	ok := call("Coordinator.ReportTaskDone", &args, &reply)
	if ok {
		return nil
	} else {
		return fmt.Errorf("ReportTaskDone call failed")
	}
}

// to use map tasks
func doMap(reply RequestTaskReply, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(reply.MapTaskFile)
	if err != nil {
		return err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	file.Close()
	kva := mapf(reply.MapTaskFile, string(content))
	buckets := make([][]KeyValue, reply.NReduce)

	for _, kv := range kva {
		r := ihash(kv.Key) % reply.NReduce
		buckets[r] = append(buckets[r], kv)
	}

	for i, bucket := range buckets {
		jsonData, err := json.Marshal(bucket)
		if err != nil {
			return fmt.Errorf("doMap: json marshal %v", err)
		}
		reduceFileName := fmt.Sprintf("/tmp/mr-%d-%d", reply.Ref.TaskID, i)
		err = os.WriteFile(reduceFileName, jsonData, 0644)
		if err != nil {
			return fmt.Errorf("doMap: write file %v %v", reduceFileName, err)
		}
	}

	return nil
}

// used reduce task
func doReduce(reply RequestTaskReply, reducef func(string, []string) string) error {
	var kva []KeyValue
	var intermediate []KeyValue

	for i := 0; i < reply.NMap; i++ {
		reduceFile := fmt.Sprintf("/tmp/mr-%d-%d", i, reply.Ref.TaskID)
		file, err := os.Open(reduceFile)
		if err != nil {
			return err
		}
		jsonData, err := io.ReadAll(file)
		if err != nil {
			return err
		}
		file.Close()
		err = json.Unmarshal(jsonData, &kva)
		if err != nil {
			return err
		}
		intermediate = append(intermediate, kva...)
	}

	oname := fmt.Sprintf("./mr-out-%d", reply.Ref.TaskID)
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}
	sort.Sort(ByKey(intermediate))
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
