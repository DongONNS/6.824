package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func keyReduceIndex(key string, nReduce int) int {
	return ihash(key) % nReduce
}

const pathPrefix = "./"

func makePgFileName(name *string) string {
	return "pg-" + *name + ".txt"
}

func makeIntermediateFromFile(filename string, mapFunc func(string, string) []KeyValue) []KeyValue {
	path := filename
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("cannot open %v", path)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file %v", filename)
	}
	file.Close()

	keyValueList := mapFunc(filename, string(content))
	return keyValueList
}

type AWorker struct {
	mapFunc    func(string, string) []KeyValue
	reduceFunc func(string, []string) string

	// false代表map true代表reduce
	mapOrReduce bool
	// true的时候即退出
	allDone bool

	workerId int
}

//
// main/mrworker.go calls this function.
//
func Worker(mapFunc func(string, string) []KeyValue, reduceFunc func(string, []string) string) {

	time.Sleep(1000 * time.Millisecond)

	// Your worker implementation here.
	worker := AWorker{}
	worker.mapFunc = mapFunc
	worker.reduceFunc = reduceFunc
	worker.mapOrReduce = false
	worker.allDone = false
	worker.workerId = -1

	worker.logPrintf("initialized!\n")

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for !worker.allDone {
		worker.process()
	}
	worker.logPrintf("no more tasks, all done, exiting... \n")
}

func (worker *AWorker) logPrintf(format string, vars ...interface{}) {
	log.Printf("worker %d: "+format, worker.workerId, vars)
}

func (worker *AWorker) askMapTask() *MapTaskReply {
	args := MapTaskArgs{}
	args.WorkedId = worker.workerId
	reply := MapTaskReply{}

	worker.logPrintf("requesting for map task...\n")
	call("Coordinator.GiveMapTask", &args, &reply)
	worker.logPrintf("request for map task received...\n")

	worker.workerId = reply.WorkerId

	if -1 == reply.FileId {
		// 拒绝分配task
		if reply.AllDone {
			worker.logPrintf("no more map tasks, switch to reduce mod\n")
			return nil
		} else {
			worker.logPrintf("there is no task available for now, there will be more just a moment...\n")
			return &reply
		}
	}
	worker.logPrintf("got map task on file %v %v\n", reply.FileId, reply.FileName)

	// 分配一个任务
	return &reply
}

func (worker *AWorker) writeToFiles(fileId int, nReduce int, intermediate []KeyValue) {
	// 初始化对象
	keyValueAll := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		keyValueAll[i] = make([]KeyValue, 0)
	}
	// 数据写入
	for _, kv := range intermediate {
		index := keyReduceIndex(kv.Key, nReduce)
		keyValueAll[index] = append(keyValueAll[index], kv)
	}
	for i := 0; i < nReduce; i++ {
		tempFile, err := os.CreateTemp(".", "mrTemp")
		if err != nil {
			log.Fatal(err)
		}
		encoder := json.NewEncoder(tempFile)
		for _, kv := range keyValueAll {
			err := encoder.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		outName := fmt.Sprintf("mr-%v-%v \n", fileId, i)
		err = os.Rename(tempFile.Name(), outName)
		if err != nil {
			worker.logPrintf("rename tempFile failed for %v \n", outName)
		}
	}
}

func (worker *AWorker) joinMapTask(fileId int) {
	args := MapTaskJoinArgs{}
	args.fileId = fileId
	args.workerId = worker.workerId

	reply := MapTaskJoinReply{}
	call("Coordinator.JoinMapTask", &args, &reply)

	if reply.accept {
		worker.logPrintf("accepted\n")
	} else {
		worker.logPrintf("not accepted\n")
	}
}

func (worker *AWorker) executeMap(reply *MapTaskReply) {
	intermediate := makeIntermediateFromFile(reply.FileName, worker.mapFunc)
	worker.logPrintf("writing map results to file\n")
	worker.writeToFiles(reply.FileId, reply.NReduce, intermediate)
	worker.joinMapTask(reply.FileId)
}

func (worker *AWorker) askReduceTask() *ReduceTaskReply {
	args := ReduceTaskArgs{}
	args.WorkerId = worker.workerId
	reply := ReduceTaskReply{}

	worker.logPrintf("requesting for reduce task...\n")
	call("Coordinator.GiveReduceTask", &args, &reply)

	if -1 == reply.reduceIndex {
		if reply.allDone {
			worker.logPrintf("no more reduce tasks, try to terminate worker\n")
			return nil
		} else {
			worker.logPrintf("there is no task available for now, there will be more just a moment...\n")
			return &reply
		}
	}
	worker.logPrintf("got reduce task on %v th cluster", reply.reduceIndex)

	return &reply
}

type ByKey []KeyValue

// Len 返回列表长度
func (a ByKey) Len() int { return len(a) }

// Swap 交换key对应的值
func (a ByKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less 比较两键的大小，用于排序
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func reduceKVSlice(intermediate []KeyValue, reduceFunc func(string, []string) string, outFile io.Writer) {
	sort.Sort(ByKey(intermediate))
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && (intermediate)[j].Key == (intermediate)[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, (intermediate)[k].Value)
		}
		output := reduceFunc((intermediate)[i].Key, values)

		fmt.Fprintf(outFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
}

func readIntermediate(fileId int, reduceId int) []KeyValue {
	fileName := fmt.Sprintf("mr-%v-%v", fileId, reduceId)
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open file: %v\n", fileName)
	}
	decoder := json.NewDecoder(file)
	keyValueAll := make([]KeyValue, 0)
	for {
		var kv KeyValue
		err = decoder.Decode(&kv)
		if err != nil {
			break
		}
		keyValueAll = append(keyValueAll, kv)
	}
	file.Close()

	return keyValueAll
}

func (worker *AWorker) joinReduceTask(reduceIndex int) {
	args := ReduceTaskJoinArgs{}
	args.reduceIndex = reduceIndex
	args.workerId = worker.workerId

	reply := ReduceTaskJoinReply{}
	call("Coordinator.JoinReduceTask", &args, &reply)

	if reply.accept {
		worker.logPrintf("accepted\n")
	} else {
		worker.logPrintf("not accepted\n")
	}
}

func (worker *AWorker) executeReduce(reply *ReduceTaskReply) {
	outName := fmt.Sprint("mr-out-", reply.reduceIndex)
	intermediate := make([]KeyValue, 0)
	for i := 0; i < reply.fileCount; i++ {
		worker.logPrintf("generating intermediates on cluster %v \n", i)
		intermediate = append(intermediate, readIntermediate(i, reply.reduceIndex)...)
	}
	worker.logPrintf("total intermediate count \n", len(intermediate))
	tempFile, err := os.CreateTemp(".", "mrTemp")
	if err != nil {
		worker.logPrintf("cannot create tempFile fot %v \n", outName)
	}
	reduceKVSlice(intermediate, worker.reduceFunc, tempFile)
	tempFile.Close()
	err = os.Rename(tempFile.Name(), outName)
	if err != nil {
		worker.logPrintf("rename tempFile failed for %v\n", outName)
	}
	worker.joinReduceTask(reply.reduceIndex)
}

func (worker *AWorker) process() {
	if worker.allDone {
		// 退出
	}

	if !worker.mapOrReduce {
		// 执行map任务
		reply := worker.askMapTask()
		if reply == nil {
			// 请求失败，切换为reduce模式
			worker.mapOrReduce = true
		} else {
			if reply.FileId == -1 {
				return
			} else {
				worker.executeMap(reply)
			}
		}
	}
	if worker.mapOrReduce {
		// 执行reduce任务
		reply := worker.askReduceTask()
		if reply == nil {
			worker.allDone = true
		} else {
			if reply.reduceIndex == -1 {
				// 当前无可用的task
				return
			} else {
				// 执行任务
				worker.executeReduce(reply)
			}
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
