package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const maxTaskTime = 10 //单位为秒

type MapTaskState struct {
	beginSecond int64
	workerId    int
	fileId      int
}

type ReduceTaskState struct {
	beginSecond int64
	workerId    int
	fileId      int
}

type Coordinator struct {
	// Your definitions here.
	fileNames []string
	nReduce   int

	currWorkerId int

	unIssuedMapTasks *BlockQueue
	issuedMapTasks   *MapSet
	issuedMapMutex   sync.Mutex

	unIssuedReduceTasks *BlockQueue
	issuedReduceTasks   *MapSet
	issuedReduceMutex   sync.Mutex

	// 任务状态
	mapTasks    []MapTaskState
	reduceTasks []ReduceTaskState

	// state
	mapDone bool
	allDone bool
}

type MapTaskArgs struct {
	WorkedId int
}

type MapTaskReply struct {
	// worker把这个传递给操作系统
	FileName string

	// 标记一个用于mapping的单独文件
	FileId int

	// 用于reduce任务
	NReduce int

	// 分配的workerId
	WorkerId int

	// 用于判断该类任务是否都完成了，如果没有完成则文件id为-1，
	AllDone bool
}

func mapDoneProcess(reply *MapTaskReply) {
	log.Printf("all map tasks complete, telling workers to switch to reduce mode")
	reply.FileId = -1
	reply.AllDone = true
}

func (coordinator *Coordinator) GiveMapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	log.Printf("===============rpc call success!================")
	if args.WorkedId == -1 {
		reply.WorkerId = coordinator.currWorkerId
		log.Printf("=================== %v================", coordinator.currWorkerId)
		coordinator.currWorkerId++
	} else {
		reply.WorkerId = args.WorkedId
	}
	log.Printf("worker %v asks for a map task\n", reply.WorkerId)

	coordinator.issuedMapMutex.Lock()

	if coordinator.mapDone {
		coordinator.issuedMapMutex.Unlock()
		mapDoneProcess(reply)
		return nil
	}
	if coordinator.unIssuedMapTasks.size() == 0 && coordinator.issuedMapTasks.size() == 0 {
		coordinator.issuedMapMutex.Unlock()
		mapDoneProcess(reply)
		coordinator.prepareAllReduceTasks()
		coordinator.mapDone = true
		return nil
	}
	log.Printf("%v unissued map tasks, %v issued map tasks at hand\n", coordinator.unIssuedMapTasks.size(), coordinator.issuedMapTasks.size())
	coordinator.issuedMapMutex.Unlock()

	currTime := getNowTimeSecond()
	ret, err := coordinator.unIssuedMapTasks.popBack()
	var fileId int
	if err != nil {
		log.Printf("no map task yet, let worker wait...")
		fileId = -1
	} else {
		fileId = ret.(int)
		coordinator.issuedMapMutex.Lock()
		reply.FileName = coordinator.fileNames[fileId]
		coordinator.mapTasks[fileId].beginSecond = currTime
		coordinator.mapTasks[fileId].workerId = reply.WorkerId
		coordinator.issuedMapTasks.insert(fileId)
		coordinator.issuedMapMutex.Unlock()
		log.Printf("giving map task %v on file %v at second %v\n", fileId, reply.FileName, currTime)
	}
	reply.FileId = fileId
	reply.AllDone = false
	reply.NReduce = coordinator.nReduce

	return nil
}

type MapTaskJoinArgs struct {
	fileId   int
	workerId int
}

type MapTaskJoinReply struct {
	accept bool
}

func getNowTimeSecond() int64 {
	return time.Now().UnixNano() / int64(time.Second)
}

func (coordinator *Coordinator) JoinMapTask(args *MapTaskJoinArgs, reply *MapTaskJoinReply) error {
	log.Printf("got join request from worker %v on file %v %v\n", args.workerId, args.fileId, coordinator.fileNames[args.fileId])
	coordinator.issuedMapMutex.Lock()

	currTime := getNowTimeSecond()
	taskTime := coordinator.mapTasks[args.fileId].beginSecond
	if !coordinator.issuedMapTasks.contains(args.fileId) {
		log.Println("task abandoned or does not exists, ignoring...")
		coordinator.issuedMapMutex.Unlock()
		reply.accept = false
		return nil
	}

	if coordinator.mapTasks[args.fileId].workerId != args.workerId {
		log.Printf("map task belongs to worker %v not this worker %v, ignoring...", coordinator.mapTasks[args.fileId].workerId, args.workerId)
		coordinator.issuedMapMutex.Unlock()
		reply.accept = false
		return nil
	}

	if currTime-taskTime > maxTaskTime {
		log.Printf("tasks exceeds max wait time, abandoning")
		reply.accept = false
		coordinator.unIssuedMapTasks.putFront(args.fileId)
	} else {
		log.Println("task within max time, accpting...")
		reply.accept = true
		coordinator.issuedMapTasks.remove(args.fileId)
	}
	coordinator.issuedMapMutex.Unlock()
	return nil
}

type ReduceTaskArgs struct {
	WorkerId int
}

type ReduceTaskReply struct {
	reduceIndex int
	nReduce     int
	fileCount   int
	allDone     bool
}

func (coordinator *Coordinator) prepareAllReduceTasks() {
	for i := 0; i < coordinator.nReduce; i++ {
		log.Printf("putting %vth reduce task into channel\n", i)
		coordinator.unIssuedReduceTasks.putFront(i)
	}
}

func (coordinator *Coordinator) giveReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	log.Printf("worker %v asking for a reduce task\n", args.WorkerId)
	coordinator.issuedReduceMutex.Lock()

	if coordinator.unIssuedReduceTasks.size() == 0 && coordinator.issuedReduceTasks.size() == 0 {
		log.Printf("all reduce tasks complete, telling workers to terminate")
		coordinator.issuedReduceMutex.Unlock()
		coordinator.allDone = true
		reply.reduceIndex = -1
		reply.allDone = true
		return nil
	}

	log.Printf("%v unissued reduce tasks, %v issued reduce tasks at hand\n", coordinator.unIssuedReduceTasks.size(), coordinator.issuedMapTasks.size())

	coordinator.issuedReduceMutex.Unlock()
	currTime := getNowTimeSecond()
	ret, err := coordinator.unIssuedReduceTasks.popBack()
	var reduceIndex int
	if err != nil {
		log.Printf("no reduce task yet, let worker wait...")
		reduceIndex = -1
	} else {
		reduceIndex = ret.(int)
		coordinator.issuedReduceMutex.Lock()
		coordinator.reduceTasks[reduceIndex].beginSecond = currTime
		coordinator.reduceTasks[reduceIndex].workerId = args.WorkerId
		coordinator.issuedReduceTasks.insert(reduceIndex)
		coordinator.issuedReduceMutex.Unlock()
		log.Printf("giving reduce task %v at second %v\n", reduceIndex, currTime)
	}

	reply.reduceIndex = reduceIndex
	reply.allDone = false
	reply.nReduce = coordinator.nReduce
	reply.fileCount = len(coordinator.fileNames)

	return nil
}

type ReduceTaskJoinArgs struct {
	workerId    int
	reduceIndex int
}

type ReduceTaskJoinReply struct {
	accept bool
}

func (coordinator *Coordinator) JoinReduceTask(args *ReduceTaskJoinArgs, reply *ReduceTaskJoinReply) error {
	log.Printf("got join request from worker %v on reduce task %v\n", args.workerId, args.reduceIndex)

	coordinator.issuedReduceMutex.Lock()

	currTime := getNowTimeSecond()
	taskTime := coordinator.reduceTasks[args.reduceIndex].beginSecond
	if !coordinator.issuedReduceTasks.contains(args.reduceIndex) {
		log.Printf("task abandoned or does not exists, ignoring...")
		coordinator.issuedReduceMutex.Unlock()
		return nil
	}
	if coordinator.reduceTasks[args.reduceIndex].workerId != args.workerId {
		log.Printf("reduce task belongs to worker %v not this %v, ignoring...", coordinator.reduceTasks[args.reduceIndex].workerId, args.workerId)
		coordinator.issuedReduceMutex.Unlock()
		reply.accept = false
		return nil
	}

	if currTime-taskTime > maxTaskTime {
		log.Println("task exceeds max wait time, abandoning...")
		reply.accept = false
		coordinator.unIssuedReduceTasks.putFront(args.reduceIndex)
	} else {
		log.Println("task within max wait time, accepting...")
		reply.accept = true
		coordinator.issuedReduceTasks.remove(args.reduceIndex)
	}

	coordinator.issuedReduceMutex.Unlock()
	return nil
}

func (mapSet *MapSet) removeTimeoutMapTasks(mapTaskStates []MapTaskState, unIssuedMapTasks *BlockQueue) {
	for fileId, issued := range mapSet.mapBool {
		now := getNowTimeSecond()
		if issued {
			if now-mapTaskStates[fileId.(int)].beginSecond > maxTaskTime {
				log.Printf("worker %v on file %v abandoned due to timeout\n", mapTaskStates[fileId.(int)].workerId, fileId)
				mapSet.mapBool[fileId.(int)] = false
				mapSet.count--
				unIssuedMapTasks.putFront(fileId.(int))
			}
		}
	}
}

func (mapSet *MapSet) removeTimeoutReduceTasks(reduceTaskState []ReduceTaskState, unIssuedReduceTasks *BlockQueue) {
	for fileId, issued := range mapSet.mapBool {
		now := getNowTimeSecond()
		if issued {
			if now-reduceTaskState[fileId.(int)].beginSecond > maxTaskTime {
				log.Printf("worker %v on file %v abandoned due to timeout\n", reduceTaskState[fileId.(int)].workerId, fileId)
				mapSet.mapBool[fileId.(int)] = false
				mapSet.count--
				unIssuedReduceTasks.putFront(fileId.(int))
			}
		}
	}
}

func (coordinator *Coordinator) removeTimeoutTasks() {
	log.Println("removing timeout mapTasks...")
	coordinator.issuedMapMutex.Lock()
	coordinator.issuedMapTasks.removeTimeoutMapTasks(coordinator.mapTasks, coordinator.unIssuedMapTasks)
	coordinator.issuedMapMutex.Unlock()

	log.Printf("removing timeout reduceTasks...")
	coordinator.issuedReduceMutex.Lock()
	coordinator.issuedReduceTasks.removeTimeoutReduceTasks(coordinator.reduceTasks, coordinator.unIssuedReduceTasks)
	coordinator.issuedReduceMutex.Unlock()
}

func (coordinator *Coordinator) loopRemoveTimeoutTasks() {
	for true {
		time.Sleep(2 * 1000 * time.Millisecond)
		coordinator.removeTimeoutTasks()
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	log.Println("register...")
	rpc.Register(c)

	log.Println("handle http...")
	rpc.HandleHTTP()

	//l, e := net.Listen("tcp", ":1234")
	log.Println("get sockName...")
	sockName := coordinatorSock()
	os.Remove(sockName)
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	//ret := false

	// Your code here.
	if c.allDone {
		log.Println("asked whether i am done, replying yes...")
	} else {
		log.Println("asked whether i am done, replying no...")
	}
	return c.allDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	log.SetPrefix("coordinator:")
	log.Println("making coordinator")

	c.fileNames = files
	c.nReduce = nReduce
	c.currWorkerId = 0
	c.mapTasks = make([]MapTaskState, len(files))
	c.reduceTasks = make([]ReduceTaskState, nReduce)
	c.unIssuedMapTasks = NewBlockQueue()
	c.issuedMapTasks = newMapSet()
	c.unIssuedReduceTasks = NewBlockQueue()
	c.issuedReduceTasks = newMapSet()
	c.allDone = false
	c.mapDone = false

	c.server()
	log.Println("listening started...")

	go c.loopRemoveTimeoutTasks()

	log.Printf("file count %d\n", len(files))
	for i := 0; i < len(files); i++ {
		log.Printf("sending %vth file map task to channel\n", i)
		c.unIssuedMapTasks.putFront(i)
	}
	return &c
}
