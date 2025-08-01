package mr

import (
	"log"
	"math/rand"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mu           sync.Mutex
	mapTasks     []TaskMetaData
	reduceTasks  []TaskMetaData
	inputFiles   []string
	nMap         int
	nReduce      int
	isMapDone    bool
	isReduceDone bool
}

type TaskMetaData struct {
	TimeReceived time.Time
	TaskType     string
	FileName     string
	WorkerID     int
	TaskStatus   string
	TaskID       int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

/*type RequestedTaskReply struct {
	TaskType string
	FileName string
	NReduce  int
	TaskID   int
}*/

func (c *Coordinator) AssignTask(requestTask *RequestTask, taskReply *RequestedTaskReply) {
	c.mu.Lock()
	defer c.mu.Unlock()

	mapCompleteCount := 0
	reduceCompleteCount := 0

	rand.Seed(time.Now().UnixNano())

	for _, t := range c.mapTasks {
		if t.TaskStatus == "COMPLETE" {
			mapCompleteCount++
		}
	}
	c.isMapDone = (mapCompleteCount == len(c.mapTasks))

	if len(c.mapTasks) != 0 && !c.isMapDone {

		for i := 0; i < len(c.mapTasks); i++ {
			if c.mapTasks[i].TaskStatus == "IDLE" {
				taskReply.TaskType = "MAP"
				taskReply.FileName = c.mapTasks[i].FileName
				taskReply.NReduce = c.nReduce
				taskReply.TaskID = rand.Intn(99999)
				c.mapTasks[i].TaskID = rand.Intn(999999)
				c.mapTasks[i].WorkerID = requestTask.WorkerID
				c.mapTasks[i].TaskStatus = "IP"
				c.mapTasks[i].TimeReceived = time.Now()
				c.mapTasks[i].TaskType = "MAP"
				break
			}

		}

	}

	if c.isMapDone && len(c.reduceTasks) != 0 {
		for i := 0; i < len(c.reduceTasks); i++ {
			if c.reduceTasks[i].TaskStatus == "IDLE" {
				taskReply.TaskType = "REDUCE"
				taskReply.FileName = c.reduceTasks[i].FileName
				taskReply.NReduce = c.nReduce
				taskReply.TaskID = rand.Intn(99999)
				c.reduceTasks[i].TaskID = rand.Intn(999999)
				c.reduceTasks[i].WorkerID = requestTask.WorkerID
				c.reduceTasks[i].TaskStatus = "IP"
				c.reduceTasks[i].TimeReceived = time.Now()
				c.reduceTasks[i].TaskType = "REDUCE"
				break
			} else if c.reduceTasks[i].TaskStatus == "COMPLETE" {
				reduceCompleteCount++
			}
		}

	}
}

func (c *Coordinator) RecievedTaskDone(TheReport *ReportBackToMaster) {
	if TheReport.TaskType == "MAP" {
		for i := 0; i < len(c.mapTasks); i++ {
			if c.mapTasks[i].TaskID == TheReport.TaskID {
				c.mapTasks[i].TaskStatus = "COMPLETE"
				break
			}
		}
	} else if TheReport.TaskType == "MAP" {
		for i := 0; i < len(c.mapTasks); i++ {
			if c.mapTasks[i].TaskID == TheReport.TaskID {
				c.mapTasks[i].TaskStatus = "COMPLETE"
				break
			}
		}
	}
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

	return c.isReduceDone && c.isMapDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

/*type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	mapTasks    []TaskMetaData
	reduceTasks []TaskMetaData
	inputFiles  []string
	nMap        int
	nReduce     int
}*/

/*type TaskMetaData struct {
	TimeReceived time.Time
	TaskType     string
	FileName     string
	WorkerID     int
	TaskStatus   string
}*/

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		isMapDone:    false,
		isReduceDone: false,
	}

	for i := 0; i < len(files); i++ {
		taskM := TaskMetaData{
			TaskType:   "MAP",
			FileName:   files[i],
			TaskStatus: "IDLE",
		}
		c.mapTasks = append(c.mapTasks, taskM)
		c.nReduce = nReduce

	}

	for i := 0; i < nReduce; i++ {
		taskR := TaskMetaData{
			TaskType:   "REDUCE",
			TaskStatus: "IDLE",
		}
		c.reduceTasks = append(c.reduceTasks, taskR)
	}
	// Your code here.

	c.server()
	return &c
}
