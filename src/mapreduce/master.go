package mapreduce

import (
	"container/list"
	"sync"
)
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) runWorkerJobs(jobType JobType) {
	var jobNumber int
	var otherNumber int

	switch jobType {
	case Map:
		jobNumber = mr.nMap
		otherNumber = mr.nReduce
	case Reduce:
		jobNumber = mr.nReduce
		otherNumber = mr.nMap
	}

	var wg sync.WaitGroup
	wg.Add(jobNumber)

	for i := 0; i < jobNumber; {
		fmt.Printf("Master: Try to start %s job %d!\n", jobType, i)
		select {
		case workerAddress := <-mr.registerChannel:
			mr.workersChannel <- workerAddress
		case workerAddress := <-mr.workersChannel: {
			go func(job int) {
				args := &DoJobArgs{
					File:          mr.file,
					Operation:     jobType,
					JobNumber:     job,
					NumOtherPhase: otherNumber,
				}
				var reply DoJobReply
				ok := call(workerAddress, "Worker.DoJob", args, &reply)
				if !ok || !reply.OK {
					if !ok {
						fmt.Printf("Master call Error: Worker %s call error, Job Id %d, Job Type %s!\n",
							workerAddress, job, Map)
					} else {
						fmt.Printf("DoJob Error: Worker %s reply not OK, Job Id %d, Job Type %s!\n",
							workerAddress, job, Map)
					}
					return
				}
				mr.workersChannel <- workerAddress
				wg.Done()
				fmt.Printf("Master: %s Job Number %d Done!\n", jobType, job)
			}(i)

			i++
		}
		}
	}

	wg.Wait()
}


func (mr *MapReduce) RunMaster() *list.List {
	fmt.Printf("Master: Map Reduce has %d map jobs, %d reduce jobs!\n", mr.nMap, mr.nReduce)

	mr.runWorkerJobs(Map)
	mr.runWorkerJobs(Reduce)

	return mr.KillWorkers()
}
