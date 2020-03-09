package mapreduce

import (
	"container/list"
)
import "fmt"


type WorkerInfo struct {
	address string
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

func (mr *MapReduce) startListenWorkerRegister() {
	Loop:
		for {
			select {
				case workerAddress := <-mr.registerChannel:
					mr.workersChannel <- workerAddress
				case <-mr.listenerDoneChannel: {
					break Loop
				}
			}
		}
}

func (mr *MapReduce) endListenWorkerRegister() {
	mr.listenerDoneChannel <- true
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

	jobsChannel := make(chan *DoJobArgs, jobNumber)
	doneChannel := make(chan struct{}, jobNumber)
	completeChannel := make(chan struct{}, 1)

	produceJobs := func() {
		for i := 0; i < jobNumber; i++ {
			args := &DoJobArgs{
				File:	mr.file,
				Operation:	jobType,
				JobNumber:  i,
				NumOtherPhase: otherNumber,
			}
			jobsChannel <- args
		}
	}

	go produceJobs()

	completed := 0

	Loop:
	for {
		select {
			case args := <-jobsChannel: {
				workerAddress := <-mr.workersChannel
				fmt.Printf("Master: Try to start %s job %d!\n", jobType, args.JobNumber)
				go func (){
					var reply DoJobReply
					ok := call(workerAddress, "Worker.DoJob", args, &reply)
					if !ok || !reply.OK{
						jobsChannel <- args
				 	} else{
				 		doneChannel <- struct{}{}
				 		fmt.Printf("Master: %s Job Number %d Done!\n", jobType, args.JobNumber)
				 	}
				 	mr.workersChannel <- workerAddress
				}()
			}
			case <-doneChannel: {
				completed += 1
				fmt.Printf("Master: completed %d %s jobs!\n", completed, jobType)
				if completed == jobNumber {
					completeChannel <- struct{}{}
				}
			}
			case <-completeChannel: {
				fmt.Printf("Master: completed all %s jobs!\n", jobType)
				break Loop
			}
		}
	}
}


func (mr *MapReduce) RunMaster() *list.List {
	fmt.Printf("Master: Map Reduce has %d map jobs, %d reduce jobs!\n", mr.nMap, mr.nReduce)

	go mr.startListenWorkerRegister()

	mr.runWorkerJobs(Map)
	mr.runWorkerJobs(Reduce)

	mr.endListenWorkerRegister()

	return mr.KillWorkers()
}
