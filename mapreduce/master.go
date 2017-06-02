package mapreduce

import "container/list"
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

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	mr.Workers = make(map[string]*WorkerInfo)
	done := make(chan bool)

	var submitJob func(args *DoJobArgs)
	submitJob = func(args *DoJobArgs) {
		worker := <-mr.registerChannel
		if _, ok := mr.Workers[worker]; !ok {
			mr.Workers[worker] = &WorkerInfo{worker}
		}

		var res DoJobReply
		ok := call(worker, "Worker.DoJob", args, &res)
		if ok {
			done <- true
			mr.registerChannel <- worker
		} else {
			fmt.Printf("DoJob: RPC %s Job error\n", worker)
			delete(mr.Workers, worker)
			// recursively keep trying with a different worker
			submitJob(args)
		}
	}

	go func() {
		for i := 0; i < mr.nMap; i++ {
			args := &DoJobArgs{mr.file, Map, i, mr.nReduce}
			go func(args *DoJobArgs) {
				submitJob(args)
			}(args)
		}
	}()

	for i := 0; i < mr.nMap; i++ {
		<-done
	}

	go func() {
		for i := 0; i < mr.nReduce; i++ {
			args := &DoJobArgs{mr.file, Reduce, i, mr.nMap}
			go func(args *DoJobArgs) {
				submitJob(args)
			}(args)
		}
	}()

	for i := 0; i < mr.nReduce; i++ {
		<-done
	}

	close(done)
	return mr.KillWorkers()
}

// one more way would be to keep track of all the number of jobs remaining instead of creating a chan

// func (mr *MapReduce) RunMaster() *list.List {
// 	// Your code here
// 	mr.Workers = make(map[string]*WorkerInfo)
//
// 	findWorker := func() (worker string) {
// 		worker = <-mr.registerChannel
// 		if _, ok := mr.Workers[worker]; !ok {
// 			mr.Workers[worker] = &WorkerInfo{worker}
// 		}
// 		return
// 	}
//
// 	var submitJob func(worker string, args *DoJobArgs)
// 	submitJob = func(worker string, args *DoJobArgs) {
// 		var res DoJobReply
//
// 		ok := call(worker, "Worker.DoJob", args, &res)
// 		if ok {
// 			mr.registerChannel <- worker
// 		} else {
// 			fmt.Printf("DoJob: RPC %s Job error\n", worker)
// 			delete(mr.Workers, worker)
//
// 			// recursively keep trying with a different worker
// 			worker := findWorker()
// 			submitJob(worker, args)
// 		}
// 	}
//
// 	// assign unique job id
// 	maps, reduce := 0, 0
// 	for reduce != mr.nReduce {
// 		worker := findWorker()
//
// 		var args *DoJobArgs
// 		if maps != mr.nMap {
// 			args = &DoJobArgs{mr.file, Map, maps, mr.nReduce}
// 			maps += 1
// 		} else {
// 			args = &DoJobArgs{mr.file, Reduce, reduce, mr.nMap}
// 			reduce += 1
// 		}
//
// 		go func(worker string, args *DoJobArgs) {
// 			submitJob(worker, args)
// 		}(worker, args)
// 	}
//
// 	return mr.KillWorkers()
// }
