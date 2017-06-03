package mapreduce

import "container/list"
import "fmt"
import "sync"

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
	var mu sync.Mutex

	done := make(chan bool)
	work := make(chan *DoJobArgs)

	startWorker := func(worker string) {
		for {
			args := <-work
			var res DoJobReply
			ok := call(worker, "Worker.DoJob", args, &res)
			if ok {
				done <- true
			} else {
				fmt.Printf("DoJob: RPC %s Job error\n", worker)
				delete(mr.Workers, worker)
				// add work back to the work channel
				work <- args
				break
			}
		}
	}

	go func() {
		for {
			worker := <-mr.registerChannel
			if _, ok := mr.Workers[worker]; !ok {
				mu.Lock()
				mr.Workers[worker] = &WorkerInfo{worker}
				mu.Unlock()
			}

			go startWorker(worker)
		}
	}()

	go func() {
		for i := 0; i < mr.nMap; i++ {
			args := &DoJobArgs{mr.file, Map, i, mr.nReduce}
			work <- args
		}
	}()

	for i := 0; i < mr.nMap; i++ {
		<-done
	}

	go func() {
		for i := 0; i < mr.nReduce; i++ {
			args := &DoJobArgs{mr.file, Reduce, i, mr.nMap}
			work <- args
		}
	}()

	for i := 0; i < mr.nReduce; i++ {
		<-done
	}

	return mr.KillWorkers()
}
