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

	// marks completion of one job
	done := make(chan bool)
	// finish using one worker
	finish := make(chan bool)
	// done processing all jobs
	quit := make(chan bool)

	work := make(chan *DoJobArgs)

	startWorker := func(worker string) {
	loop:
		for {
			select {
			case args := <-work:
				var res DoJobReply
				ok := call(worker, "Worker.DoJob", args, &res)
				if ok {
					done <- true
				} else {
					fmt.Printf("DoJob: RPC %s Job error\n", worker)
					delete(mr.Workers, worker)
					// add work back to the work channel
					work <- args
					break loop
				}
			case <-finish:
				break loop
			}
		}
	}

	go func() {
	loop:
		for {
			select {
			case worker := <-mr.registerChannel:
				mr.Workers[worker] = &WorkerInfo{worker}
				go func(worker string) {
					startWorker(worker)
				}(worker)
			case <-quit:
				for _, _ = range mr.Workers {
					finish <- true
				}
				close(finish)
				close(work)
				break loop
			}
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

	quit <- true
	close(quit)
	close(done)

	return mr.KillWorkers()
}
