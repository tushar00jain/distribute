package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.
	vshost string
	view   viewservice.View
	db     map[string]string
	rand   map[int64]string
	mu     sync.Mutex
}

func (pb *PBServer) Copy(args *CopyArgs, reply *CopyReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	for k, v := range args.Db {
		pb.db[k] = v
	}

	for k, v := range args.Rand {
		pb.rand[k] = v
	}

	reply.Err = ""
	return nil
}

func (pb *PBServer) copy() {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Backup == "" {
		return
	}

	args := CopyArgs{Db: pb.db, Rand: pb.rand}
	var reply CopyReply
	ok := call(pb.view.Backup, "PBServer.Copy", args, &reply)

	if ok {
		return
	}

	fmt.Println("Backup Copy failed")
}

func (pb *PBServer) replicateGet(args *GetArgs, method string) {
	if pb.view.Backup == "" {
		return
	}

	var reply GetReply
	bargs := *args
	bargs.Me = pb.me
	ok := call(pb.view.Backup, method, bargs, &reply)

	if ok {
		return
	}

	fmt.Println("Backup Get failed")
}

func (pb *PBServer) replicatePut(args *PutArgs, method string) {
	if pb.view.Backup == "" {
		return
	}

	var breply PutReply
	bargs := *args
	bargs.Me = pb.me
	ok := call(pb.view.Backup, method, bargs, &breply)

	if ok {
		return
	}

	fmt.Println("Backup Put failed")
}

func (pb *PBServer) get(args *GetArgs, reply *GetReply) {
	if val, ok := pb.rand[args.Rand]; ok {
		// handle duplicates requests
		reply.Value = val
		reply.Err = ""
		return
	}

	if val, ok := pb.db[args.Key]; ok {
		// key already exisits
		reply.Value = val
		reply.Err = ""
	} else {
		// key does not exist
		reply.Value = ""
		reply.Err = ErrNoKey
	}

	pb.rand[args.Rand] = reply.Value
}

func (pb *PBServer) put(args *PutArgs, reply *PutReply) {
	if val, ok := pb.rand[args.Rand]; ok {
		// handle duplicates requests
		reply.PreviousValue = val
		reply.Err = ""
		return
	}

	if args.DoHash {
		// PutHash
		if val, ok := pb.db[args.Key]; ok {
			// key already exists
			reply.PreviousValue = val
			h := hash(val + args.Value)
			pb.db[args.Key] = strconv.Itoa(int(h))
		} else {
			// key does not exist
			reply.PreviousValue = ""
			h := hash(args.Value)
			pb.db[args.Key] = strconv.Itoa(int(h))
		}
	} else {
		// Put
		pb.db[args.Key] = args.Value
		if val, ok := pb.db[args.Key]; ok {
			// key already exists
			reply.PreviousValue = val
		} else {
			// key does not exist
			reply.PreviousValue = ""
		}
	}

	reply.Err = ""
	pb.rand[args.Rand] = reply.PreviousValue
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Primary == pb.me {
		// primary server
		pb.replicateGet(args, "PBServer.Get")
		pb.get(args, reply)
	} else if pb.view.Backup == pb.me {
		// backup server
		if pb.view.Primary == args.Me {
			// request came from primary
			pb.get(args, reply)
		} else {
			// request did not come from primary
			reply.Value = ""
			reply.Err = ErrWrongServer
		}
	} else {
		// request came to unknown server
		reply.Value = ""
		reply.Err = ErrWrongServer
	}

	return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Primary == pb.me {
		// primary server
		pb.replicatePut(args, "PBServer.Put")
		pb.put(args, reply)
	} else if pb.view.Backup == pb.me {
		// backup server
		if pb.view.Primary == args.Me {
			// request came from primary
			pb.put(args, reply)
		} else {
			// request did not come from primary
			reply.PreviousValue = ""
			reply.Err = ErrWrongServer
		}
	} else {
		// request came to unknown server
		reply.PreviousValue = ""
		reply.Err = ErrWrongServer
	}

	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.
	temp := pb.view.Backup

	args := &viewservice.PingArgs{Me: pb.me, Viewnum: pb.view.Viewnum}
	var reply viewservice.PingReply
	ok := call(pb.vshost, "ViewServer.Ping", args, &reply)

	if ok {
		if reply.View.Primary == pb.me || reply.View.Backup == pb.me {
			// server is primary or backup
			pb.view = reply.View
		} else {
			// server is unknown
			pb.view.Viewnum = 0
		}

		if reply.View.Primary == pb.me && pb.view.Backup != temp {
			pb.copy()
		}

		return
	}

	pb.view = viewservice.View{Viewnum: 0, Primary: "", Backup: ""}
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.vshost = vshost
	pb.view = viewservice.View{Viewnum: 0, Primary: "", Backup: ""}
	pb.db = make(map[string]string)
	pb.rand = make(map[int64]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
