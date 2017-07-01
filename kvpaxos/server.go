package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type operation int

const (
	GET operation = iota
	PUT
)

type Op struct {
	Key    string
	Value  string
	DoHash bool

	Operation operation
	Rand      int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	db map[string]string

	rand      map[int64]string
	last_done int
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if val, ok := kv.rand[args.Rand]; ok {
		reply.Value = val
		return nil
	}

	op := Op{Key: args.Key, Value: "", DoHash: false, Operation: GET, Rand: args.Rand}
	last := kv.update(op)

	reply.Value = last

	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if val, ok := kv.rand[args.Rand]; ok {
		reply.PreviousValue = val
		return nil
	}

	op := Op{Key: args.Key, Value: args.Value, DoHash: args.DoHash, Operation: PUT, Rand: args.Rand}
	last := kv.update(op)

	reply.PreviousValue = last

	return nil
}

func (kv *KVPaxos) update(op Op) string {
	var value Op
	for {
		if decided, temp := kv.px.Status(kv.last_done + 1); decided {
			value = temp.(Op)
		} else {
			kv.px.Start(kv.last_done+1, op)
			value = kv.wait(kv.last_done + 1)
		}

		last := kv.execute(value)
		kv.last_done++
		kv.px.Done(kv.last_done)

		if value == op {
			return last
		}
	}
}

func (kv *KVPaxos) execute(op Op) string {
	last, _ := kv.db[op.Key]
	kv.rand[op.Rand] = last

	if op.Operation == PUT {
		if op.DoHash {
			kv.db[op.Key] = NextValue(last, op.Value)
		} else {
			kv.db[op.Key] = op.Value
		}
	}

	return last
}

func (kv *KVPaxos) wait(seq int) Op {
	to := 10 * time.Millisecond

	for {
		decided, value := kv.px.Status(seq)
		if decided {
			return value.(Op)
		}

		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.db = make(map[string]string)

	kv.rand = make(map[int64]string)
	kv.last_done = 0

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
