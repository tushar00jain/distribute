package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all values <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- values before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	values map[int]interface{}
	done   []int

	start int // highest seq seen by Start

	instance map[int]Instance // instance information of all the peers
}

type Instance struct {
	me_p int
	me_a int

	n_p int         // highest prepare seen
	n_a int         // highest accept seen
	v_a interface{} // value of highest accept seen
}

type PrepareArgs struct {
	Me_N int
	Seq  int

	Me  int
	N   int
	Err string
}

type PrepareReply struct {
	Me_N int

	Me  int
	N   int
	V   interface{}
	Err string
}

type AcceptArgs struct {
	Me_N int
	Seq  int

	Me int
	N  int
	V  interface{}
}

type AcceptReply struct {
	Me  int
	N   int
	Err string
}

type DecidedArgs struct {
	Done int

	Me  int
	Seq int
	V   interface{}
}

type DecidedReply struct {
	Me  int
	Err string
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	reply.Me = px.me

	instance := px.instance[args.Seq]

	if (args.N*10 + args.Me_N) > (instance.n_p*10 + instance.me_p) {
		instance.me_p = args.Me
		instance.n_p = args.N

		reply.Me_N = instance.me_a
		reply.N = instance.n_a
		reply.V = instance.v_a

		px.instance[args.Seq] = instance
	} else {
		reply.Me_N = instance.me_p
		reply.N = instance.n_p
		reply.Err = "Err"
	}

	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	reply.Me = px.me

	instance := px.instance[args.Seq]

	if (args.N*10 + args.Me_N) >= (instance.n_p*10 + instance.me_p) {
		instance = Instance{me_p: args.Me_N, me_a: args.Me_N, n_p: args.N, n_a: args.N, v_a: args.V}
		px.instance[args.Seq] = instance

		reply.N = args.N
	} else {
		reply.Err = "Err"
	}

	return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	reply.Me = px.me

	if px.me != args.Me {
		// proposer sends its done value when a decision is reached
		if px.done[args.Me] < args.Done {
			px.done[args.Me] = args.Done
		}
	}

	// update max seq
	fmt.Println("DECIDED", args.Seq, "-", args.V, args.Me, "->", px.me)
	px.values[args.Seq] = args.V
	if px.start < args.Seq {
		px.start = args.Seq
	}

	return nil
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			// fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	if seq < px.Min() {
		return
	}

	go func() {
		px.mu.Lock()
		n_p := px.instance[seq].n_p
		v_a := px.instance[seq].v_a
		px.mu.Unlock()

		for {
			status_ok, _ := px.Status(seq)
			if status_ok {
				return
			}

			n_p++

			prepare_ok_servers := 0
			max := 0
			alt := false

			for i, peer := range px.peers {
				args := &PrepareArgs{Me: px.me, Me_N: px.me, N: n_p, Seq: seq}

				var reply PrepareReply
				var prepare_ok bool
				if i == px.me {
					_ = px.Prepare(args, &reply)
					prepare_ok = true
				} else {
					prepare_ok = call(peer, "Paxos.Prepare", args, &reply)
				}

				if prepare_ok {
					if reply.Err == "" {
						prepare_ok_servers++
						if (reply.N*10 + reply.Me_N) > max {
							alt = true
							max = reply.N*10 + reply.Me_N

							v_a = reply.V
						}
					} else {
						n_p = reply.N
					}
				}
			}

			if prepare_ok_servers > len(px.peers)/2 {
				accept_ok_servers := 0
				args := &AcceptArgs{Me: px.me, Me_N: px.me, N: n_p, V: v, Seq: seq}
				if alt {
					args.V = v_a
				}

				for i, peer := range px.peers {
					var reply AcceptReply
					var accept_ok bool
					if i == px.me {
						_ = px.Accept(args, &reply)
						accept_ok = true
					} else {
						accept_ok = call(peer, "Paxos.Accept", args, &reply)
					}
					if accept_ok {
						if reply.Err == "" {
							accept_ok_servers++
						}
					}
				}

				if accept_ok_servers > len(px.peers)/2 {
					for i, peer := range px.peers {
						args := &DecidedArgs{Me: px.me, Seq: seq, V: v, Done: px.done[px.me]}
						if alt {
							args.V = v_a
						}
						var reply DecidedReply
						if i == px.me {
							_ = px.Decided(args, &reply)
						} else {
							_ = call(peer, "Paxos.Decided", args, &reply)
						}
					}
				}
			}
		}
	}()
	return
}

//
// the application on this machine is done with
// all values <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()

	if px.done[px.me] < seq {
		px.done[px.me] = seq
	}

	px.mu.Unlock()

	_ = px.Min()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	return px.start
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any values it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on values that it
// missed -- the other peers therefor cannot forget these
// values.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	min := px.done[px.me]
	for _, e := range px.done {
		if e < min {
			min = e
		}
	}

	for k, _ := range px.values {
		if k <= min {
			delete(px.values, k)
			delete(px.instance, k)
		}
	}

	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.
	if seq < px.Min() {
		return false, nil
	}

	px.mu.Lock()
	defer px.mu.Unlock()

	if val, ok := px.values[seq]; ok {
		return true, val
	}

	return false, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.values = make(map[int]interface{})
	px.done = make([]int, len(peers))
	for i := range px.done {
		px.done[i] = -1
	}

	px.instance = make(map[int]Instance)

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
