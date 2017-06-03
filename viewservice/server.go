package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	record map[string]time.Time
	view   View
	acked  bool
}

//
// server Ping RPC handler.
//

func (vs *ViewServer) promote() {
	// promote backup to primary
	if vs.view.Backup != "" {
		vs.view.Viewnum++
		vs.view.Primary = vs.view.Backup
		vs.view.Backup = ""
	}
}

func (vs *ViewServer) healthy(srv string) bool {
	// returns true if the server is dead
	return time.Since(vs.record[srv]) > DeadPings*PingInterval
}

func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.record[args.Me] = time.Now()

	if args.Me == vs.view.Primary && args.Viewnum == vs.view.Viewnum {
		// has the primary ACKed
		vs.acked = true
	}

	if args.Viewnum == 0 && vs.acked {
		if args.Me == vs.view.Primary {
			// primary restarted
			vs.promote()
		} else if args.Me == vs.view.Backup {
			// backup restarted
			vs.view.Viewnum++
		} else {
			// idle server
			if vs.view.Primary == "" {
				// no primary
				vs.promote()
				if vs.view.Primary == "" {
					// no backup to promote
					vs.view.Viewnum++
					vs.view.Primary = args.Me
				}
			} else if vs.view.Backup == "" {
				// no backup
				vs.view.Viewnum++
				vs.view.Backup = args.Me
			} else {
				// nothing
			}
		}
	}

	if args.Me == vs.view.Primary && args.Viewnum != vs.view.Viewnum {
		// change ACKed to false if the view changed
		vs.acked = false
	}

	reply.View = vs.view
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.view
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//

func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if !vs.acked {
		// can't chagne the view if the primary has not ACKed
		return
	}

	temp := vs.view.Viewnum

	if vs.view.Primary != "" {
		if vs.healthy(vs.view.Primary) {
			vs.promote()
		}
	}

	if vs.view.Backup != "" {
		if vs.healthy(vs.view.Backup) {
			vs.view.Backup = ""
			vs.view.Viewnum++
		}
	}

	if temp != vs.view.Viewnum {
		// primary has not ACKed since the view changed
		vs.acked = false
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.record = make(map[string]time.Time)
	vs.view = View{Viewnum: 0, Primary: "", Backup: ""}
	vs.acked = true

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
