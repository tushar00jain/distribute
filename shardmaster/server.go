package shardmaster

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

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num

	last_done int
}

type operation int

const (
	JOIN = iota
	LEAVE
	MOVE
	QUERY
)

type Op struct {
	GID       int64
	Operation operation
	Shard     int
	Servers   []string
	Num       int
	Rand      int64
}

func copy(c Config, num int) Config {
	config := Config{}

	config.Num = num
	config.Shards = c.Shards
	config.Groups = make(map[int64][]string)

	for k, v := range c.Groups {
		config.Groups[k] = v
	}

	return config
}

func min(count map[int64]int) int64 {
	m := int(^uint(0) >> 1)
	var min_key int64

	for key, value := range count {
		if value < m {
			m = value
			min_key = key
		}
	}

	return min_key
}

func shards_per_group(config Config) map[int64]int {
	count := make(map[int64]int)
	for k, _ := range config.Groups {
		count[k] = 0
	}

	for _, shard := range config.Shards {
		if shard == 0 {
			continue
		}

		count[shard]++
	}

	return count
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	rand := nrand()
	op := Op{GID: args.GID, Operation: JOIN, Servers: args.Servers, Rand: rand}

	sm.update(op)

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	rand := nrand()
	op := Op{GID: args.GID, Operation: LEAVE, Rand: rand}

	sm.update(op)

	return nil
}

func (sm *ShardMaster) update(op Op) {
	var value Op

	for {
		if decided, temp := sm.px.Status(sm.last_done + 1); decided {
			value = temp.(Op)
		} else {
			sm.px.Start(sm.last_done+1, op)
			value = sm.wait(sm.last_done + 1)
		}

		sm.execute(value)
		sm.last_done++
		sm.px.Done(sm.last_done)

		if value.Rand == op.Rand {
			return
		}
	}
}

func (sm *ShardMaster) wait(seq int) Op {
	to := 10 * time.Millisecond

	for {
		decided, value := sm.px.Status(seq)
		if decided {
			return value.(Op)
		}

		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (sm *ShardMaster) execute(op Op) {
	num := len(sm.configs)
	config := copy(sm.configs[num-1], num)

	var c Config

	if op.Operation == JOIN {
		config.Groups[op.GID] = op.Servers

		count := shards_per_group(config)
		c = sm.balance(config, count, op.GID)

	} else if op.Operation == LEAVE {
		delete(config.Groups, op.GID)
		for i, shard := range config.Shards {
			if shard == op.GID {
				config.Shards[i] = 0
			}
		}

		count := shards_per_group(config)
		c = sm.balance(config, count, -1)

	} else if op.Operation == MOVE {
		config.Shards[op.Shard] = op.GID
		c = config

	} else {
		return
	}

	sm.configs = append(sm.configs, c)
}

func (sm *ShardMaster) balance(c Config, ct map[int64]int, GID int64) Config {
	config := c
	count := ct
	new_count := make(map[int64]int)

	if GID == -1 {
		if len(count) > 0 {
			even := 10 / len(count)
			for i, shard := range config.Shards {
				if shard == 0 {
					config_shard := min(count)
					config.Shards[i] = config_shard
					if _, ok := new_count[config_shard]; ok {
						new_count[config_shard]++
					} else {
						new_count[config_shard] = 1
					}
				} else {
					if val, ok := new_count[shard]; ok {
						if val >= even {
							delete(count, shard)
							config_shard := min(count)
							config.Shards[i] = config_shard
							new_count[config_shard]++
						} else {
							new_count[shard]++
						}
					} else {
						config.Shards[i] = shard
						new_count[shard] = 1
					}
				}
			}
		} else {
			for i, _ := range config.Shards {
				config.Shards[i] = 0
			}
		}
	} else {
		if len(count) > 1 {
			// number of shards to be assigned to each group
			even := 10 / len(count)
			for i, shard := range config.Shards {
				if val, ok := new_count[shard]; ok {
					if val >= even {
						delete(count, shard)
						config_shard := min(count)
						config.Shards[i] = config_shard
						new_count[config_shard]++
					} else {
						new_count[shard]++
					}
				} else {
					config.Shards[i] = shard
					new_count[shard] = 1
				}
			}
		} else {
			// assign the only group to all the shards
			for i, _ := range config.Shards {
				config.Shards[i] = GID
			}
		}
	}

	return config
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	rand := nrand()
	op := Op{GID: args.GID, Operation: MOVE, Shard: args.Shard, Rand: rand}

	sm.update(op)

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	rand := nrand()
	op := Op{Num: args.Num, Operation: QUERY, Rand: rand}

	sm.update(op)

	num := len(sm.configs)

	if args.Num == -1 || args.Num > num {
		reply.Config = sm.configs[num-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}

	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	sm.last_done = 0

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
