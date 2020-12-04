package chord

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type Finger struct {
	ID   Key
	Addr *net.TCPAddr
}

type Fingers []*Finger

func (a Fingers) Len() int { return len(a) }

type virtualNode struct {
	ID        Key
	virtualID uint32

	lock        sync.Mutex
	fingers     Fingers
	predecessor *Finger
	successors  Fingers
}

func CreateVirtualNode(addr *net.TCPAddr, virtualID uint32) *virtualNode {
	res := new(virtualNode)
	res.ID = CreateKey(addr, virtualID)
	res.virtualID = virtualID
	for i := 0; i < maxFingers; i++ {
		res.fingers = append(res.fingers, nil)
	}
	return res
}

// Gets the successor
// REQUIRES LOCKING
func (vnode *virtualNode) getSuccessor(i uint) *Finger {
	if i == 0 {
		return vnode.fingers[0]
	} else {
		return vnode.successors[i-1]
	}
}

// REQUIRES LOCKING
func (vnode *virtualNode) setSuccessor(i uint, succ *Finger) {
	if i == 0 {
		vnode.fingers[0] = succ
	} else {
		vnode.successors[i-1] = succ
	}
}

// REQUIRES LOCKING
func (vnode *virtualNode) findClosestPredecessor(ID Key, localAddr *net.TCPAddr) *Finger {
	ran := vnode.ID.To(&ID)
	for i := vnode.fingers.Len() - 1; i > 0; i-- {
		if vnode.fingers[i] != nil && vnode.fingers[i].ID.In(&ran) {
			return vnode.fingers[i]
		}
	}
	res := new(Finger)
	res.ID = vnode.ID
	res.Addr = localAddr
	return res
}

// REQUIRES LOCKING
func (vnode *virtualNode) notify(predecessor *Finger) {
	if vnode.predecessor == nil {
		vnode.predecessor = predecessor
		return
	}
	ran := vnode.predecessor.ID.To(&vnode.ID)
	if predecessor.ID.In(&ran) {
		vnode.predecessor = predecessor
	}
}

func call(addr *net.TCPAddr, name string, args interface{}, result interface{}) error {
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		// TODO Should this remove the successor from the list?
		return fmt.Errorf("Failed to connect to %v: %v", addr, err)
	}
	rpc := rpc.NewClient(conn)
	err = rpc.Call(name, args, result)
	if err != nil {
		firstErr := fmt.Errorf("Error during RPC call: %v", err)
		err = rpc.Close()
		if err != nil {
			return fmt.Errorf("%v\nAlso: Error while closing RPC client: %v", firstErr, err)
		}
		return firstErr
	}
	rpc.Close()
	return nil
}

// DON'T LOCK
func (vnode *virtualNode) FindSuccessor(find Key, cfg *Config) (*Finger, error) {
	cur := &Finger{
		ID:   vnode.ID,
		Addr: cfg.host,
	}
	vnode.lock.Lock()
	succ := vnode.getSuccessor(0)
	vnode.lock.Unlock()
	ran := cur.ID.To(&succ.ID)
	for !find.In(&ran) {
		{
			args := FindClosestPredecessorArg{
				ID:   cur.ID,
				Find: find,
			}
			res := new(FindClosestPredecessorRes)
			err := call(cur.Addr, "Node.FindClosestPredecessor", &args, res)
			if err != nil {
				// TODO better error handling
				return nil, err
			}
			if res.Predecessor == nil {
				panic("res.Predecessor == nil")
			}
			cur = res.Predecessor
		}
		args := SuccessorArgs{
			ID: cur.ID,
		}
		res := new(SuccessorResult)
		err := call(cur.Addr, "Node.Successor", &args, res)
		if err != nil {
			// TODO better error handling
			return nil, err
		}
		if res.Successors == nil {
			panic("res.Successors == nil")
		}
		if succ.ID.Cmp(&res.Successors.ID) == 0 {
			return succ, nil
		}
		succ = res.Successors
		ran = cur.ID.To(&succ.ID)
	}
	return cur, nil
}

// Add random fluctation to sleeping to time spread load over time
func sleepInterval(cfg *Config) {
	rand := rand.Int63()%200 - 100
	if rand == 0 {
		rand = 1
	}
	fluctuation := cfg.stabilizeInterval.Nanoseconds() / int64(10) / rand
	t := cfg.stabilizeInterval.Nanoseconds() + fluctuation
	duration := time.Duration(t)
	time.Sleep(duration)
}

// Fix predecessors and successors
func (vnode *virtualNode) stabilize(cfg *Config) {
	for {
		log.Tracef("[%v]:stabilize", vnode.ID.Readable())
		vnode.lock.Lock()
		succ := vnode.getSuccessor(0)
		vnode.lock.Unlock()
		// Just some assertions for sanity
		if succ == nil {
			panic("succ == nil")
		}
		if succ.Addr == nil {
			panic("succ.Addr == nil")
		}

		// Get the predecessor from the
		args := PredecessorArgs{
			ID: succ.ID,
		}
		res := new(PredecessorResult)
		err := call(succ.Addr, "Node.Predecessor", &args, res)
		if err != nil {
			log.Warnf("Error trying to retrieve successor.predecessor at addr %v : %v", succ.Addr, err)
			sleepInterval(cfg)
			continue
		}
		if res.Predecessor != nil {
			ran := vnode.ID.To(&succ.ID)
			if res.Predecessor.ID.In(&ran) {
				vnode.lock.Lock()
				vnode.setSuccessor(0, res.Predecessor)
				vnode.lock.Unlock()
			}
			succ = res.Predecessor
		}
		notifyArgs := NotifyArgs{
			ID: succ.ID,
			Notify: &Finger{
				ID:   vnode.ID,
				Addr: cfg.host,
			},
		}
		notifyRes := new(NotifyRes)
		err = call(succ.Addr, "Node.Notify", &notifyArgs, notifyRes)
		if err != nil {
			log.Warnf("Error trying to notify successor: %v", err)
		}
		sleepInterval(cfg)
	}
}

func (vnode *virtualNode) fixFingers(cfg *Config) {
	for {
		log.Tracef("[%v]:fixFingers", vnode.ID.Readable())
		random := rand.Uint32() % maxFingers
		id := vnode.ID.Next(uint(random))
		new, err := vnode.FindSuccessor(id, cfg)
		if err != nil {
			log.Warnf("Failed to fix a finger: %v", err)
		} else {
			vnode.lock.Lock()
			vnode.fingers[random] = new
			vnode.lock.Unlock()
		}
		sleepInterval(cfg)
	}
}
