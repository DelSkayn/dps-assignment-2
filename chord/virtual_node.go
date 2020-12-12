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
	ID            Key
	virtualID     uint32
	numSuccessors uint32

	lock        sync.Mutex
	fingers     Fingers
	predecessor *Finger
	successors  Fingers
}

func CreateVirtualNode(addr *net.TCPAddr, virtualID uint32, bitsInKey uint32, numSuccessors uint32) *virtualNode {
	res := new(virtualNode)
	res.ID = CreateKey(addr, virtualID, bitsInKey)
	res.virtualID = virtualID
	for i := 0; i < int(bitsInKey); i++ {
		res.fingers = append(res.fingers, nil)
	}
	res.numSuccessors = numSuccessors
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
func (vnode *virtualNode) invalidateSuccessor(i uint) {
	if i == 0 {
		vnode.fingers[0] = nil
		if len(vnode.successors) != 0 {
			vnode.fingers[0] = vnode.successors[0]
			vnode.successors = vnode.successors[1:]
		}
	} else {
		vnode.successors = append(vnode.successors[:i+1], vnode.successors[i+2:]...)
	}
}

// REQUIRES LOCKING
func (vnode *virtualNode) appendSuccessor(succ *Finger) {
	len := vnode.successors.Len() + 1
	prev := vnode.ID
	i := 0
	for i < len {
		cur := vnode.getSuccessor(uint(i)).ID
		if cur.Cmp(&succ.ID) == 0 {
			return
		}
		ran := prev.To(&cur)
		if succ.ID.In(&ran) {
			break
		}
		prev = cur
		i++
	}
	if i == 0 {
		prev := vnode.getSuccessor(0)
		vnode.setSuccessor(0, succ)
		vnode.successors = append((Fingers{prev}), vnode.successors...)
	} else {
		idx := i - 1
		if idx == vnode.successors.Len() {
			vnode.successors = append(vnode.successors, succ)
		} else {
			vnode.successors = append(vnode.successors[:idx+1], vnode.successors[idx:]...)
			vnode.successors[idx] = succ
		}
	}
	if uint32(vnode.successors.Len()+1) > vnode.numSuccessors {
		vnode.successors = vnode.successors[:vnode.numSuccessors-1]
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

func (vnode *virtualNode) stabilizeLoop(cfg *Config) {
	for {
		vnode.stabilize(cfg)
		sleepInterval(cfg)
	}
}

// Fix predecessors and successors
func (vnode *virtualNode) stabilize(cfg *Config) {
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

	// Get the predecessor from the successor
	args := PredecessorArgs{
		ID: succ.ID,
	}
	res := new(PredecessorResult)
	err := call(succ.Addr, "Node.Predecessor", &args, res)
	if err != nil {
		log.Warnf("Error trying to retrieve successor.predecessor at addr %v : %v", succ.Addr, err)
		vnode.invalidateSuccessor(0)
		return
	}
	if res.Predecessor != nil {
		vnode.lock.Lock()
		vnode.appendSuccessor(res.Predecessor)
		succ = vnode.getSuccessor(0)
		vnode.lock.Unlock()
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
		return
	}
}

func (vnode *virtualNode) maintainSuccessorsLoop(cfg *Config) {
	for {
		vnode.maintainSuccessors(cfg)
		sleepInterval(cfg)
	}
}

func (vnode *virtualNode) maintainSuccessors(cfg *Config) {
	// Find additional successor

	vnode.lock.Lock()
	pick := uint(rand.Int()) % uint(vnode.successors.Len())
	succ := vnode.getSuccessor(pick)
	vnode.lock.Unlock()
	arg := SuccessorArgs{
		ID: succ.ID,
	}
	res := new(SuccessorResult)
	if err := call(succ.Addr, "Node.Successor", &arg, res); err != nil {
		log.Warnf("Error trying to find successor of successor: %v", err)
		vnode.lock.Lock()
		vnode.invalidateSuccessor(pick)
		vnode.lock.Unlock()
		return
	} else {
		vnode.lock.Lock()
		vnode.appendSuccessor(res.Successors)
		vnode.lock.Unlock()
	}
}

func (vnode *virtualNode) fixFingersLoop(cfg *Config) {
	for {
		vnode.fixFingers(cfg)
		sleepInterval(cfg)
	}
}

func (vnode *virtualNode) fixFingers(cfg *Config) {
	log.Tracef("[%v]:fixFingers", vnode.ID.Readable())
	random := rand.Uint32() % cfg.bitsInKey
	id := vnode.ID.Next(uint(random))
	new, err := vnode.FindSuccessor(id, cfg)
	if err != nil {
		log.Warnf("Failed to fix a finger: %v", err)
	} else {
		vnode.lock.Lock()
		vnode.fingers[random] = new
		vnode.lock.Unlock()
	}
}

func (vnode *virtualNode) checkPredecessorLoop(cfg *Config) {
	for {
		vnode.checkPredecessor(cfg)
		sleepInterval(cfg)
	}
}

func (vnode *virtualNode) checkPredecessor(cfg *Config) {
	log.Tracef("[%v]:fixFingers", vnode.ID.Readable())
	vnode.lock.Lock()
	pred := vnode.predecessor
	vnode.lock.Unlock()
	if pred == nil {
		return
	}
	args := SuccessorArgs{
		ID: pred.ID,
	}
	res := new(SuccessorResult)
	if err := call(pred.Addr, "Node.Successor", &args, res); err != nil {
		log.Warnf("Predecessor failed: %v", err)
		vnode.lock.Lock()
		if vnode.predecessor.ID.Cmp(&pred.ID) == 0 {
			vnode.predecessor = nil
		}
		vnode.lock.Unlock()
	}
}
