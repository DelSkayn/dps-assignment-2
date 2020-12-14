package chord

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"time"

	log "github.com/sirupsen/logrus"
)

type virtualNode struct {
	ID            Key
	virtualID     uint32
	numSuccessors uint32

	fingerTable fingerTable
}

func CreateVirtualNode(addr *net.TCPAddr, virtualID uint32, bitsInKey uint32, numSuccessors uint32, successor *Finger) *virtualNode {
	res := new(virtualNode)
	res.ID = CreateKey(addr, virtualID, bitsInKey)
	res.virtualID = virtualID
	res.fingerTable = CreateFingerTable(res.ID, bitsInKey, numSuccessors, successor)
	return res
}

func (vnode *virtualNode) findClosestPredecessor(ID Key, localAddr *net.TCPAddr) *Finger {
	var res *Finger
	vnode.fingerTable.with(func(data *fingerTableData) {
		res = data.findClosestPredecessor(ID)
	})
	if res == nil {
		return &Finger{
			ID:   ID,
			Addr: localAddr,
		}
	}
	return res
}

// REQUIRES LOCKING
func (vnode *virtualNode) notify(predecessor *Finger) {
	vnode.fingerTable.with(func(data *fingerTableData) {
		if data.predecessor == nil {
			data.predecessor = predecessor
		} else {
			ran := data.predecessor.ID.To(&data.ID)
			if predecessor.ID.In(&ran) {
				data.predecessor = predecessor
			}
		}
	})
}

func call(addr *net.TCPAddr, name string, args interface{}, result interface{}) error {
	log.Tracef("calling [%v]%v", addr, name)
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

func (vnode *virtualNode) FindSuccessor(find Key, cfg *Config) (*Finger, error) {
	var succ *Finger
	vnode.fingerTable.with(func(data *fingerTableData) {
		succ = data.fingers[0]
	})
	ran := vnode.ID.To(&succ.ID)
	if find.In(&ran) {
		return &Finger{
			ID:   vnode.ID,
			Addr: cfg.host,
		}, nil
	}
	pred := vnode.findClosestPredecessor(find, cfg.host)
	if pred == nil {
		panic("failed to find a single predecessor")
	}
	succArg := SuccessorArgs{
		ID: pred.ID,
	}
	succRes := new(SuccessorResult)
	if err := call(pred.Addr, "Node.Successor", &succArg, succRes); err != nil {
		vnode.fingerTable.with(func(self *fingerTableData) {
			self.invalidateKey(pred.ID)
		})
		// Retry
		return vnode.FindSuccessor(find, cfg)
	}
	succ = succRes.Successors
	ran = pred.ID.To(&succ.ID)
	for !find.In(&ran) {
		{
			args := FindClosestPredecessorArg{
				ID:   pred.ID,
				Find: find,
			}
			res := new(FindClosestPredecessorRes)
			err := call(pred.Addr, "Node.FindClosestPredecessor", &args, res)
			if err != nil {
				// TODO better error handling
				return nil, err
			}
			if res.Predecessor == nil {
				panic("res.Predecessor == nil")
			}
			pred = res.Predecessor
		}
		args := SuccessorArgs{
			ID: pred.ID,
		}
		res := new(SuccessorResult)
		err := call(pred.Addr, "Node.Successor", &args, res)
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
		ran = pred.ID.To(&succ.ID)
	}
	return succ, nil
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
	var succ *Finger
	vnode.fingerTable.with(func(self *fingerTableData) {
		succ = self.fingers[0]
	})
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
		vnode.fingerTable.with(func(self *fingerTableData) {
			self.invalidateKey(succ.ID)
		})
		return
	}
	if res.Predecessor != nil {
		vnode.fingerTable.with(func(self *fingerTableData) {
			self.append(res.Predecessor)
			succ = self.fingers[0]
		})
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

	var succ *Finger
	vnode.fingerTable.with(func(self *fingerTableData) {
		pick := uint(rand.Int()) % uint(self.successors.Len()+1)
		if pick == 0 {
			succ = self.fingers[0]
		} else {
			succ = self.successors[pick-1]
		}
	})
	arg := SuccessorArgs{
		ID: succ.ID,
	}
	res := new(SuccessorResult)
	if err := call(succ.Addr, "Node.Successor", &arg, res); err != nil {
		log.Warnf("Error trying to find successor of successor: %v", err)
		vnode.fingerTable.with(func(self *fingerTableData) {
			self.invalidateKey(succ.ID)
		})
		return
	} else {
		vnode.fingerTable.with(func(self *fingerTableData) {
			self.append(res.Successors)
		})
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
		vnode.fingerTable.with(func(self *fingerTableData) {
			self.append(new)
		})
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
	var pred *Finger
	vnode.fingerTable.with(func(self *fingerTableData) {
		pred = self.predecessor
	})
	if pred == nil {
		return
	}
	args := SuccessorArgs{
		ID: pred.ID,
	}
	res := new(SuccessorResult)
	if err := call(pred.Addr, "Node.Successor", &args, res); err != nil {
		log.Warnf("Predecessor failed: %v", err)
		vnode.fingerTable.with(func(self *fingerTableData) {
			if self.predecessor.ID.Cmp(&pred.ID) == 0 {
				self.predecessor = nil
			}
		})
	}
}
