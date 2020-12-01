package chord

import (
	"math/rand"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// FingerEntry An entrie in the finger table
type FingerEntry struct {
	// The hash of the node id
	nodeID Key
	// The address of the node
	address *net.TCPAddr
}

type FingerEntries []*FingerEntry

func (a FingerEntries) Len() int           { return len(a) }
func (a FingerEntries) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a FingerEntries) Less(i, j int) bool { return a[i].nodeID.Less(&a[j].nodeID) }

// FingerTable The finger table containing the addresses of other nodes
type FingerTable struct {
	entries      FingerEntries
	previous     *FingerEntry
	additionSucc []*FingerEntry
}

// Node A single virtual node
type VirtualNode struct {
	// The hash of the current nodes id
	nodeID Key
	addr   *net.TCPAddr
	// The virtual Id of the node
	virtualID uint32
	cfg       *Config

	lock  sync.Mutex
	table FingerTable
}

// NewNode create a new virtual node
func NewVirtualNode(cfg *Config, virtualID uint32, addr *net.TCPAddr) *VirtualNode {
	nodeID := CreateKey(addr, virtualID)
	log.WithFields(log.Fields{
		"virtualId": virtualID,
		"id":        nodeID.Inner.String(),
	}).Trace("Starting virtual node")

	res := new(VirtualNode)
	res.table = FingerTable{
		entries:      nil,
		previous:     nil,
		additionSucc: nil,
	}
	res.addr = addr
	res.nodeID = nodeID
	res.virtualID = virtualID
	res.cfg = cfg
	return res
}

// ClosestPrecedingFinger returns the closest preceding finger according to chord paper
func (vnode *VirtualNode) ClosestPrecedingFinger(nodeID Key) *FingerEntry {
	ran := vnode.nodeID.to(&nodeID)
	for i := len(vnode.table.entries) - 1; i > 0; i++ {
		if vnode.table.entries[i].nodeID.in(&ran) {
			return vnode.table.entries[i]
		}
	}
	res := new(FingerEntry)
	res.nodeID = vnode.nodeID
	res.address = vnode.addr
	return res
}

func (vnode *VirtualNode) setSuccessor(entry *FingerEntry, i int) {
	if i == 0 {
		vnode.table.entries[0] = entry
	} else {
		vnode.table.additionSucc[i-1] = entry
	}
}

func (vnode *VirtualNode) getSuccessor(i int) *FingerEntry {
	if i == 0 {
		return vnode.table.entries[0]
	} else {
		return vnode.table.additionSucc[i-1]
	}
}

func (vnode *VirtualNode) Stabilize(chord *Chord) {
	// TODO add random fluctation to sleep time to spread load over time
	go (func() {
		for {
			vnode.lock.Lock()
			succ := vnode.getSuccessor(0)
			vnode.lock.Unlock()

			pred, err := chord.predecessor(succ)
			if err != nil {
				log.Warn("Failed to retrieve predecessor of successor: ", err)
				time.Sleep(chord.cfg.stabilizeInterval)
				continue
				//TODO
			}
			ran := vnode.nodeID.to(&succ.nodeID)
			if pred.nodeID.in(&ran) {
				vnode.lock.Lock()
				vnode.setSuccessor(pred, 0)
				vnode.lock.Unlock()
			}
			time.Sleep(chord.cfg.stabilizeInterval)
		}
	})()
	go (func() {
		for {
			random := rand.Uint64() % uint64(maxFingers)
			toFind := vnode.nodeID.Next(uint(random))
			successor, err := chord.FindSuccessor(&FingerEntry{
				address: vnode.addr,
				nodeID:  vnode.nodeID,
			}, toFind)
			if err != nil {
				log.WithFields(log.Fields{
					"error": err,
				}).Warn("Failed to find successor")
				time.Sleep(chord.cfg.stabilizeInterval)
				continue
			}
			vnode.lock.Lock()
			vnode.table.entries[random] = successor
			vnode.lock.Unlock()
			time.Sleep(chord.cfg.stabilizeInterval)
		}
	})()
}
