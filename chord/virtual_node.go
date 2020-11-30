package chord

import (
	"net"
	"sync"

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
func (a FingerEntries) Less(i, j int) bool { return a[i].nodeID < a[j].nodeID }

// FingerTable The finger table containing the addresses of other nodes
type FingerTable struct {
	entries    FingerEntries
	previous   *FingerEntry
	successors []*FingerEntry
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
	log.Trace("Starting virtual node %v with id %x.", virtualID, nodeID)

	entries := []*FingerEntry{}
	if len(cfg.bootstrap) == 0 {
		entries = append(entries, &FingerEntry{
			nodeID:  nodeID,
			address: addr,
		})
	}

	res := new(VirtualNode)
	res.table = FingerTable{
		entries:    entries,
		previous:   nil,
		successors: nil,
	}
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
