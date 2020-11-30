package chord

import (
	"fmt"
	"net"
	"sort"
)

type ChordNode struct {
	nodes Nodes
}

func NewNode(cfg *Config, addr *net.TCPAddr) *ChordNode {
	res := new(ChordNode)

	nodes := Nodes{}
	for i := uint32(0); i < cfg.numVirtualNodes; i++ {
		nodes = append(nodes, NewVirtualNode(cfg, i, addr))
	}
	sort.Sort(nodes)
	res.nodes = nodes

	return res
}

func (node *ChordNode) findVirtualNode(nodeID *Key) (*VirtualNode, error) {
	idx := sort.Search(len(node.nodes), func(i int) bool {
		return node.nodes[i].nodeID.LessEqual(nodeID)
	})
	if !node.nodes[idx].nodeID.Equal(nodeID) {
		return nil, fmt.Errorf("Could not find node with address at address")
	}
	return node.nodes[idx], nil
}

type Empty struct{}

type Args struct {
	nodeID Key
}

func (node *ChordNode) Predecessor(args *Args, res *FingerEntry) error {
	vNode, err := node.findVirtualNode(&args.nodeID)
	if err != nil {
		return err
	}
	vNode.lock.Lock()
	res = vNode.table.previous
	vNode.lock.Unlock()
	return nil
}

func (node *ChordNode) Successor(args *Args, res *FingerEntry) error {
	vNode, err := node.findVirtualNode(&args.nodeID)
	if err != nil {
		return err
	}
	vNode.lock.Lock()
	res = vNode.table.successors[0]
	vNode.lock.Unlock()
	return nil
}

type NotifyArgs struct {
	nodeID   Key
	previous FingerEntry
}

func (node *ChordNode) Notify(args *NotifyArgs, res *Empty) error {
	vNode, err := node.findVirtualNode(&args.nodeID)
	if err != nil {
		return err
	}
	vNode.lock.Lock()
	if vNode.table.previous == nil || (!vNode.table.previous.nodeID.LessEqual(&args.nodeID) && vNode.nodeID.Less(&args.nodeID)) {
		vNode.table.previous.address = args.previous.address
		vNode.table.previous.nodeID = args.previous.nodeID
	}
	vNode.lock.Unlock()
	return nil
}

// FindPredecessorArgs Arguments to the FindClosestPredecessor function
type FindPredecessorArgs struct {
	// The node to find the key on
	nodeID Key
	// The key to look for
	find Key
}

func (node *ChordNode) FindClosestPredecessor(args *FindPredecessorArgs, res *FingerEntry) error {
	vNode, err := node.findVirtualNode(&args.nodeID)
	if err != nil {
		return err
	}
	vNode.lock.Lock()
	res = vNode.ClosestPrecedingFinger(args.find)
	vNode.lock.Unlock()
	return nil
}
