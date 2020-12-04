package chord

import (
	"fmt"
	"net"
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
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

// Initialize the virtual nodes as a single local network.
func (node *ChordNode) initialize(addr *net.TCPAddr) {
	log.Info("Initializing virtual nodes")
	len := uint64(node.nodes.Len())
	for i := uint64(0); i < len; i++ {
		cur := node.nodes[i]
		cur.lock.Lock()
		ran := cur.nodeID.to(&cur.nodeID)
		for j := uint64(0); j < maxFingers; j++ {
			findKey := cur.nodeID.Next(uint(j))
			ran.to = findKey
			k := uint64(0)
			idx := (i + k) % uint64(node.nodes.Len())
			for node.nodes[idx].nodeID.in(&ran) {
				k++
				idx = (i + k) % uint64(node.nodes.Len())
			}
			cur.table.entries = append(cur.table.entries, &FingerEntry{
				NodeID:  node.nodes[idx].nodeID,
				Address: addr,
			})
		}
		log.WithFields(log.Fields{
			"entries":   fmt.Sprintf("%+v", cur.table),
			"virtualId": i,
		}).Debug("initialized virtual node")
		// Find the successor in the current nodes
		cur.lock.Unlock()
	}
}

func (node *ChordNode) bootstrap(chord *Chord, bootstrap *net.TCPAddr) {
	key := CreateKey(bootstrap, 0)
	finger := new(FingerEntry)
	finger.Address = bootstrap
	finger.NodeID = key
	for i := 0; i < len(node.nodes); i++ {
		node.bootstrapSuccessor(chord, finger, i)
	}
}

func (node *ChordNode) bootstrapSuccessor(chord *Chord, finger *FingerEntry, i int) {
	successor, err := chord.FindSuccessor(finger, node.nodes[i].nodeID)
	if err != nil {
		log.Warn("Failed to bootstrap virtualnode %v: %v", i, err)
		go (func() {
			time.Sleep(100 * time.Millisecond)
			log.Info("Retrying to bootstrap virtual node %v", i)
			node.bootstrapSuccessor(chord, finger, i)
		})()
	}
	node.nodes[i].lock.Lock()
	node.nodes[i].setSuccessor(successor, 0)
	node.nodes[i].lock.Unlock()
}

func (node *ChordNode) findVirtualNode(nodeID *Key) (*VirtualNode, error) {
	nodes := node.nodes
	for i := range nodes {
		if nodes[i].nodeID.Equal(nodeID) {
			return nodes[i], nil
		}
	}
	return nil, fmt.Errorf("Failed to find node")
}

type Empty struct{}

type Args struct {
	NodeID Key
}

type PredecessorResult struct {
	Predecessor *FingerEntry
}

func (node *ChordNode) Predecessor(args *Args, res *PredecessorResult) error {
	vNode, err := node.findVirtualNode(&args.NodeID)
	if err != nil {
		return err
	}
	vNode.lock.Lock()
	res.Predecessor = vNode.table.previous
	vNode.lock.Unlock()
	return nil
}

func (node *ChordNode) Successor(args *Args, res *FingerEntry) error {
	vNode, err := node.findVirtualNode(&args.NodeID)
	if err != nil {
		return err
	}
	vNode.lock.Lock()
	tmp := vNode.getSuccessor(0)
	res.Address = tmp.Address
	res.NodeID = tmp.NodeID
	vNode.lock.Unlock()
	return nil
}

type NotifyArgs struct {
	NodeID   Key
	Previous *FingerEntry
}

func (node *ChordNode) Notify(args *NotifyArgs, res *Empty) error {
	vNode, err := node.findVirtualNode(&args.NodeID)
	if err != nil {
		return err
	}
	vNode.lock.Lock()
	if vNode.table.previous == nil || (!vNode.table.previous.NodeID.LessEqual(&args.NodeID) && vNode.nodeID.Less(&args.NodeID)) {
		vNode.table.previous = args.Previous
	}
	vNode.lock.Unlock()
	return nil
}

// FindPredecessorArgs Arguments to the FindClosestPredecessor function
type FindPredecessorArgs struct {
	// The node to find the key on
	NodeID Key
	// The key to look for
	Find Key
}

func (node *ChordNode) FindClosestPredecessor(args *FindPredecessorArgs, res *FingerEntry) error {
	vNode, err := node.findVirtualNode(&args.NodeID)
	if err != nil {
		return err
	}
	vNode.lock.Lock()
	tmp := vNode.ClosestPrecedingFinger(args.Find)
	res.Address = tmp.Address
	res.NodeID = tmp.NodeID
	vNode.lock.Unlock()
	return nil
}
