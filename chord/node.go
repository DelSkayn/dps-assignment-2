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
		next := node.nodes[(i+1)%len]
		cur.lock.Lock()
		cur.table.entries = append(cur.table.entries, &FingerEntry{
			address: next.addr,
			nodeID:  next.nodeID,
		})

		ran := cur.nodeID.to(&next.nodeID)

		for i := uint64(0); i < maxFingers; i++ {
			nextKey := cur.nodeID.Next(uint(i))
			if !nextKey.in(&ran) {
				idx := sort.Search(node.nodes.Len(), func(j int) bool {
					searchIdx := (uint64(j) + i) % len
					return node.nodes[searchIdx].nodeID.in(&ran)
				})
				idx = (idx + 1) % node.nodes.Len()
				cur.table.entries = append(cur.table.entries, &FingerEntry{
					address: addr,
					nodeID:  node.nodes[idx].nodeID,
				})
				ran.to = node.nodes[idx].nodeID
			}
		}
		cur.table.successors = append(cur.table.successors, cur.table.entries[0])
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
	finger.address = bootstrap
	finger.nodeID = key
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
	node.nodes[i].SetSuccessor(successor, 0)
	node.nodes[i].lock.Unlock()
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
