package chord

import (
	"fmt"
	"net"
	"net/rpc"
	"sort"

	log "github.com/sirupsen/logrus"
)

type VNodes []*virtualNode

func (a VNodes) Len() int           { return len(a) }
func (a VNodes) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a VNodes) Less(i, j int) bool { return a[i].ID.Cmp(&a[j].ID) == -1 }

type Node struct {
	cfg   *Config
	nodes VNodes
}

func createNode(cfg *Config) (*Node, error) {
	nodes := VNodes{}
	for i := uint32(0); i < cfg.numVirtualNodes; i++ {
		nodes = append(nodes, CreateVirtualNode(cfg.host, i, cfg.bitsInKey, cfg.numSuccessors))
	}
	sort.Sort(nodes)
	if cfg.bootstrap != nil {
		// Use bootstrap node to find successors
		conn, err := net.DialTCP("tcp", nil, cfg.bootstrap)
		if err != nil {
			return nil, fmt.Errorf("Failed to connect to bootstrap node: %v", err)
		}
		client := rpc.NewClient(conn)
		id := CreateKey(cfg.bootstrap, 0, cfg.bitsInKey)
		for i := range nodes {
			arg := FindSuccessorArgs{
				ID:   id,
				Find: nodes[i].ID,
			}
			res := new(FindSuccessorRes)
			if err := client.Call("Node.FindSuccessor", &arg, res); err != nil {
				return nil, fmt.Errorf("Fialed to find successor for virtual node: %v", err)
			}
			if res.Successor == nil {
				panic("res.Predecessor == nil")
			}
			nodes[i].setSuccessor(0, res.Successor)
		}
	} else {
		// Node is the initial node of the network so initialize localy
		for i := range nodes {
			nodes[i].setSuccessor(0, &Finger{
				ID:   nodes[(i+1)%nodes.Len()].ID,
				Addr: cfg.host,
			})
		}
	}
	res := new(Node)
	res.cfg = cfg
	res.nodes = nodes
	return res, nil
}

// Find a virtual node with the given key
// returns nil if the node was not found
func (node *Node) findVirtualNode(id *Key) *virtualNode {
	idx := 0
	for idx = range node.nodes {
		if node.nodes[idx].ID.Cmp(id) == 0 {
			break
		}
	}
	if idx >= node.nodes.Len() {
		return nil
	} else {
		if node.nodes[idx].ID.Cmp(id) != 0 {
			return nil
		} else {
			return node.nodes[idx]
		}
	}
}

// Start all the stablization ticks
func (node *Node) run() {
	for i := range node.nodes {
		go node.nodes[i].fixFingersLoop(node.cfg)
		go node.nodes[i].stabilizeLoop(node.cfg)
		go node.nodes[i].checkPredecessorLoop(node.cfg)
		go node.nodes[i].maintainSuccessorsLoop(node.cfg)
	}
}

type PredecessorArgs struct {
	ID Key
}

type PredecessorResult struct {
	Predecessor *Finger
}

func (node *Node) Predecessor(args *PredecessorArgs, res *PredecessorResult) error {
	vnode := node.findVirtualNode(&args.ID)
	if vnode == nil {
		return fmt.Errorf("No such node [%v]", args.ID.Readable())
	}
	log.Tracef("[%v]RPC:Predecessor", vnode.ID.Readable())
	vnode.lock.Lock()
	res.Predecessor = vnode.predecessor
	vnode.lock.Unlock()
	return nil
}

type SuccessorArgs struct {
	ID Key
}

type SuccessorResult struct {
	Successors *Finger
}

func (node *Node) Successor(args *SuccessorArgs, res *SuccessorResult) error {
	vnode := node.findVirtualNode(&args.ID)
	if vnode == nil {
		return fmt.Errorf("No such node [%v]", args.ID.Readable())
	}
	log.Tracef("[%v]RPC:Successor", vnode.ID.Readable())
	vnode.lock.Lock()
	res.Successors = vnode.getSuccessor(0)
	vnode.lock.Unlock()
	return nil
}

type FindClosestPredecessorArg struct {
	ID   Key
	Find Key
}

type FindClosestPredecessorRes struct {
	Predecessor *Finger
}

func (node *Node) FindClosestPredecessor(args *FindClosestPredecessorArg, res *FindClosestPredecessorRes) error {
	vnode := node.findVirtualNode(&args.ID)
	if vnode == nil {
		return fmt.Errorf("No such node [%v]", args.ID.Readable())
	}
	log.Tracef("[%v]RPC:FindClosestPredecessor", vnode.ID.Readable())
	vnode.lock.Lock()
	res.Predecessor = vnode.findClosestPredecessor(args.Find, node.cfg.host)
	vnode.lock.Unlock()
	return nil
}

type NotifyArgs struct {
	ID     Key
	Notify *Finger
}

type NotifyRes struct{}

func (node *Node) Notify(args *NotifyArgs, res *NotifyRes) error {
	vnode := node.findVirtualNode(&args.ID)
	if vnode == nil {
		return fmt.Errorf("No such node [%v]", args.ID.Readable())
	}
	log.Tracef("[%v]RPC:Notify", vnode.ID.Readable())
	vnode.lock.Lock()
	vnode.notify(args.Notify)
	vnode.lock.Unlock()
	return nil
}

type FindSuccessorArgs struct {
	ID   Key
	Find Key
}

type FindSuccessorRes struct {
	Successor *Finger
}

func (node *Node) FindSuccessor(args *FindSuccessorArgs, res *FindSuccessorRes) error {
	vnode := node.findVirtualNode(&args.ID)
	if vnode == nil {
		return fmt.Errorf("No such node [%v]", args.ID.Readable())
	}
	log.Tracef("[%v]RPC:FindSuccessor", vnode.ID.Readable())
	// Should not lock
	result, err := vnode.FindSuccessor(args.Find, node.cfg)
	if err != nil {
		return err
	}
	res.Successor = result
	return nil
}
