package chord

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

// Key The key type of a node.

type Nodes []*VirtualNode

func (a Nodes) Len() int           { return len(a) }
func (a Nodes) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Nodes) Less(i, j int) bool { return a[i].nodeID.Less(&a[j].nodeID) }

type KeyUpdate struct {
	lost bool
	key  Key
}

// Chord main struct handling the protocol
type Chord struct {
	cfg        *Config
	node       *ChordNode
	keyUpdates *chan KeyUpdate
	server     *rpc.Server
	addr       *net.TCPAddr
}

func handleRequests(incomming *net.TCPListener, server *rpc.Server) {
	for {
		con, err := incomming.Accept()
		if err != nil {
			log.Error("Error acception connection: %v", err)
		}
		go server.ServeConn(con)
	}
}

func initializeLogger() {
	env := os.Getenv("CHORD_LOG")
	switch env {
	case "TRACE":
		log.SetLevel(log.TraceLevel)
	case "trace":
		log.SetLevel(log.TraceLevel)
	case "DEBUG":
		log.SetLevel(log.DebugLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "INFO":
		log.SetLevel(log.InfoLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "WARN":
		log.SetLevel(log.WarnLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "ERROR":
		log.SetLevel(log.ErrorLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "FATAL":
		log.SetLevel(log.FatalLevel)
	case "fatal":
		log.SetLevel(log.FatalLevel)
	case "PANIC":
		log.SetLevel(log.PanicLevel)
	case "panic":
		log.SetLevel(log.PanicLevel)
	}
}

// Run start the chord node
func Run(cfg *Config) (*Chord, error) {
	initializeLogger()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	log.WithFields(log.Fields{
		"host": cfg.host,
	}).Info("Initializing chord swarm")

	// Resolve the addres
	addr, err := net.ResolveTCPAddr("", cfg.host)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve host name: %v", err)
	}
	log.WithFields(log.Fields{
		"resolvedHost": addr,
	}).Trace("Resolved host")

	// Create all the nodes
	node := NewNode(cfg, addr)

	// Create the server
	socket, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create socket: %v", err)
	}

	server := rpc.NewServer()
	server.Register(node)

	// Create the chord object
	res := new(Chord)
	res.cfg = cfg
	res.node = node
	res.server = server
	res.keyUpdates = new(chan KeyUpdate)
	res.addr = addr

	// Initialize all the nodes
	if cfg.bootstrap != nil {
		bootstrap, err := net.ResolveTCPAddr("", *cfg.bootstrap)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve bootstrap addres: %v", err)
		}
		res.initializeWithBootstrap(bootstrap)
	} else {
		log.Info("No bootstrap address, initializing as first node in the network.")
		res.initializeNodes()
	}

	// Start the server
	handleRequests(socket, server)

	return res, nil
}

// Connect to a specific node returining a RPC client
func (chord *Chord) connect(to *FingerEntry) (*rpc.Client, error) {
	log.Trace("connecting to ", to.address.String())
	conn, err := net.DialTCP("tcp", nil, to.address)
	if err != nil {
		return nil, err
	}
	client := rpc.NewClient(conn)
	return client, nil
}

// Find the closest predecessor on a specific node
func (chord *Chord) findClosestPredecessor(on *FingerEntry, of Key) (*FingerEntry, error) {
	arg := FindPredecessorArgs{
		NodeID: on.nodeID,
		Find:   of,
	}
	result := new(FingerEntry)
	client, err := chord.connect(on)
	if err != nil {
		return nil, err
	}
	if err := client.Call("ChordNode.FindClosestPredecessor", &arg, result); err != nil {
		return nil, err
	}
	client.Close()
	return result, nil
}

// Returns the successor of a specific node
func (chord *Chord) successor(on *FingerEntry) (*FingerEntry, error) {
	args := Args{
		NodeID: on.nodeID,
	}
	result := new(FingerEntry)
	client, err := chord.connect(on)
	if err != nil {
		return nil, err
	}
	err = client.Call("ChordNode.Successor", &args, result)
	if err != nil {
		return nil, err
	}
	client.Close()
	return result, nil
}

// Returns the predecessor of a specific node
func (chord *Chord) predecessor(on *FingerEntry) (*FingerEntry, error) {
	args := Args{
		NodeID: on.nodeID,
	}
	result := new(FingerEntry)
	client, err := chord.connect(on)
	if err != nil {
		return nil, err
	}
	err = client.Call("ChordNode.Predecessor", &args, result)
	if err != nil {
		return nil, err
	}
	client.Close()
	return result, nil
}

// Find the successor of a specific key starting with a specific node
func (chord *Chord) FindSuccessor(on *FingerEntry, of Key) (*FingerEntry, error) {
	n := on
	successor, err := chord.successor(on)
	if err != nil {
		log.Debug("d test")
		return nil, fmt.Errorf("find successor: %v", err)
	}
	keyRange := n.nodeID.to(&successor.nodeID)

	for !of.in(&keyRange) {
		n, err = chord.findClosestPredecessor(n, of)
		if err != nil {
			log.Debug("c test")
			return nil, fmt.Errorf("find successor: %v", err)
		}
		successor, err = chord.successor(n)
		if err != nil {
			log.Debug("b test")
			return nil, fmt.Errorf("find successor: %v", err)
		}
		keyRange = n.nodeID.to(&successor.nodeID)
	}
	successor, err = chord.successor(n)
	if err != nil {
		log.Debug("a test")
		return nil, fmt.Errorf("find successor: %v", err)
	}
	return successor, nil
}

func (chord *Chord) initializeNodes() {
	nodes := chord.node.nodes
	log.Info("Initializing virtual nodes")
	for i := range nodes {
		cur := nodes[i]
		cur.lock.Lock()

		ran := cur.nodeID.to(&cur.nodeID)
		for j := 0; j < maxFingers; j++ {
			findKey := cur.nodeID.Next(uint(j))
			ran.to = findKey
			k := 0
			idx := (i + k) % nodes.Len()
			for nodes[idx].nodeID.in(&ran) {
				k++
				idx = (i + k) % nodes.Len()
			}
			cur.table.entries = append(cur.table.entries, &FingerEntry{
				nodeID:  nodes[idx].nodeID,
				address: chord.addr,
			})
		}
		log.WithFields(log.Fields{
			"virtualId": i,
		}).Trace("initialized virtual node")
		// Find the successor in the current nodes
		cur.lock.Unlock()
	}
	// Start the stabilization process
	for i := range nodes {
		go nodes[i].Stabilize(chord)
	}

}

func (chord *Chord) initializeWithBootstrap(bootstrap *net.TCPAddr) {
	nodes := chord.node.nodes
	key := CreateKey(bootstrap, 0)
	finger := new(FingerEntry)
	finger.address = bootstrap
	finger.nodeID = key

	for i := 0; i < nodes.Len(); i++ {
		go func() {
			for {
				successor, err := chord.FindSuccessor(finger, nodes[i].nodeID)
				if err != nil {
					log.Warn("Failed to bootstrap virtualnode ", i, err, " retrying...")
					time.Sleep(chord.cfg.stabilizeInterval)
					log.Trace("Retrying to bootstrap virtual node", i)
					continue
				}
				nodes[i].lock.Lock()
				nodes[i].setSuccessor(successor, 0)
				nodes[i].lock.Unlock()
				// Start the stabilization process
				go nodes[i].Stabilize(chord)
				break
			}
		}()
	}
}
