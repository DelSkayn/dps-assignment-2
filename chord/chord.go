package chord

import (
	"fmt"
	"net"
	"net/rpc"

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

// Run start the chord node
func Run(cfg Config) (*Chord, error) {
	log.Info("Initializing chord swarm on: ", cfg.host)

	addr, err := net.ResolveTCPAddr("", cfg.host)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve host name: %v", err)
	}
	log.Trace("Resolved host to: ", addr)

	node := NewNode(&cfg, addr)

	socket, err := net.ListenTCP("", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create socket: %v", err)
	}

	server := rpc.NewServer()
	server.Register(node)

	go handleRequests(socket, server)

	res := new(Chord)
	res.node = node
	res.server = server
	res.keyUpdates = new(chan KeyUpdate)
	res.addr = addr

	return res, nil
}

func (chord *Chord) connect(to *FingerEntry) (*rpc.Client, error) {
	conn, err := net.DialTCP("", nil, to.address)
	if err != nil {
		return nil, err
	}
	client := rpc.NewClient(conn)
	return client, nil
}

func (chord *Chord) findClosestPredecessor(on *FingerEntry, of Key) (*FingerEntry, error) {
	arg := FindPredecessorArgs{
		nodeID: on.nodeID,
		find:   of,
	}
	result := new(FingerEntry)
	client, err := chord.connect(on)
	if err != nil {
		return nil, err
	}
	if err := client.Call("findClosestPredecessor", &arg, result); err != nil {
		return nil, err
	}
	client.Close()
	return result, nil
}

func (chord *Chord) successor(on *FingerEntry) (*FingerEntry, error) {
	args := Args{
		nodeID: on.nodeID,
	}
	result := new(FingerEntry)
	client, err := chord.connect(on)
	if err != nil {
		return nil, err
	}
	err = client.Call("Successor", &args, result)
	if err != nil {
		return nil, err
	}
	client.Close()
	return result, nil
}

func (chord *Chord) predecessor(on *FingerEntry) (*FingerEntry, error) {
	args := Args{
		nodeID: on.nodeID,
	}
	result := new(FingerEntry)
	client, err := chord.connect(on)
	if err != nil {
		return nil, err
	}
	err = client.Call("Predecessor", &args, result)
	if err != nil {
		return nil, err
	}
	client.Close()
	return result, nil
}

func (chord *Chord) FindSuccessor(on *FingerEntry, of Key) (*FingerEntry, error) {
	n := on
	successor, err := chord.successor(on)
	if err != nil {
		return nil, err
	}
	keyRange := n.nodeID.to(&successor.nodeID)

	for !of.in(&keyRange) {
		n, err = chord.findClosestPredecessor(n, of)
		if err != nil {
			return nil, err
		}
		successor, err = chord.successor(n)
		if err != nil {
			return nil, err
		}
		keyRange = n.nodeID.to(&successor.nodeID)
	}
	successor, err = chord.successor(n)
	if err != nil {
		return nil, err
	}
	return successor, nil
}
