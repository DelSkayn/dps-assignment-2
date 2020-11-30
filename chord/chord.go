package chord

import (
	"fmt"
	"net"
	"sort"

	log "github.com/sirupsen/logrus"
)

// Key The key type of a node.
type Key [32]byte

func Less(a, b Key) bool {
	for i := 0; i < 32; i++ {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	return false
}

type Nodes []*Node

func (a Nodes) Len() int           { return len(a) }
func (a Nodes) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Nodes) Less(i, j int) bool { return Less(a[i].nodeID, a[j].nodeID) }

type KeyUpdate struct {
	lost bool
	key  Key
}

// Chord main struct handling the protocol
type Chord struct {
	nodes      Nodes
	keyUpdates *chan KeyUpdate
}

// Run start the chord node
func Run(cfg Config) (*Chord, error) {
	log.Info("Initializing chord swarm on: ", cfg.host)
	addr, err := net.ResolveTCPAddr("", cfg.host)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve host name: %v", err)
	}
	log.Trace("Resolved host to: ", addr)

	nodes := Nodes{}
	for i := uint32(0); i < cfg.numVirtualNodes; i++ {
		nodes = append(nodes, NewNode(&cfg, i, addr))
	}

	sort.Sort(nodes)

	return nil, nil
}

func (chord *Chord) Resolve(key Key) *FingerEntry {
	pred := sort.Search(len(chord.nodes), func(i int) bool {
		Less(chord.nodes[i].nodeID, key)
	})
	return chord.nodes[pred].Resolve(key)
}

func (chord *Chord) Dial(nodeID Key, addr net.Addr) chan
