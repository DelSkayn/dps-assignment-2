package chord

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"net"
	"sort"

	log "github.com/sirupsen/logrus"
)

// FingerEntry An entrie in the finger table
type FingerEntry struct {
	// The hash of the node id
	nodeID [32]byte
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
type Node struct {
	table FingerTable
	// The hash of the current nodes id
	nodeID Key
	// The virtual Id of the node
	virtualID uint32
	cfg       *Config
}

func createID(virtualID uint32, addr *net.TCPAddr) Key {
	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(addr)
	b.WriteByte(byte(0xff & virtualID))
	b.WriteByte(byte(0xff & (virtualID >> 8)))
	b.WriteByte(byte(0xff & (virtualID >> 16)))
	b.WriteByte(byte(0xff & (virtualID >> 24)))
	hasher := sha256.New()
	hasher.Write(b.Bytes())
	res := hasher.Sum(nil)
	if len(res) != 32 {
		log.Panic("invalid size hash")
	}
	buf := [32]byte{}
	copy(buf[:], res)
	return buf
}

// NewNode create a new virtual node
func NewNode(cfg *Config, virtualID uint32, addr *net.TCPAddr) *Node {
	nodeID := createID(virtualID, addr)
	log.Trace("Starting virtual node %v with id %x.", virtualID, nodeID)

	entries := {}
	if len(cfg.bootstrap) == 0 {
		entries = append(entries, &FingerEntry{
			nodeID:  nodeID,
			address: addr,
		})
	}

	res := new(Node)
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

func (node *Node) Resolve(key Key) *FingerEntry,error {
	log.Panic("todo")
	predecessor := sort.Search(len(node.table.entries), func(i int) bool{
		Less(node.table.entries[i].nodeID,key)
	})
	return nil,nil
}
