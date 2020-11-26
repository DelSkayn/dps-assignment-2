package chord

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"fmt"
	"math"
	"math/bits"
	"net"
)

type Config struct {
	bootstrap []string
	maxPeers  uint64
	host      string
}

func ConfigBuilder() Config {
	return Config{
		bootstrap: nil,
		maxPeers:  math.MaxUint64,
		host:      "localhost:8080",
	}
}

func (cfg *Config) BootstrapAddr(addr string) *Config {
	cfg.bootstrap = append(cfg.bootstrap, addr)
	return cfg
}

func (cfg *Config) MaxPeers(maxPeers uint64) *Config {
	cfg.maxPeers = maxPeers
	return cfg
}

func (cfg *Config) Host(host string) *Config {
	cfg.host = host
	return cfg
}

type FingerEntry struct {
	nodeId  uint64
	address net.Addr
}

type FingerTable struct {
	entries    []*FingerEntry
	prev       FingerEntry
	successors []*FingerEntry
}

type Node struct {
	table FingerTable
}

func hashAddr(addr net.IPAddr) uint64 {
	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(addr)
	hasher := sha1.New()
	hasher.Write(b.Bytes())
	hasher.Sum64
}


// Run start the chord node
func Run(cfg Config) error {
	addr, err := net.ResolveIPAddr("tcp", cfg.host)
	if err != nil {
		fmt.Println("failed to resolve hostname:" + cfg.host)
		return fmt.Errorf("failed to resolve hostname", err)
	}

	numFingers := bits.Len64(cfg.maxPeers)
	id := hashAddr(addr)

	var fingerTable
	if len(cfg.bootstrap) == 0 {
		for _, finger := range numFin {
			fingerTable = append(fingerTable,FingerEntry{
				nodeId: id,
				address: addr
			})
		}
	}else{
		for _, finger := range numFin {
			fingerTable = append(nil)
		}

	}

	ln, err := net.ListenIP("tcp", addr)
	if err != nil {
		return err
	}
	for {
		conn, err := ln.Accept()
		fmt.Println("Incomming connection!")
		if err != nil {
			return err
		}
		go handleIncomming(conn)
	}
}
