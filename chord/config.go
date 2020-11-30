package chord

import "math"

// Config a struct containig the configuration for the chord network
type Config struct {
	bootstrap       []string
	maxPeers        uint64
	numSuccessors   uint32
	numVirtualNodes uint32
	host            string
}

// ConfigBuilder struct for creating a config for the chord network
func ConfigBuilder() Config {
	return Config{
		bootstrap:       nil,
		maxPeers:        math.MaxUint64,
		host:            "localhost:0",
		numSuccessors:   3,
		numVirtualNodes: 5,
	}
}

// BootstrapAddr add a address for bootstraping the node into the network
func (cfg Config) BootstrapAddr(addr string) Config {
	cfg.bootstrap = append(cfg.bootstrap, addr)
	return cfg
}

// MaxPeers set the maximum amount of peers a node can have
func (cfg Config) MaxPeers(maxPeers uint64) Config {
	cfg.maxPeers = maxPeers
	return cfg
}

// NumSuccessors set the amount of successor nodes maintained by a single node
func (cfg Config) NumSuccessors(numSuccessors uint32) Config {
	cfg.numSuccessors = numSuccessors
	return cfg
}

// Host change the host address
func (cfg Config) Host(host string) Config {
	cfg.host = host
	return cfg
}
