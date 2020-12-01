package chord

import (
	"fmt"
)

// Config a struct containig the configuration for the chord network
type Config struct {
	bootstrap       *string
	numSuccessors   uint32
	numVirtualNodes uint32
	host            string
}

// ConfigBuilder struct for creating a config for the chord network
func ConfigBuilder() Config {
	return Config{
		bootstrap:       nil,
		host:            "localhost:0",
		numSuccessors:   3,
		numVirtualNodes: 5,
	}
}

// BootstrapAddr add a address for bootstraping the node into the network
func (cfg Config) BootstrapAddr(addr string) Config {
	cfg.bootstrap = &addr
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

func (cfg *Config) Validate() error {
	if cfg.numSuccessors == 0 {
		return fmt.Errorf("numSuccessors set to 0, should atleast be one")
	}
	if cfg.numVirtualNodes == 0 {
		return fmt.Errorf("numVirtualNodes set to 0, should atleast be two")
	}
	return nil
}
