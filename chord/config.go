package chord

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

// Config a struct containig the configuration for the chord network
type Config struct {
	bootstrap         *string
	numSuccessors     uint32
	numVirtualNodes   uint32
	host              string
	stabilizeInterval time.Duration
}

// ConfigBuilder struct for creating a config for the chord network
func ConfigBuilder() *Config {
	duration, err := time.ParseDuration("10s")
	if err != nil {
		log.Panic(err)
	}
	return &Config{
		bootstrap:         nil,
		host:              "localhost:8080",
		numSuccessors:     3,
		numVirtualNodes:   5,
		stabilizeInterval: duration,
	}
}

// BootstrapAddr add a address for bootstraping the node into the network
func (cfg *Config) BootstrapAddr(addr string) *Config {
	cfg.bootstrap = &addr
	return cfg
}

// NumSuccessors set the amount of successor nodes maintained by a single node
func (cfg *Config) NumSuccessors(numSuccessors uint32) *Config {
	cfg.numSuccessors = numSuccessors
	return cfg
}

// Host change the host address
func (cfg *Config) Host(host string) *Config {
	cfg.host = host
	return cfg
}

// Host change the host address
func (cfg *Config) StabilizeInterval(interval string) (*Config, error) {
	duration, err := time.ParseDuration(interval)
	if err != nil {
		return nil, err
	}
	cfg.stabilizeInterval = duration
	return cfg, nil
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
