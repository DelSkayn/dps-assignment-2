package chord

import (
	"fmt"
	"net"
	"time"

	log "github.com/sirupsen/logrus"
)

// Config a struct containig the configuration for the chord network
type Config struct {
	bootstrap         *net.TCPAddr
	numSuccessors     uint32
	numVirtualNodes   uint32
	bitsInKey         uint32
	host              *net.TCPAddr
	stabilizeInterval time.Duration
}

// ConfigBuilder struct for creating a config for the chord network
func ConfigBuilder() *Config {
	duration, err := time.ParseDuration("2s")
	if err != nil {
		log.Panic(err)
	}
	host, err2 := net.ResolveTCPAddr("tcp", "localhost:8080")
	if err2 != nil {
		log.Panic(err2)
	}
	return &Config{
		bootstrap:         nil,
		host:              host,
		numSuccessors:     3,
		bitsInKey:         32,
		numVirtualNodes:   4,
		stabilizeInterval: duration,
	}
}

// BootstrapAddr add a address for bootstraping the node into the network
func (cfg *Config) BootstrapAddr(addr string) (*Config, error) {
	resolveAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	cfg.bootstrap = resolveAddr
	return cfg, nil
}

// NumSuccessors set the amount of successor nodes maintained by a single node
func (cfg *Config) NumSuccessors(numSuccessors uint32) *Config {
	cfg.numSuccessors = numSuccessors
	return cfg
}

// Host change the host address
func (cfg *Config) Host(host string) (*Config, error) {
	resolveAddr, err := net.ResolveTCPAddr("tcp", host)
	if err != nil {
		return nil, err
	}
	cfg.host = resolveAddr
	return cfg, nil
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

func (cfg *Config) BitsInKey(bits uint32) *Config {
	cfg.bitsInKey = bits
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
