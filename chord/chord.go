package chord

import (
	"fmt"
	"net"
	"net/rpc"
	"os"

	log "github.com/sirupsen/logrus"
)

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

type Chord struct {
	cfg    *Config
	node   *Node
	server *net.TCPListener
	rpc    *rpc.Server
}

func (cfg *Config) CreateChord() (*Chord, error) {
	initializeLogger()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("Invalid configuration: %v", err)
	}

	server, err := net.ListenTCP("tcp", cfg.host)
	if err != nil {
		return nil, fmt.Errorf("Failed to create server: %v", err)
	}

	node, err := createNode(cfg)
	if err != nil {
		return nil, fmt.Errorf("Failed to initialize node: %v", err)
	}

	rpc := rpc.NewServer()
	err = rpc.Register(node)
	if err != nil {
		return nil, fmt.Errorf("Failed to register RPC function %v", err)
	}

	res := new(Chord)
	res.cfg = cfg
	res.node = node
	res.server = server
	res.rpc = rpc

	return res, nil
}

/// Run start running the node
func (chord *Chord) Run() {
	chord.node.run()
	chord.rpc.Accept(chord.server)
}
