package chord

import (
	"hash"
	"net"
	"time"
)

const (
	defaultRPCTimeout         = 5 * time.Second
	defaultStabilizeInterval  = 5 * time.Second
	defaultFixFingersInterval = 5 * time.Second
)

var (
	rpcTimeout = defaultRPCTimeout
	// stabilizeInterval  = defaultStabilizeInterval
	// fixFingersInterval = defaultFixFingersInterval
)

//Config for ring
type Config struct {
	Timeout            time.Duration //rpc timeout
	HashMethod         hash.Hash     //to generate the ID
	Introducer         *NodeInfo     ///could be arbitrary node in one ring
	Listener           net.Listener  //Listener for rpc server
	StabilizeInterval  time.Duration //periodically excute Stabilize
	FixFingersInterval time.Duration //periodically excute FixFingers
}
