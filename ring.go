/*
*	Copyright © 2017 Joe Xu <joe.0x01@gmail.com>
*	This work is free. You can redistribute it and/or modify it under the
*	terms of the Do What The Fuck You Want To Public License, Version 2,
*	as published by Sam Hocevar. See the COPYING file for more details.
*
 */

//Package chord implements chord protocol
//https://pdos.csail.mit.edu/6.824/papers/stoica-chord.pdf
package chord

import (
	"github.com/Joe-xu/logger"
	"google.golang.org/grpc"
)

//Ring is prototype of chord ring
type Ring struct {
	Config *Config

	introducer *NodeInfo //could be arbitrary node in one ring
	conn       *grpc.ClientConn
}

//JoinRing join existing ring
func JoinRing(config *Config) (*Ring, error) {

	r := &Ring{
		Config:     config,
		introducer: config.Introducer,
	}

	conn, err := r.introducer.dial()
	if err != nil {
		return nil, err
	}
	r.conn = conn

	return r, nil
}

//Locate returns the node-info where key is store
func (r *Ring) Locate(key string) (*NodeInfo, error) {

	target := &NodeInfo{
		ID: r.Config.HashMethod.Sum([]byte(key)),
	}
	target.ID = mod2(target.ID, len(target.ID)*8)

	// target.ID = []byte(key) //DEBUG
	logger.Debug.Printf("Locate:% x", target.ID)

	return findSuccessorRPC(r.conn, target)
}

//Leave the ring
func (r *Ring) Leave() {
	r.conn.Close()
}
