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

//Ring is prototype of chord ring
type Ring struct {
	Introducer *NodeInfo //could be arbitrary node in one ring
	Config     *Config
}

//JoinRing join existing ring
func JoinRing(config *Config) *Ring {

	return &Ring{
		Introducer: config.Introducer,
		Config:     config,
	}
}

//Locate returns the node-info where key is store
func (r *Ring) Locate(key string) (*NodeInfo, error) {

	conn, err := r.Introducer.dial()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	target := &NodeInfo{
		ID: r.Config.HashMethod.Sum([]byte(key)),
	}
	target.ID = mod2(target.ID, len(target.ID)*8)

	return findSuccessorRPC(conn, target)
}
