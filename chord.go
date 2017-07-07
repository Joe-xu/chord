/*
*	Copyright © 2017 Joe Xu <joe.0x01@gmail.com>
*	This work is free. You can redistribute it and/or modify it under the
*	terms of the Do What The Fuck You Want To Public License, Version 2,
*	as published by Sam Hocevar. See the COPYING file for more details.
*
 */

//go:generate protoc --go_out=plugins=grpc:. chord.proto

//Package chord implements chord protocol
//https://pdos.csail.mit.edu/6.824/papers/stoica-chord.pdf
package chord

import (
	"crypto/md5"
	"fmt"
	"hash"
	"net"
	"strings"

	google_protobuf "github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

//Node prototype on the chord ring
type Node struct {
	IP   string
	Port string

	ID         []byte
	Value      interface{}
	hashMethod hash.Hash
	m          int //length of ID in bit

	fingers     fingerTable //fingers[0].node==successor[0]
	successor   []*Node
	predecessor *Node
}

//NewNode init and return new node
func NewNode(port string) *Node {
	n := &Node{
		Port:       port,
		successor:  make([]*Node, 0),
		hashMethod: md5.New(),
	}

	n.ID = n.hashMethod.Sum([]byte(n.IP + n.Port))
	n.m = len(n.ID) * 8

	return n
}

//Serve makes node listen on the port of the spec network type
func (n *Node) Serve(network string, port string) error {

	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	listener, err := net.Listen(network, port)
	if err != nil {
		return err
	}

	rpcServer := grpc.NewServer()
	RegisterNodeServer(rpcServer, n)
	reflection.Register(rpcServer)
	err = rpcServer.Serve(listener)

	return err
}

//Join let n join the network,accept arbitrary node's info as parma
func (n *Node) Join(info ...*NodeInfo) error {

	if len(info) >= 1 { //TODO:handle mutiple join

		conn, err := grpc.Dial(fmt.Sprintf("%s:%s", info[0].IP, info[0].Port), grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()

		rpcClient := NewNodeClient(conn)
		_, err = rpcClient.OnJoin(context.Background(), info[0])

		return err
	}
	// only n in the network
	n.fingers = make([]*finger, n.m)
	for i := range n.fingers {
		n.fingers[i].node = n
	}
	n.predecessor = n

	return nil

}

//force interface check
// var _ NodeServer = (*Node)(nil)

//OnJoin implements NodeServer interface
func (n *Node) OnJoin(ctx context.Context, info *NodeInfo) (*google_protobuf.Empty, error) {

	newNode := &Node{
		IP:   info.IP,
		Port: info.Port,
	}

	n.successor = append(n.successor, newNode)

	n.predecessor = newNode

	fmt.Printf("%#v\n", n)

	return &google_protobuf.Empty{}, nil
}

//OnNotify implements NodeServer interface
func (n *Node) OnNotify(ctx context.Context, info *NodeInfo) (*google_protobuf.Empty, error) {

	return &google_protobuf.Empty{}, nil
}
