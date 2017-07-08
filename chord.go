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
	successor   *Node
	predecessor *Node
}

//NewNode init and return new node
func NewNode(port string) *Node {
	n := &Node{
		Port:       port,
		hashMethod: md5.New(),
	}

	n.ID = n.hashMethod.Sum([]byte(n.IP + n.Port))
	n.m = len(n.ID) * 8

	return n
}

//Info return node's info used in rpc
func (n *Node) Info() *NodeInfo {
	info := &NodeInfo{
		ID:   n.ID,
		IP:   n.IP,
		Port: n.Port,
		// Successor:n.successor.Info(),//
	}

	if n.successor != nil {
		info.Successor = n.successor.Info()
	}

	return info
}

//isGreaterThan tell if n'ID greater
func (n *Node) isGreaterThan(id []byte) bool {
	return compareID(n.ID, id)
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

//OnJoin implements NodeServer interface [rpc]
func (n *Node) OnJoin(ctx context.Context, info *NodeInfo) (*google_protobuf.Empty, error) {

	newNode := &Node{
		IP:   info.IP,
		Port: info.Port,
	}

	n.successor = newNode

	n.predecessor = newNode

	fmt.Printf("%#v\n", n)

	return &google_protobuf.Empty{}, nil
}

//OnNotify implements NodeServer interface [rpc]
func (n *Node) OnNotify(ctx context.Context, info *NodeInfo) (*google_protobuf.Empty, error) {

	return &google_protobuf.Empty{}, nil
}

//FindSuccessor implements NodeServer interface [rpc]
func (n *Node) FindSuccessor(ctx context.Context, info *NodeInfo) (*NodeInfo, error) {

	return n.findSuccessor(info)
}

// findSuccessor  [local invoke]
func (n *Node) findSuccessor(info *NodeInfo) (*NodeInfo, error) {

	node, err := n.findPredecessor(info)

	return node.Successor, err
}

//FindPredecessor implements NodeServer interface [rpc]
func (n *Node) FindPredecessor(ctx context.Context, info *NodeInfo) (*NodeInfo, error) {

	return n.findPredecessor(info)
}

//findPredecessor [local invoke]
func (n *Node) findPredecessor(info *NodeInfo) (*NodeInfo, error) {

	nodeTmp := n

	// for info.ID > nodeTmp.ID && info.ID <= nodeTmp.successor.ID {
	for !nodeTmp.isGreaterThan(info.ID) && nodeTmp.successor.isGreaterThan(info.ID) {

		conn, err := grpc.Dial(fmt.Sprintf("%s:%s", nodeTmp.IP, nodeTmp.Port), grpc.WithInsecure())
		if err != nil {
			return nil, err
		}

		cli := NewNodeClient(conn)
		resp, err := cli.ClosestPrecedingFinger(context.Background(), &NodeInfo{
			ID: info.ID,
		})
		conn.Close()
		if err != nil {
			return nil, err
		}

		nodeTmp = &Node{
			ID:   resp.ID,
			IP:   resp.IP,
			Port: resp.Port,
			successor: &Node{
				ID:   resp.Successor.ID,
				IP:   resp.Successor.IP,
				Port: resp.Successor.Port,
			},
		}

	}

	return nodeTmp.Info(), nil
}

//ClosestPrecedingFinger implements NodeServer interface [rpc]
func (n *Node) ClosestPrecedingFinger(ctx context.Context, info *NodeInfo) (*NodeInfo, error) {

	return n.closestPrecedingFinger(info)
}

// closestPrecedingFinger [local invoke]
func (n *Node) closestPrecedingFinger(info *NodeInfo) (*NodeInfo, error) {

	for i := len(n.fingers) - 1; i >= 0; i-- {
		if !n.fingers[i].node.isGreaterThan(info.ID) && n.fingers[i].node.isGreaterThan(info.ID) {
			return n.fingers[i].node.Info(), nil
		}
	}

	return n.Info(), nil
}
