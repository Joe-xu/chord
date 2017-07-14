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
	"math"
	"net"
	"strings"
	"sync"

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
	hashMethod hash.Hash
	hashBitL   int //length of ID in bit
	// Value      interface{}

	sync.Mutex              //mutex for following field
	fingers     fingerTable //fingers[0].node==successor
	predecessor *NodeInfo
	// successor   *NodeInfo
}

//NewNode init and return new node
func NewNode(port string) *Node {
	n := &Node{
		Port:       port,
		hashMethod: md5.New(),
	}
	// n.successor = n.fingers[0].node
	n.ID = n.hashMethod.Sum([]byte(n.IP + n.Port))
	n.hashBitL = len(n.ID) * 8

	n.fingers = make([]*finger, n.hashBitL)
	for i := range n.fingers {
		n.fingers[i] = &finger{}
	}

	return n
}

//Info return node's info
func (n *Node) Info() *NodeInfo {
	info := &NodeInfo{
		ID:   n.ID,
		IP:   n.IP,
		Port: n.Port,
	}

	n.Lock()
	if n.fingers[0] != nil && n.fingers[0].node != nil {
		info.Successor = n.fingers[0].node
	}
	n.Unlock()

	return info
}

//info without mutex
func (n *Node) info() *NodeInfo {
	info := &NodeInfo{
		ID:   n.ID,
		IP:   n.IP,
		Port: n.Port,
	}

	if n.fingers[0] != nil && n.fingers[0].node != nil {
		info.Successor = n.fingers[0].node
	}

	return info
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

	if len(info) >= 1 { //TODO:handle multiple join <<<<<<<<<<<<<<

		err := n.initFingerTable(info[0])
		if err != nil {
			return nil
		}
		err = n.updateOthers()
		//TODO: move keys in (predecessor,n] from successor <<<<<<
		return err
	}
	// only n in the network
	n.Lock()
	selfInfo := n.info()
	for i := range n.fingers {
		n.fingers[i].node = selfInfo
	}
	n.predecessor = selfInfo
	n.Unlock()

	return nil

}

//initFingerTable init node local finger table
func (n *Node) initFingerTable(info *NodeInfo) error {

	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", info.IP, info.Port), grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	cli := NewNodeClient(conn)

	n.Lock()
	defer n.Unlock()

	n.fingers[0].node, err = cli.FindSuccessor(context.Background(), &NodeInfo{
		ID: n.fingers[0].start,
	})
	if err != nil {
		return err
	}

	n.predecessor, err = n.fingers[0].node.predecessor()
	if err != nil {
		return err
	}
	err = n.fingers[0].node.setPredecessor(n.info())
	if err != nil {
		return err
	}

	for i := 0; i < len(n.fingers)-1; i++ {

		//if finger[i+1].start in [n.ID , n.fingers[i].node.ID)
		if compareID(n.fingers[i+1].start, n.ID) != less &&
			compareID(n.fingers[i+1].start, n.fingers[i].node.ID) == less {
			n.fingers[i+1].node = n.fingers[i].node
		} else {

			// cli := NewNodeClient(conn)
			n.fingers[i+1].node, err = cli.FindSuccessor(context.Background(), &NodeInfo{
				ID: n.fingers[i+1].start,
			})
			if err != nil {
				return err
			}

		}

	}

	return nil
}

//update all nodes whose finger tables should refer to n
func (n *Node) updateOthers() error {

	n.Lock()
	defer n.Unlock()
	for i := range n.fingers {

		p, err := n.findPredecessor(&NodeInfo{
			ID: subID(n.ID, uint32(math.Pow(2, float64(i-1)))),
		})
		if err != nil {
			return err
		}

		conn, err := grpc.Dial(fmt.Sprintf("%s:%s", p.IP, p.Port), grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()

		cli := NewNodeClient(conn)
		_, err = cli.UpdateFingerTable(context.Background(), &UpdateRequest{
			Updater: n.info(),
			I:       int32(i),
		})
		if err != nil {
			return err
		}

	}

	return nil
}

//UpdateFingerTable implements NOdeServer interface [rpc]
func (n *Node) UpdateFingerTable(ctx context.Context, req *UpdateRequest) (*google_protobuf.Empty, error) {

	return &google_protobuf.Empty{}, n.updateFingerTable(req.Updater, req.I)
}

//update finger table [local invoke]
func (n *Node) updateFingerTable(info *NodeInfo, i int32) error {

	//info.ID in [n.ID , n.fingers[i].node.ID)
	n.Lock()
	defer n.Unlock()
	if compareID(info.ID, n.ID) != less &&
		compareID(info.ID, n.fingers[i].node.ID) == less {
		n.fingers[i].node = info

		conn, err := grpc.Dial(fmt.Sprintf("%s:%s", n.predecessor.IP, n.predecessor.Port), grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()

		cli := NewNodeClient(conn)
		_, err = cli.UpdateFingerTable(context.Background(), &UpdateRequest{
			Updater: info,
			I:       i,
		})
		if err != nil {
			return err
		}

	}

	return nil
}

//force interface check
// var _ NodeServer = (*Node)(nil)

//OnJoin implements NodeServer interface [rpc]
func (n *Node) OnJoin(ctx context.Context, info *NodeInfo) (*google_protobuf.Empty, error) {

	//TODO: fake implementation  !!!!!!  <<<<<<<<<

	newNode := &Node{
		IP:   info.IP,
		Port: info.Port,
	}

	n.fingers[0].node = newNode.info()

	n.predecessor = newNode.info()

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

	nodeTmp := n.info()
	for {
		// for info.ID in (nodeTmp.ID,nodeTmp.successor.ID]
		if compRes := compareID(info.ID, nodeTmp.ID); compRes != greater {
			break
		}
		if compRes := compareID(info.ID, nodeTmp.Successor.ID); compRes == greater {
			break
		}

		conn, err := grpc.Dial(fmt.Sprintf("%s:%s", nodeTmp.IP, nodeTmp.Port), grpc.WithInsecure())
		if err != nil {
			return nil, err
		}

		cli := NewNodeClient(conn)
		nodeTmp, err = cli.ClosestPrecedingFinger(context.Background(), &NodeInfo{
			ID: info.ID,
		})
		conn.Close()
		if err != nil {
			return nil, err
		}

	}

	return nodeTmp, nil
}

//ClosestPrecedingFinger implements NodeServer interface [rpc]
func (n *Node) ClosestPrecedingFinger(ctx context.Context, info *NodeInfo) (*NodeInfo, error) {

	return n.closestPrecedingFinger(info)
}

// closestPrecedingFinger [local invoke]
func (n *Node) closestPrecedingFinger(info *NodeInfo) (*NodeInfo, error) {

	n.Lock()
	defer n.Unlock()
	for i := len(n.fingers) - 1; i >= 0; i-- {
		//n.fingers[i].node.ID in (n.ID,info.ID)
		if compareID(n.fingers[i].node.ID, n.ID) == greater &&
			compareID(n.fingers[i].node.ID, info.ID) == less {
			return n.fingers[i].node, nil
		}
	}

	return n.info(), nil
}

//Predecessor implements NodeServer interface [rpc]
func (n *Node) Predecessor(ctx context.Context, _ *google_protobuf.Empty) (*NodeInfo, error) {

	n.Lock()
	defer n.Unlock()
	return n.predecessor, nil
}

//SetPredecessor implements NodeServer interface [rpc]
func (n *Node) SetPredecessor(ctx context.Context, info *NodeInfo) (*google_protobuf.Empty, error) {

	n.Lock()
	n.predecessor = info
	n.Unlock()
	return &google_protobuf.Empty{}, nil
}
