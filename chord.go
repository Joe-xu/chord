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
	"sync"

	"github.com/Joe-xu/logger"
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

	//DEBUG
	if port == "50015" {
		n.ID = []byte{0x01}
	} else if port == "50016" {
		n.ID = []byte{0x10}
	} else if port == "50017" {
		n.ID = []byte{0x18}
	}

	n.hashBitL = len(n.ID) * 8
	n.ID = mod2(n.ID, n.hashBitL)

	n.fingers = make([]*finger, n.hashBitL)

	for i := range n.fingers {
		n.fingers[i] = &finger{
			start: mod2(addID(n.ID, pow2(i)), n.hashBitL),
		}

	}

	logger.Debug.Println(n.fingers) // DEBUG

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

// DEBUG
func (n *Node) LogFingerTable() string {
	n.Lock()
	defer n.Unlock()
	return fmt.Sprintf("=====\n\n%s \npredecessor:%s\n====\n\n", n.fingers, n.predecessor)
}

// //Join let n join the network,accept arbitrary node's info as parma
// func (n *Node) Join(info ...*NodeInfo) error {

// 	// defer fmt.Println(n.fingers) // DEBUG
// 	n.Lock()
// 	defer n.Unlock()

// 	if len(info) >= 1 { //TODO:handle multiple join <<<<<<<<<<<<<<

// 		err := n.initFingerTable(info[0])
// 		if err != nil {
// 			return nil
// 		}

// 		logger.Debug.Printf("%s predecessor:%s \n", n.fingers, n.predecessor) // DEBUG

// 		err = n.updateOthers()
// 		//TODO: move keys in (predecessor,n] from successor <<<<<<
// 		return err
// 	}
// 	// only n in the network
// 	for i := range n.fingers {
// 		n.fingers[i].node = n.info()
// 	}
// 	n.fingers[0].node = n.info()      //patch
// 	n.fingers[0].node.Successor = nil //clean off useless data
// 	n.predecessor = n.info()

// 	return nil

// }

//Join let n join the network,accept arbitrary node's info as parma
func (n *Node) Join(info ...*NodeInfo) error {

	// defer fmt.Println(n.fingers) // DEBUG
	n.Lock()
	defer n.Unlock()

	for i := range n.fingers {
		n.fingers[i].node = n.info()
	}
	if len(info) >= 1 { //TODO:handle multiple join <<<<<<<<<<<<<<

		logger.Info.Printf("[initFingerTable]dial up %s:%s", info[0].IP, info[0].Port)
		conn, err := grpc.Dial(fmt.Sprintf("%s:%s", info[0].IP, info[0].Port), grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()

		cli := NewNodeClient(conn)
		n.fingers[0].node, err = cli.FindSuccessor(context.Background(), n.info())
		n.fingers[0].node.Successor = nil //clean off useless data
		n.predecessor = nil

		return err
	}
	// only n in the network
	// for i := range n.fingers {
	// 	n.fingers[i].node = n.info()
	// }
	n.fingers[0].node = n.info()      //patch
	n.fingers[0].node.Successor = nil //clean off useless data
	n.predecessor = n.info()

	return nil

}

//initFingerTable init node local finger table
func (n *Node) initFingerTable(info *NodeInfo) error {

	logger.Info.Printf("[initFingerTable]dial up %s:%s", info.IP, info.Port)
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", info.IP, info.Port), grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	cli := NewNodeClient(conn)

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

	for i := range n.fingers {
		logger.Debug.Printf("[updateOthers]findPredecessor %d:ID:% x", i, mod2(subID(n.ID, pow2(i)), n.hashBitL))
		p, err := n.findPredecessor(&NodeInfo{
			ID: mod2(subID(n.ID, pow2(i)), n.hashBitL),
		})
		if err != nil {
			return err
		}
		logger.Debug.Printf("[updateOthers]findPredecessor:got:%v", p)

		logger.Info.Printf("[updateOthers]dial up %s:%s", p.IP, p.Port)

		if isSameNode(p, n.info()) {
			logger.Warn.Print("[updateOthers] local rpc")
			err = n.updateFingerTable(n.info(), int32(i))
			if err != nil {
				return err
			}
			continue
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

	logger.Info.Println("UpdateFingerTable")
	n.Lock()
	defer n.Unlock()
	return &google_protobuf.Empty{}, n.updateFingerTable(req.Updater, req.I)
}

//update finger table [local invoke]
func (n *Node) updateFingerTable(info *NodeInfo, i int32) error {

	//info.ID in [n.ID , n.fingers[i].node.ID)
	//EXPERIEMNTAL:check n.ID with n.fingers[i].node.ID , in case of init-chaos
	//if current finger is invaild,init-chaos,we take the newest and vaild one , aka. info.ID >= n.fingers[i].start
	if (compareID(n.ID, n.fingers[i].node.ID) == equal &&
		compareID(info.ID, n.fingers[i].start) != less) || //init case
		(compareID(info.ID, n.ID) != less && //common case
			compareID(info.ID, n.fingers[i].node.ID) == less) {

		n.fingers[i].node = info
		logger.Info.Printf("[updateFingerTable] %d-th finger updated: %v", i, info)

		if isSameNode(info, n.predecessor) {
			logger.Warn.Print("[updateFingerTable] loopback update")
			return nil
		}

		logger.Info.Printf("[updateFingerTable]dial up %s:%s", n.predecessor.IP, n.predecessor.Port)
		if isSameNode(n.predecessor, n.info()) { //prevent dead loop
			logger.Warn.Print("[updateFingerTable]: skip local rpc")
			return nil
		}

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

//OnNotify implements NodeServer interface [rpc]
func (n *Node) OnNotify(ctx context.Context, info *NodeInfo) (*google_protobuf.Empty, error) {
	logger.Info.Println("OnNOtify")
	n.Lock()
	defer n.Unlock()
	return &google_protobuf.Empty{}, n.onNotify(info)
}

//	info might be n's predecessor [local invoke]
func (n *Node) onNotify(info *NodeInfo) error {

	logger.Info.Print("[onNotify]")

	// predecessor == nil or info.ID in (n.predecessor.ID , n.ID)
	if n.predecessor == nil || (compareID(info.ID, n.predecessor.ID) == greater &&
		compareID(info.ID, n.ID) == less) {

		n.predecessor = info
		logger.Debug.Printf("[onNotify]: predecessor updated : %s", info)
	}

	return nil
}

//FindSuccessor implements NodeServer interface [rpc]
func (n *Node) FindSuccessor(ctx context.Context, info *NodeInfo) (*NodeInfo, error) {
	logger.Info.Println("FindSuccessor")
	n.Lock()
	defer n.Unlock()
	return n.findSuccessor(info)
}

// findSuccessor  [local invoke]
func (n *Node) findSuccessor(info *NodeInfo) (*NodeInfo, error) {

	node, err := n.findPredecessor(info)
	node.Successor.Successor = nil //clean off useless data
	return node.Successor, err
}

//FindPredecessor implements NodeServer interface [rpc]
func (n *Node) FindPredecessor(ctx context.Context, info *NodeInfo) (*NodeInfo, error) {
	logger.Info.Println("FindPredecessor")
	n.Lock()
	defer n.Unlock()
	return n.findPredecessor(info)
}

//findPredecessor [local invoke]
func (n *Node) findPredecessor(info *NodeInfo) (*NodeInfo, error) {

	logger.Info.Print("[findPredecessor]")
	nodeTmp := n.info()

	// for info.ID NOT in (nodeTmp.ID,nodeTmp.successor.ID]
	//EXPERIEMNTAL:check nodeTmp.ID with nodeTmp.Successor.ID , in case of init chaos
	for compareID(info.ID, nodeTmp.ID) != greater ||
		compareID(info.ID, nodeTmp.Successor.ID) == greater {

		logger.Debug.Print("===================================")
		logger.Debug.Printf("infoID:% x \nnodeTmpID: % x \n res:%v", info.ID, nodeTmp.ID, compareID(info.ID, nodeTmp.ID) != greater)
		logger.Debug.Printf("infoID:% x \nnodeTmpSuID: % x \n res:%v", info.ID, nodeTmp.Successor.ID, compareID(info.ID, nodeTmp.Successor.ID) == greater)

		if isSameNode(nodeTmp, n.info()) {

			var err error
			nodeTmp, err = n.closestPrecedingFinger(info)
			if err != nil {
				return nil, err
			}

			if compareID(nodeTmp.ID, n.ID) == equal {
				logger.Warn.Print("[findPredecessor][local invoke]break dead loop")
				break
			}

		} else {
			logger.Info.Printf("[findPredecessor]dial up %s:%s", nodeTmp.IP, nodeTmp.Port)
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

			if compareID(nodeTmp.ID, nodeTmp.Successor.ID) != less {
				logger.Warn.Print("[findPredecessor][rpc]break dead loop")
				break
			}
		}

		logger.Debug.Printf("infoID:% x \nnodeTmpID: % x \n res:%v", info.ID, nodeTmp.ID, compareID(info.ID, nodeTmp.ID) != greater)
		logger.Debug.Printf("infoID:% x \nnodeTmpSuID: % x \n res:%v", info.ID, nodeTmp.Successor.ID, compareID(info.ID, nodeTmp.Successor.ID) == greater)
		logger.Debug.Print("===================================")

		// if compareID(nodeTmp.ID, n.ID) == equal { //prevent dead loop
		// 	logger.Warn.Print("[findPredecessor]break dead loop")
		// 	break
		// }

		// if compareID(nodeTmp.ID, nodeTmp.Successor.ID) != less {
		// 	break
		// }

	}

	return nodeTmp, nil
}

//ClosestPrecedingFinger implements NodeServer interface [rpc]
func (n *Node) ClosestPrecedingFinger(ctx context.Context, info *NodeInfo) (*NodeInfo, error) {
	logger.Info.Println("ClosestPrecedingFinger")
	n.Lock()
	defer n.Unlock()
	return n.closestPrecedingFinger(info)
}

// closestPrecedingFinger [local invoke]
func (n *Node) closestPrecedingFinger(info *NodeInfo) (*NodeInfo, error) {

	logger.Debug.Print("[closestPrecedingFinger]:enter")
	defer func() {
		logger.Debug.Print("[closestPrecedingFinger]:exit")
	}()

	logger.Debug.Print("[closestPrecedingFinger]: start loop")
	for i := len(n.fingers) - 1; i >= 0; i-- {

		//n.fingers[i].node.ID in (n.ID,info.ID)
		//EXPERIEMNTAL:check n.fingers[i].node.ID with n.fingers[i].start , in case of init chaos
		if (compareID(n.fingers[i].node.ID, n.ID) == greater &&
			compareID(n.fingers[i].node.ID, info.ID) == less) ||
			(compareID(n.fingers[i].node.ID, n.fingers[i].start) == less && //init chaos
				// compareID(n.ID, n.fingers[i].node.ID) != equal &&
				compareID(info.ID, n.fingers[i].start) != greater) { //TODO: handle init chaos

			if n.fingers[i].node.Successor == nil { //missing successor data

				if isSameNode(n.fingers[i].node, n.info()) {
					n.fingers[i].node.Successor = n.fingers[0].node

				} else {
					var err error
					n.fingers[i].node.Successor, err = n.fingers[i].node.successor()
					if err != nil {
						return nil, err
					}
				}

			}

			logger.Debug.Printf("[closestPrecedingFinger] return %d-th finger %v", i, n.fingers[i].node)
			return n.fingers[i].node, nil
		}
	}

	logger.Debug.Print("[closestPrecedingFinger] return itself")
	return n.info(), nil
}

//Predecessor implements NodeServer interface [rpc]
func (n *Node) Predecessor(ctx context.Context, _ *google_protobuf.Empty) (*NodeInfo, error) {

	n.Lock()
	logger.Info.Printf("[Predecessor]: %s", n.predecessor)
	defer n.Unlock()
	return n.predecessor, nil

}

//SetPredecessor implements NodeServer interface [rpc]
func (n *Node) SetPredecessor(ctx context.Context, info *NodeInfo) (*google_protobuf.Empty, error) {

	logger.Info.Printf("[SetPredecessor]: %s", info)
	n.Lock()
	n.predecessor = info
	n.Unlock()
	return &google_protobuf.Empty{}, nil

}

//Successor implements NodeServer interface [rpc]
func (n *Node) Successor(ctx context.Context, _ *google_protobuf.Empty) (*NodeInfo, error) {

	n.Lock()
	logger.Info.Printf("[Successor]: %s", n.fingers[0].node)
	defer n.Unlock()
	return n.fingers[0].node, nil

}

//SetSuccessor implements NodeServer interface [rpc]
func (n *Node) SetSuccessor(ctx context.Context, info *NodeInfo) (*google_protobuf.Empty, error) {

	logger.Info.Printf("[SetSuccessor]: %s", info)
	n.Lock()
	n.fingers[0].node = info
	n.Unlock()
	return &google_protobuf.Empty{}, nil

}

//Stabilize verify n's immediate successor and tell the successor about it
func (n *Node) Stabilize() error {

	logger.Info.Print("Stabilize")
	n.Lock()
	defer n.Unlock()

	if isSameNode(n.fingers[0].node, n.info()) {

		logger.Warn.Print("[Stabilize]:try to notify itself")
		return nil
	}

	sp, err := n.fingers[0].node.predecessor()
	if err != nil {
		return err
	}

	// sp in (n.ID , n.successor.ID)
	if compareID(sp.ID, n.ID) == greater &&
		compareID(sp.ID, n.fingers[0].node.ID) == less {
		n.fingers[0].node = sp
	}

	logger.Info.Printf("[Stabilize]dial up %s:%s", n.fingers[0].node.IP, n.fingers[0].node.Port)
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", n.fingers[0].node.IP, n.fingers[0].node.Port), grpc.WithInsecure())
	if err != nil {
		return err
	}

	cli := NewNodeClient(conn)
	_, err = cli.OnNotify(context.Background(), n.info())
	conn.Close()

	return err
}

//FixFingers refresh finger
func (n *Node) FixFingers() error {

	i := randInt(1, n.hashBitL-1) //skip successor
	logger.Info.Printf("[FixFingers] fix %d-th", i)
	n.Lock()
	defer n.Unlock()
	var err error
	n.fingers[i].node, err = n.findSuccessor(&NodeInfo{ID: n.fingers[i].start})
	logger.Info.Printf("[FixFingers] updated %d-th : %v", i, n.fingers[i].node)

	return err
}
