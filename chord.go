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

	sync.RWMutex             //mutex for following field
	fingers      fingerTable //fingers[0].node == successor
	predecessor  *NodeInfo
}

//NewNode init and return new node
func NewNode(port string) *Node {
	n := &Node{
		Port:       port,
		hashMethod: md5.New(),
	}

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
			start: mod2(add(n.ID, pow2(i)), n.hashBitL),
		}

	}

	logger.Debug.Println(n.fingers) // DEBUG

	return n
}

//Info return node's info
func (n *Node) Info() *NodeInfo {

	return n.info()
}

//
func (n *Node) info() *NodeInfo {

	return &NodeInfo{
		ID:   n.ID,
		IP:   n.IP,
		Port: n.Port,
	}
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
	n.RLock()
	defer n.RUnlock()
	return fmt.Sprintf("=====\n\n%s \npredecessor:%s\n====\n\n", n.fingers, n.predecessor)
}

//Join let n join the network,accept arbitrary node's info as parma
func (n *Node) Join(info ...*NodeInfo) error {

	// defer fmt.Println(n.fingers) // DEBUG

	if len(info) >= 1 { // join an existing ring
		//TODO:handle multiple join <<<<<<<<<<<<<<
		err := n.initFingerTable(info[0])
		if err != nil {
			return nil
		}

		n.RLock()                                                             // DEBUG
		logger.Debug.Printf("%s predecessor:%s \n", n.fingers, n.predecessor) // DEBUG
		n.RUnlock()                                                           // DEBUG

		err = n.updateOthers()
		//TODO: move keys in (predecessor,n] from successor <<<<<<
		return err
	}

	// only n in the network , init a new ring
	n.Lock()
	for i := range n.fingers {
		n.fingers[i].node = n.info()
	}
	n.fingers[0].node = n.info() //patch
	n.predecessor = n.info()
	n.Unlock()

	return nil

}

/* //Join let n join the network,accept arbitrary node's info as parma
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

} */

//initFingerTable init node local finger table
func (n *Node) initFingerTable(info *NodeInfo) error {

	logger.Info.Print("[initFingerTable]")

	conn, err := info.dial()
	if err != nil {
		return err
	}
	defer conn.Close()

	n.Lock()
	defer n.Unlock()

	cli := NewNodeClient(conn)
	ctx, _ := context.WithTimeout(context.TODO(), rpcTimeout)
	n.fingers[0].node, err = cli.FindSuccessor(ctx, &NodeInfo{
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

		// finger[i+1].start in [n.ID , n.fingers[i].node.ID)
		if isBetween(n.fingers[i+1].start, n.ID, n.fingers[i].node.ID, intervLBounded) {

			n.fingers[i+1].node = n.fingers[i].node

		} else {

			n.fingers[i+1].node, err = cli.FindSuccessor(ctx, &NodeInfo{
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

	n.RLock()
	defer n.RUnlock()
	for i := range n.fingers {
		logger.Debug.Printf("===================== %d", i)
		logger.Debug.Printf("[updateOthers]findPredecessor %d:ID:% x", i, mod2(sub(n.ID, pow2(i)), n.hashBitL))
		p, err := n.findPredecessor(&NodeInfo{
			ID: mod2(sub(n.ID, pow2(i)), n.hashBitL),
		})
		logger.Debug.Printf("[updateOthers]findPredecessor:got:%v", p)
		if err != nil {
			return err
		}

		logger.Info.Printf("[updateOthers]dial up %s:%s", p.IP, p.Port)

		if isSameNode(p, n.info()) {

			logger.Warn.Print("[updateOthers] local rpc")
			n.RUnlock()
			err = n.updateFingerTable(n.info(), int32(i))
			n.RLock()
			if err != nil {
				return err
			}
			continue

		}

		conn, err := p.dial()
		if err != nil {
			return err
		}
		defer conn.Close()

		err = updateFingerTableRPC(conn, &UpdateRequest{
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

	return &google_protobuf.Empty{}, n.updateFingerTable(req.Updater, req.I)
}

func updateFingerTableRPC(conn *grpc.ClientConn, req *UpdateRequest) error {

	cli := NewNodeClient(conn)
	ctx, _ := context.WithTimeout(context.TODO(), rpcTimeout)
	_, err := cli.UpdateFingerTable(ctx, req)
	return err
}

//update finger table [local invoke]
func (n *Node) updateFingerTable(info *NodeInfo, i int32) error {

	n.RLock()

	//info.ID in [n.ID , n.fingers[i].node.ID)
	//NOTE: thought paper said "[n.ID , n.fingers[i].node.ID)" , but n's finger entry
	//		should not refer to itself unless network-init

	//EXPERIEMNTAL:check n.ID with n.fingers[i].node.ID , in case of init-chaos
	//if current finger is invaild,init-chaos,we take the newest and vaild one , aka. info.ID >= n.fingers[i].start
	if (compare(n.ID, n.fingers[i].node.ID) == equal &&
		compare(info.ID, n.fingers[i].start) != less) || //init case
		info.isBetween(n.ID, n.fingers[i].node.ID, intervUnbounded) { // checkout NOTE for why intervUnbounded

		n.RUnlock()
		n.Lock()
		n.fingers[i].node = info
		n.Unlock()
		logger.Info.Printf("[updateFingerTable] %d-th finger updated: %v", i, info)

		n.RLock()
		defer n.RUnlock()

		if isSameNode(info, n.predecessor) {
			logger.Warn.Print("[updateFingerTable] loopback update")
			return nil
		}

		if isSameNode(n.predecessor, n.info()) { //prevent dead loop
			logger.Warn.Print("[updateFingerTable]: skip local rpc")
			return nil
		}

		conn, err := n.predecessor.dial()
		if err != nil {
			return err
		}
		defer conn.Close()

		return updateFingerTableRPC(conn, &UpdateRequest{
			Updater: info,
			I:       i,
		})

	}
	n.RUnlock()
	return nil
}

//force interface check
// var _ NodeServer = (*Node)(nil)

//OnNotify implements NodeServer interface [rpc]
func (n *Node) OnNotify(ctx context.Context, info *NodeInfo) (*google_protobuf.Empty, error) {

	logger.Info.Println("OnNOtify")
	return &google_protobuf.Empty{}, n.onNotify(info)
}

func onNotifyRPC(conn *grpc.ClientConn, info *NodeInfo) error {

	cli := NewNodeClient(conn)
	ctx, _ := context.WithTimeout(context.TODO(), rpcTimeout)
	_, err := cli.OnNotify(ctx, info)

	return err
}

//	info might be n's predecessor [local invoke]
func (n *Node) onNotify(info *NodeInfo) error {

	logger.Info.Print("[onNotify]")

	n.RLock()
	// predecessor == nil or info.ID in (n.predecessor.ID , n.ID)
	if n.predecessor == nil || info.isBetween(n.predecessor.ID, n.ID, intervUnbounded) {

		n.RUnlock()
		n.Lock()
		n.predecessor = info
		n.Unlock()
		logger.Debug.Printf("[onNotify]: predecessor updated : %s", info)
		return nil
	}
	n.RUnlock()

	return nil
}

//FindSuccessor implements NodeServer interface [rpc]
func (n *Node) FindSuccessor(ctx context.Context, info *NodeInfo) (*NodeInfo, error) {

	logger.Info.Println("FindSuccessor")
	return n.findSuccessor(info)
}

func findSuccessorRPC(conn *grpc.ClientConn, info *NodeInfo) (*NodeInfo, error) {

	cli := NewNodeClient(conn)
	ctx, _ := context.WithTimeout(context.TODO(), rpcTimeout)
	return cli.FindSuccessor(ctx, info)

}

// findSuccessor  [local invoke]
func (n *Node) findSuccessor(info *NodeInfo) (*NodeInfo, error) {

	node, err := n.findPredecessor(info)
	if err != nil {
		return nil, err
	}
	return node.successor()
}

//FindPredecessor implements NodeServer interface [rpc]
func (n *Node) FindPredecessor(ctx context.Context, info *NodeInfo) (*NodeInfo, error) {

	logger.Info.Println("FindPredecessor")
	return n.findPredecessor(info)
}

func findPredecessorRPC(conn *grpc.ClientConn, info *NodeInfo) (*NodeInfo, error) {

	cli := NewNodeClient(conn)
	ctx, _ := context.WithTimeout(context.TODO(), rpcTimeout)
	return cli.FindPredecessor(ctx, info)
}

//findPredecessor [local invoke]
func (n *Node) findPredecessor(info *NodeInfo) (*NodeInfo, error) {

	logger.Info.Print("[findPredecessor]")

	np := n.info() //node predecessor
	n.RLock()
	successor := n.fingers[0].node
	n.RUnlock()

	// while info.ID NOT in (np.ID,np.successor.ID]
	//EXPERIEMNTAL:check np.ID with np.Successor.ID , in case of init chaos
	var err error
	preN := np //store prev node
	for {

		logger.Debug.Printf("\ninfoID:% x \nnpID: % x \nsuccessorID: % x \n res:%v", info.ID, np.ID, successor.ID, info.isBetween(np.ID, successor.ID, intervRBounded))
		if info.isBetween(np.ID, successor.ID, intervRBounded) {
			break
		}

		preN = np
		if isSameNode(np, n.info()) {

			np, err = n.closestPrecedingFinger(info)
			if err != nil {
				return nil, err
			}

		} else {

			conn, err := np.dial()
			if err != nil {
				return nil, err
			}

			np, err = closestPrecedingFingerRPC(conn, &NodeInfo{
				ID: info.ID,
			})
			conn.Close()
			if err != nil {
				return nil, err
			}

		}

		successor, err = np.successor()
		if err != nil {
			return nil, err
		}

		logger.Debug.Printf("[findPredecessor]:np:%s", np)
		logger.Debug.Printf("[findPredecessor]:successor:%s", successor)
		if compare(np.ID, preN.ID) == equal ||
			// compare(successor.ID, preN.ID) == equal ||
			compare(np.ID, successor.ID) != less {
			logger.Warn.Print("[findPredecessor]break dead loop")
			break
		}

	}

	return np, nil
}

//ClosestPrecedingFinger implements NodeServer interface [rpc]
func (n *Node) ClosestPrecedingFinger(ctx context.Context, info *NodeInfo) (*NodeInfo, error) {

	logger.Info.Println("ClosestPrecedingFinger")
	return n.closestPrecedingFinger(info)
}

func closestPrecedingFingerRPC(conn *grpc.ClientConn, info *NodeInfo) (*NodeInfo, error) {

	cli := NewNodeClient(conn)
	ctx, _ := context.WithTimeout(context.TODO(), rpcTimeout)
	return cli.ClosestPrecedingFinger(ctx, info)

}

// closestPrecedingFinger [local invoke]
func (n *Node) closestPrecedingFinger(info *NodeInfo) (*NodeInfo, error) {

	n.RLock()
	defer n.RUnlock()
	for i := len(n.fingers) - 1; i >= 0; i-- {

		//n.fingers[i].node.ID in (n.ID,info.ID)
		if n.fingers[i].node.isBetween(n.ID, info.ID, intervUnbounded) {

			logger.Debug.Printf("[closestPrecedingFinger] return %d-th finger %v", i, n.fingers[i].node)
			return n.fingers[i].node, nil
		}
	}

	logger.Debug.Print("[closestPrecedingFinger] return itself")
	return n.info(), nil
}

//Predecessor implements NodeServer interface [rpc]
func (n *Node) Predecessor(ctx context.Context, _ *google_protobuf.Empty) (*NodeInfo, error) {

	n.RLock()
	logger.Info.Printf("[Predecessor]: %s", n.predecessor)
	defer n.RUnlock()
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

	n.RLock()
	logger.Info.Printf("[Successor]: %s", n.fingers[0].node)
	defer n.RUnlock()
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
	n.RLock()
	defer n.RUnlock()

	if isSameNode(n.fingers[0].node, n.info()) {

		logger.Warn.Print("[Stabilize]:try to notify itself")
		return nil
	}

	sp, err := n.fingers[0].node.predecessor()
	if err != nil {
		return err
	}

	// sp in (n.ID , n.successor.ID)
	if sp.isBetween(n.ID, n.fingers[0].node.ID, intervUnbounded) {

		n.RUnlock()
		n.Lock()
		n.fingers[0].node = sp
		n.Unlock()
		n.RLock()

	}

	conn, err := n.fingers[0].node.dial()
	if err != nil {
		return err
	}
	defer conn.Close()

	return onNotifyRPC(conn, n.info())

}

//FixFingers refresh finger
func (n *Node) FixFingers() error {

	i := randInt(1, n.hashBitL-1) //skip successor
	logger.Info.Printf("[FixFingers] fix %d-th", i)

	successor, err := n.findSuccessor(&NodeInfo{ID: n.fingers[i].start})
	if err != nil {
		return err
	}

	n.Lock()
	n.fingers[i].node = successor
	logger.Info.Printf("[FixFingers] updated %d-th : %v", i, n.fingers[i].node)
	n.Unlock()

	return nil
}
