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
	"net"
	"sync"
	"time"

	"github.com/Joe-xu/glog"

	google_protobuf "github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

//Node prototype on the chord ring
type Node struct {
	id   []byte
	addr string

	config *Config

	hashBitL int //length of ID in bit

	store storage

	sync.RWMutex             //mutex for following field
	fingers      fingerTable //fingers[0].node == successor
	predecessor  *NodeInfo
}

//NewNode init and return new node
func NewNode(config *Config) *Node {

	rpcTimeout = config.Timeout

	n := &Node{
		config: config,
		addr:   config.Listener.Addr().String(),
	}

	id := md5.Sum([]byte(n.addr))
	n.id = id[:]

	n.hashBitL = len(n.id) * 8
	n.id = mod2(n.id, n.hashBitL)

	n.fingers = make([]*finger, n.hashBitL)

	for i := range n.fingers {
		n.fingers[i] = &finger{
			start: mod2(add(n.id, pow2(i)), n.hashBitL),
		}

	}

	glog.V(3).Infoln(n.fingers)

	return n
}

//Info return node's info
func (n *Node) Info() *NodeInfo {

	return n.info()
}

//return node's info
func (n *Node) info() *NodeInfo {

	return &NodeInfo{
		ID:       n.id,
		Addr:     n.addr,
		HttpPort: n.config.HttpPort,
	}
}

//ID return node's id
func (n *Node) ID() []byte {
	return n.id
}

//Addr return node's addr
func (n *Node) Addr() string {
	return n.addr
}

//JoinAndServe makes node join and listen on the ring
func (n *Node) JoinAndServe() error {

	errCh := make(chan error, 0)

	go func() {
		errCh <- n.serve(n.config.Listener)
		// close(errCh)
	}()

	go func() {
		errCh <- n.join(n.config.Introducer)
		errCh <- n.schedule()
	}()

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

//schedule stabilize and fix fingers task
func (n *Node) schedule() error {

	var stabilizeTimer, fixFingersTimer *time.Ticker

	if n.config.StabilizeInterval > 0*time.Second {
		stabilizeTimer = time.NewTicker(n.config.StabilizeInterval)
	} else {
		stabilizeTimer = time.NewTicker(defaultStabilizeInterval)
	}

	if n.config.FixFingersInterval > 0*time.Second {
		fixFingersTimer = time.NewTicker(n.config.FixFingersInterval)
	} else {
		fixFingersTimer = time.NewTicker(defaultFixFingersInterval)
	}

	for {
		select {
		case <-stabilizeTimer.C:
			err := n.Stabilize()
			if err != nil {
				return err
			}

		case <-fixFingersTimer.C:

			err := n.FixFingers()
			if err != nil {
				return err
			}
		}

	}

}

//serve makes node listen on the ring
func (n *Node) serve(listener net.Listener) error {

	rpcServer := grpc.NewServer()
	RegisterNodeServer(rpcServer, n)
	reflection.Register(rpcServer)
	return rpcServer.Serve(listener)

}

//join let n join the network,accept arbitrary node's info as parma
func (n *Node) join(introducer *NodeInfo) error {

	if introducer != nil { // join an existing ring

	JOIN_INIT_FINGERS: //only used in retry
		err := n.initFingerTable(introducer)
		if err == context.DeadlineExceeded {
			glog.Warningf("[Join]initFingerTable: %v", err)
			goto JOIN_INIT_FINGERS //retry
		} else if err != nil {
			return err
		}

		// n.RLock()                                                             // DEBUG
		// logger.Debug.Printf("%s predecessor:%s \n", n.fingers, n.predecessor) // DEBUG
		// n.RUnlock()                                                           // DEBUG

	JOIN_UPDATE_OTHERS: //only used in retry
		err = n.updateOthers()
		if err == context.DeadlineExceeded {
			glog.Warningf("[Join]updateOthers: %v", err)
			goto JOIN_UPDATE_OTHERS //retry
		} else if err != nil {
			return err
		}

		return nil
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

//initFingerTable init node local finger table
func (n *Node) initFingerTable(info *NodeInfo) error {

	glog.Infoln("[initFingerTable]")

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

	//EXPERIEMNTAL
	err = n.predecessor.setSuccessor(n.info())
	if err != nil {
		return err
	}

	err = n.fingers[0].node.setPredecessor(n.info())
	if err != nil {
		return err
	}

	for i := 0; i < len(n.fingers)-1; i++ {

		// finger[i+1].start in [n.ID , n.fingers[i].node.ID)
		if isBetween(n.fingers[i+1].start, n.id, n.fingers[i].node.ID, intervLBounded) {

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
		glog.V(3).Infof("===================== %d", i)
		glog.V(3).Infof("[updateOthers]findPredecessor %d:ID:% x", i, mod2(sub(n.id, pow2(i)), n.hashBitL))
		p, err := n.findPredecessor(&NodeInfo{
			ID: mod2(sub(n.id, pow2(i)), n.hashBitL),
		})
		glog.V(3).Infof("[updateOthers]findPredecessor:got:%v", p)
		if err != nil {
			return err
		}

		glog.V(3).Infof("[updateOthers]dial up %s", p.Addr)

		if isSameNode(p, n.info()) {

			glog.Warningf("[updateOthers] local rpc")
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

//Metadata implements NOdeServer interface [rpc]
func (n *Node) Metadata(ctx context.Context, _ *google_protobuf.Empty) (*NodeInfo, error) {

	return &NodeInfo{
		ID:       n.id,
		Addr:     n.addr,
		HttpPort: n.config.HttpPort,
	}, nil
}

//UpdateFingerTable implements NOdeServer interface [rpc]
func (n *Node) UpdateFingerTable(ctx context.Context, req *UpdateRequest) (*google_protobuf.Empty, error) {

	glog.Infoln("UpdateFingerTable")

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
	if info.isBetween(n.id, n.fingers[i].node.ID, intervLBounded) {

		n.RUnlock()
		n.Lock()
		n.fingers[i].node = info
		n.Unlock()
		glog.Infof("[updateFingerTable] %d-th finger updated: %v", i, info)

		n.RLock()
		defer n.RUnlock()

		if isSameNode(info, n.predecessor) {
			glog.Warningln("[updateFingerTable] loopback update")
			return nil
		}

		if isSameNode(n.predecessor, n.info()) { //prevent dead loop
			glog.Warningln("[updateFingerTable]: skip local rpc")
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

	glog.Infoln("OnNOtify")
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

	glog.Infoln("[onNotify]")

	n.RLock()
	// predecessor == nil or info.ID in (n.predecessor.ID , n.ID)
	if n.predecessor == nil || info.isBetween(n.predecessor.ID, n.id, intervUnbounded) {

		n.RUnlock()
		n.Lock()
		n.predecessor = info
		n.Unlock()
		glog.V(3).Infof("[onNotify]: predecessor updated : %s", info)
		return nil
	}
	n.RUnlock()

	return nil
}

//FindSuccessor implements NodeServer interface [rpc]
func (n *Node) FindSuccessor(ctx context.Context, info *NodeInfo) (*NodeInfo, error) {

	glog.Infoln("FindSuccessor")
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

	glog.Infoln("FindPredecessor")
	return n.findPredecessor(info)
}

func findPredecessorRPC(conn *grpc.ClientConn, info *NodeInfo) (*NodeInfo, error) {

	cli := NewNodeClient(conn)
	ctx, _ := context.WithTimeout(context.TODO(), rpcTimeout)
	return cli.FindPredecessor(ctx, info)
}

//findPredecessor [local invoke]
func (n *Node) findPredecessor(info *NodeInfo) (*NodeInfo, error) {

	glog.Infoln("[findPredecessor]")

	np := n.info() //node predecessor
	n.RLock()
	successor := n.fingers[0].node
	n.RUnlock()

	// while info.ID NOT in (np.ID,np.successor.ID]
	//EXPERIEMNTAL:check np.ID with np.Successor.ID , in case of init chaos
	var err error
	preN := np //store prev node
	for {

		glog.V(3).Infof("\ninfoID:% x \nnpID: % x \nsuccessorID: % x \n res:%v", info.ID, np.ID, successor.ID, info.isBetween(np.ID, successor.ID, intervRBounded))
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

		glog.V(3).Infof("[findPredecessor]:np:%s", np)
		glog.V(3).Infof("[findPredecessor]:successor:%s", successor)
		if compare(np.ID, preN.ID) == equal ||
			// compare(successor.ID, preN.ID) == equal ||
			compare(np.ID, successor.ID) != less {
			glog.V(3).Infof("[findPredecessor]break dead loop")
			break
		}

	}

	return np, nil
}

//ClosestPrecedingFinger implements NodeServer interface [rpc]
func (n *Node) ClosestPrecedingFinger(ctx context.Context, info *NodeInfo) (*NodeInfo, error) {

	glog.Infoln("ClosestPrecedingFinger")
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
		if n.fingers[i].node.isBetween(n.id, info.ID, intervUnbounded) {

			glog.V(3).Infof("[closestPrecedingFinger] return %d-th finger %v", i, n.fingers[i].node)
			return n.fingers[i].node, nil
		}
	}

	glog.V(3).Infoln("[closestPrecedingFinger] return itself")
	return n.info(), nil
}

//Predecessor implements NodeServer interface [rpc]
func (n *Node) Predecessor(ctx context.Context, _ *google_protobuf.Empty) (*NodeInfo, error) {

	n.RLock()
	glog.Infof("[Predecessor]: %s", n.predecessor)
	defer n.RUnlock()
	return n.predecessor, nil

}

//SetPredecessor implements NodeServer interface [rpc]
func (n *Node) SetPredecessor(ctx context.Context, info *NodeInfo) (*google_protobuf.Empty, error) {

	glog.Infof("[SetPredecessor]: %s", info)
	n.Lock()
	n.predecessor = info
	n.Unlock()
	return &google_protobuf.Empty{}, nil

}

//Successor implements NodeServer interface [rpc]
func (n *Node) Successor(ctx context.Context, _ *google_protobuf.Empty) (*NodeInfo, error) {

	n.RLock()
	glog.Infof("[Successor]: %s", n.fingers[0].node)
	defer n.RUnlock()
	return n.fingers[0].node, nil

}

//SetSuccessor implements NodeServer interface [rpc]
func (n *Node) SetSuccessor(ctx context.Context, info *NodeInfo) (*google_protobuf.Empty, error) {

	glog.Infof("[SetSuccessor]: %s", info)
	n.Lock()
	n.fingers[0].node = info
	n.Unlock()
	return &google_protobuf.Empty{}, nil

}

//Stabilize verify n's immediate successor and tell the successor about it
func (n *Node) Stabilize() error {

	glog.Infoln("Stabilize")
	n.RLock()
	defer n.RUnlock()

	if isSameNode(n.fingers[0].node, n.info()) {

		glog.Warningln("[Stabilize]:try to notify itself")
		return nil
	}

	sp, err := n.fingers[0].node.predecessor()
	if err != nil {
		return err
	}

	// sp in (n.ID , n.successor.ID)
	if sp.isBetween(n.id, n.fingers[0].node.ID, intervUnbounded) {

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
	glog.Infof("[FixFingers] fix %d-th", i)

	successor, err := n.findSuccessor(&NodeInfo{ID: n.fingers[i].start})
	if err != nil {
		return err
	}

	n.Lock()
	n.fingers[i].node = successor
	glog.Infof("[FixFingers] updated %d-th : %v", i, n.fingers[i].node)
	n.Unlock()

	return nil
}
