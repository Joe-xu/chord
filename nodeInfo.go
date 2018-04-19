package chord

import (
	"github.com/Joe-xu/glog"
	"github.com/bitly/go-simplejson"

	google_protobuf "github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

//*************************************
// type NodeInfo defined in chord.proto
//	-node information prototype
//*************************************

// func (n *NodeInfo) String() string {
// 	return fmt.Sprintf("ID: % x | Addr: %s:%s ", n.ID, n.IP, n.Port)
// }

func (ni *NodeInfo) isBetween(start, end []byte, intervalType int) bool {
	return isBetween(ni.ID, start, end, intervalType)
}

func (ni *NodeInfo) dial() (*grpc.ClientConn, error) {

	glog.Infof("[dial]: %s", ni.Addr)
	return grpc.Dial(ni.Addr, grpc.WithInsecure())

}

//get predecessor [rpc]
func (ni *NodeInfo) predecessor() (*NodeInfo, error) {

	glog.Infoln("[predecessor]")
	conn, err := ni.dial()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cli := NewNodeClient(conn)
	ctx, _ := context.WithTimeout(context.TODO(), rpcTimeout)
	return cli.Predecessor(ctx, &google_protobuf.Empty{})

}

//set Predecessor [rpc]
func (ni *NodeInfo) setPredecessor(info *NodeInfo) error {

	glog.Infof("[setPredecessor]")
	conn, err := ni.dial()
	if err != nil {
		return err
	}
	defer conn.Close()

	cli := NewNodeClient(conn)
	ctx, _ := context.WithTimeout(context.TODO(), rpcTimeout)
	_, err = cli.SetPredecessor(ctx, info)
	return err
}

//get successor [rpc]
func (ni *NodeInfo) successor() (*NodeInfo, error) {

	glog.Infoln("[successor]")
	conn, err := ni.dial()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cli := NewNodeClient(conn)
	ctx, _ := context.WithTimeout(context.TODO(), rpcTimeout)
	return cli.Successor(ctx, &google_protobuf.Empty{})

}

//set Successor  [rpc]
func (ni *NodeInfo) setSuccessor(info *NodeInfo) error {

	glog.Infoln("[setSuccessor]")
	conn, err := ni.dial()
	if err != nil {
		return err
	}
	defer conn.Close()

	cli := NewNodeClient(conn)
	ctx, _ := context.WithTimeout(context.TODO(), rpcTimeout)
	_, err = cli.SetSuccessor(ctx, info)
	return err
}

func (ni *NodeInfo) json() *simplejson.Json {

	j := simplejson.New()

	j.Set("addr", ni.Addr)
	j.Set("id", ni.ID)

	return j
}
