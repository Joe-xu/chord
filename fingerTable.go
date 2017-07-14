package chord

import (
	"fmt"

	google_protobuf "github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type fingerTable []*finger

//item of finger table
type finger struct {
	start    []byte    // ID , ( n + 2^(k-1) ) mod 2^m , 1 <= k <= m
	interval int       // [ fingers[k].start , fingers[k+1].start )
	node     *NodeInfo //first node >= fingers[k].start
}

//get predecessor
func (n *NodeInfo) predecessor() (*NodeInfo, error) {

	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", n.IP, n.Port), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cli := NewNodeClient(conn)
	return cli.Predecessor(context.Background(), &google_protobuf.Empty{})

}

func (n *NodeInfo) setPredecessor(info *NodeInfo) error {

	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", n.IP, n.Port), grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	cli := NewNodeClient(conn)
	_, err = cli.SetPredecessor(context.Background(), info)
	return err
}
