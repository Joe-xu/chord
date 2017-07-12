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
	start    []byte //ID
	interval int
	node     *NodeInfo
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
