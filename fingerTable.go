package chord

import (
	"fmt"

	"bytes"

	google_protobuf "github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const MaxFingerTableLen = 32

type fingerTable []*finger

//item of finger table
type finger struct {
	start    []byte    // ID , ( n + 2^(k-1) ) mod 2^m , 1 <= k <= m
	interval int       // [ fingers[k].start , fingers[k+1].start )
	node     *NodeInfo //first node >= fingers[k].start
}

func (ft fingerTable) String() string {

	buf := bytes.NewBuffer(nil)
	for i := range ft {
		fmt.Fprintf(buf, "%s\n", ft[i])
	}
	return buf.String()
}

func (f *finger) String() string {
	return fmt.Sprintf("start: % x | interval: %d | node: %s", f.start, f.interval, f.node)
}

// func (n *NodeInfo) String() string {
// 	return fmt.Sprintf("ID: % x | Addr: %s:%s ", n.ID, n.IP, n.Port)
// }

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
