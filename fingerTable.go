package chord

import (
	"fmt"

	"bytes"
)

type fingerTable []*finger

//item of finger table
type finger struct {
	start []byte    // ID , ( n + 2^(k-1) ) mod 2^m , 1 <= k <= m
	node  *NodeInfo //first node >= fingers[k].start
	// interval int       // [ fingers[k].start , fingers[k+1].start )
}

func (ft fingerTable) String() string {

	buf := bytes.NewBuffer(nil)
	for i := range ft {
		fmt.Fprintf(buf, "%s\n", ft[i])
	}
	return buf.String()
}

func (f *finger) String() string {
	return fmt.Sprintf("start: % x | node: %s", f.start, f.node)
}
