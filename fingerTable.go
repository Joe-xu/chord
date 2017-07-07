package chord

type fingerTable []*finger

//item of finger table
type finger struct {
	start    int
	interval int
	node     *Node
}
