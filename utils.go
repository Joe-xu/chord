package chord

const (
	less = iota
	equal
	greater
)

//compareID tells a is less/greater than or euqal to b , both a and b should have the same length
func compareID(a, b []byte) int {

	if len(a) != len(b) {
		panic("different length of ID")
	}

	for i := range a {
		if a[i] < b[i] {
			return less
		}
	}

	if a[len(a)-1] == b[len(a)-1] {
		return equal
	}

	return greater
}

//subID returns result of a subtracts b
func subID(a []byte, b int) []byte {
	//<<<<<<<<
	return []byte{}
}

//max returns the greater one
func max(a, b int) int {
	if a > b {
		return a
	}

	return b
}
