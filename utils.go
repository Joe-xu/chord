package chord

//compareID returns true when a is greater than b
//both a and b should have the same length
func compareID(a, b []byte) bool {

	if len(a) != len(b) {
		panic("different length of ID")
	}

	for i := range a {
		if a[i] < b[i] {
			return false
		}
	}

	if a[len(a)-1] == b[len(a)-1] {
		panic("same ID")
	}

	return true
}

//max returns the greater one
func max(a, b int) int {
	if a > b {
		return a
	}

	return b
}
