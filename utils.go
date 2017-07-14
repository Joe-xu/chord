package chord

import "encoding/binary"

import "math"

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
func subID(a []byte, b uint32) []byte {

	res := make([]byte, len(a))
	copy(res, a)

	tmp := binary.BigEndian.Uint32(res[len(res)-4:])
	if tmp < b {
		if len(res) < 5 {
			panic("overflow")
		}

		for i := len(res) - 5; i >= 0; i-- {
			if res[i] == 0x00 {
				res[i] = 0xff
				if i == 0 {
					panic("overflow")
				}
			} else {
				res[i]--
				break
			}
		}
	}

	binary.BigEndian.PutUint32(res[len(res)-4:], tmp-b)

	return res
}

//addID returns result of a and b
func addID(a []byte, b uint32) []byte {

	res := make([]byte, len(a))
	copy(res, a)

	tmp := binary.BigEndian.Uint32(res[len(res)-4:])

	if b > math.MaxUint32-tmp {

		if len(res) < 5 {
			panic("overflow")
		}

		for i := len(res) - 5; i >= 0; i-- {
			if res[i] == 0xff {
				res[i] = 0x00
				if i == 0 {
					panic("overflow")
				}
			} else {
				res[i]++
				break
			}
		}
	}

	binary.BigEndian.PutUint32(res[len(res)-4:], tmp+b)

	return res
}

func min(a, b int) int {
	if a > b {
		return b
	}

	return a
}
