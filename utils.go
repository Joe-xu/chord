package chord

import (
	crand "crypto/rand"
	"encoding/binary"
	"math"
	"math/rand"
)

const (
	less = iota
	equal
	greater
)

//compare tells a is less/greater than or euqal to b , both a and b should have the same length
func compare(a, b []byte) int {

	if len(a) != len(b) {
		panic("different in length")
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

//sub returns result of val subtracts n
//assume val's length  is bigger than n's
func sub(val, n []byte) []byte {

	nLen, valLen := len(n), len(val)
	diff := valLen - nLen
	if diff < 0 {
		panic("overflow")
	}

	res := make([]byte, valLen)
	copy(res, val)

	flag := byte(0x00)
	for i := valLen - 1; i >= 0; i-- {

		if val[i] < flag { //val[i]-flag overflow
			res[i] -= flag
			flag = 0x00
			flag++
		} else {
			res[i] -= flag
			flag = 0x00
		}

		if i >= diff {

			if res[i] < n[i-diff] {
				flag++
			}
			res[i] -= n[i-diff]

		}

		if i-diff <= 0 && flag == 0 {
			break
		}

	}

	// if flag != 0x00 {
	// 	panic("overflow")
	// }

	return res
}

//add returns result of val and n
//assume val's length  is bigger than n's
func add(val, n []byte) []byte {

	nLen, valLen := len(n), len(val)
	diff := valLen - nLen
	if diff < 0 {
		panic("overflow")
	}

	res := make([]byte, valLen)
	copy(res, val)

	flag := byte(0x00)
	for i := valLen - 1; i >= 0; i-- {

		if math.MaxUint8-val[i] < flag { // val[i]+flag overflow
			res[i] += flag
			flag = 0x00
			flag++
		} else {
			res[i] += flag
			flag = 0x00
		}

		if i >= diff {

			if math.MaxUint8-res[i] < n[i-diff] {
				flag++
			}
			res[i] += n[i-diff]

		}

		if i-diff <= 0 && flag == 0 {
			break
		}

	}

	if flag != 0x00 {
		panic("overflow")
	}

	return res
}

//pow2 return result of 2^e in byte
func pow2(e int) []byte {

	res := []byte{byte(1 << uint(e%8))}

	res = append(res, make([]byte, e/8)...)

	return res
}

func min(a, b int) int {
	if a > b {
		return b
	}

	return a
}

//mod2 return result of n mod 2^e in byte
//		n mod 2^e = n & ( 1<<k- 1 )
//assume n's length in bit is bigger than 2^e's
func mod2(n []byte, e int) []byte {

	res := make([]byte, len(n))
	copy(res, n)

	mod := sub(pow2(e), []byte{0x01})

	diff := len(n) - len(mod)
	for i := len(res) - 1; i >= 0; i-- {

		if i-diff < 0 {
			res[i] = 0x00
		} else {
			res[i] &= mod[i-diff]
		}

	}

	return res
}

//isSameNode tell if a and b is the same node
func isSameNode(a, b *NodeInfo) bool {

	if a.Addr == b.Addr { //do not check ID now
		return true
	}

	return false
}

//randInt return random number in [min , max]
func randInt(min, max int) int {

	seed := int64(0)
	binary.Read(crand.Reader, binary.LittleEndian, &seed)

	rand.Seed(seed)

	res := min
	res = rand.Intn(max-min+1) + min

	return res

}

//interval type
const (
	//(a , b)
	intervUnbounded = iota
	//[a , b)
	intervLBounded
	//(a , b]
	intervRBounded
	//[a , b]
	intervBounded
)

func isBetween(val, start, end []byte, intervalType int) bool {

	switch intervalType {
	case intervUnbounded: //(a , b)

		//ring case , start > end
		if compare(start, end) == greater {
			if compare(val, start) == greater || compare(val, end) == less {
				return true
			}
			return false
		}

		// start < end
		if compare(val, start) == greater && compare(val, end) == less {
			return true
		}
		return false
	case intervLBounded: //[a , b)

		//ring case , start > end
		if compare(start, end) == greater {
			if compare(val, start) != less || compare(val, end) == less {
				return true
			}
			return false
		}

		// start < end
		if compare(val, start) != less && compare(val, end) == less {
			return true
		}
		return false
	case intervRBounded: //(a , b]

		//ring case , start > end
		if compare(start, end) == greater {
			if compare(val, start) == greater || compare(val, end) != greater {
				return true
			}
			return false
		}

		// start < end
		if compare(val, start) == greater && compare(val, end) != greater {
			return true
		}
		return false

	case intervBounded: //[a , b]

		//ring case , start > end
		if compare(start, end) == greater {
			if compare(val, start) != less || compare(val, end) != greater {
				return true
			}
			return false
		}

		// start < end
		if compare(val, start) != less && compare(val, end) != greater {
			return true
		}
		return false
	default:
		panic("unexpected interval type")
	}

}
