package chord

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

//subID returns result of ID subtracts n
//assume ID's length  is bigger than n's
func subID(ID, n []byte) []byte {

	nLen, idLen := len(n), len(ID)
	diff := idLen - nLen
	if diff < 0 {
		panic("overflow")
	}

	res := make([]byte, idLen)
	copy(res, ID)

	flag := byte(0x00)
	for i := idLen - 1; i >= 0; i-- {

		if ID[i] < flag { //ID[i]-flag overflow
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

	if flag != 0x00 {
		panic("overflow")
	}

	return res
}

//addID returns result of ID and n
//assume ID's length  is bigger than n's
func addID(ID, n []byte) []byte {

	nLen, idLen := len(n), len(ID)
	diff := idLen - nLen
	if diff < 0 {
		panic("overflow")
	}

	res := make([]byte, idLen)
	copy(res, ID)

	flag := byte(0x00)
	for i := idLen - 1; i >= 0; i-- {

		if math.MaxUint8-ID[i] < flag { // ID[i]+flag overflow
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

	mod := subID(pow2(e), []byte{0x01})

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
