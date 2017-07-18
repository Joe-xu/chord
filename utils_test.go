package chord

import (
	"reflect"
	"testing"
)

func Test_sub(t *testing.T) {
	type args struct {
		ID []byte
		n  []byte
	}

	tests := []struct {
		name string
		args args
		want []byte
	}{
		{"#0", args{ID: []byte{0x00, 0x00, 0xab, 0xcd}, n: []byte{0x01}}, []byte{0x00, 0x00, 0xab, 0xcc}},
		{"#1", args{ID: []byte{0x00, 0x00, 0xab, 0xcd}, n: []byte{0x01, 0x00}}, []byte{0x00, 0x00, 0xaa, 0xcd}},
		{"#3", args{ID: []byte{0xab, 0xcd, 0x00, 0x00, 0x00, 0x00}, n: []byte{0xff, 0xff, 0xff, 0xff}}, []byte{0xab, 0xcc, 0x00, 0x00, 0x00, 0x01}},
		{"#4", args{ID: []byte{0xab, 0x00, 0x00, 0x00, 0x00, 0x00}, n: []byte{0xff, 0xff, 0xff, 0xff}}, []byte{0xaa, 0xff, 0x00, 0x00, 0x00, 0x01}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sub(tt.args.ID, tt.args.n); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("subID() = % x, want % x", got, tt.want)
			}
		})
	}
}

func Test_compare(t *testing.T) {
	type args struct {
		a []byte
		b []byte
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{"#0", args{a: []byte{0x00, 0x00, 0xab, 0xcd}, b: []byte{0x00, 0x00, 0xab, 0xcd}}, equal},
		{"#1", args{a: []byte{0x00, 0x00, 0xab, 0xcd}, b: []byte{0x00, 0x00, 0xab, 0xce}}, less},
		{"#2", args{a: []byte{0x00, 0x00, 0xab, 0xcd}, b: []byte{0x00, 0x00, 0xab, 0xcc}}, greater},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compare(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("compareID() = % x, want % x", got, tt.want)
			}
		})
	}
}

func Test_add(t *testing.T) {
	type args struct {
		ID []byte
		n  []byte
	}

	tests := []struct {
		name string
		args args
		want []byte
	}{
		{"#0", args{ID: []byte{0x00, 0x00, 0xab, 0xcc}, n: []byte{0x01}}, []byte{0x00, 0x00, 0xab, 0xcd}},
		{"#1", args{ID: []byte{0x00, 0x00, 0xaa, 0xcd}, n: []byte{0x01, 0x00}}, []byte{0x00, 0x00, 0xab, 0xcd}},
		{"#3", args{ID: []byte{0xab, 0xcc, 0x00, 0x00, 0x00, 0x01}, n: []byte{0xff, 0xff, 0xff, 0xff}}, []byte{0xab, 0xcd, 0x00, 0x00, 0x00, 0x00}},
		{"#4", args{ID: []byte{0xaa, 0xff, 0x00, 0x00, 0x00, 0x01}, n: []byte{0xff, 0xff, 0xff, 0xff}}, []byte{0xab, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := add(tt.args.ID, tt.args.n); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("addID() = % x, want % x", got, tt.want)
			}
		})
	}
}

func Test_pow2(t *testing.T) {
	type args struct {
		e int
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{"#0", args{0}, []byte{0x01}},
		{"#1", args{1}, []byte{0x02}},
		{"#2", args{8}, []byte{0x01, 0x00}},
		{"#3", args{12}, []byte{0x10, 0x00}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pow2(tt.args.e); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("pow2() = % x, want % x", got, tt.want)
			}
		})
	}
}

func Test_mod2(t *testing.T) {
	type args struct {
		n []byte
		e int
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{"#0", args{n: []byte{0x01}, e: 1}, []byte{0x01}},
		{"#1", args{n: []byte{0x03}, e: 1}, []byte{0x01}},
		{"#2", args{n: []byte{0xff, 0xff}, e: 4}, []byte{0x00, 0x0f}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mod2(tt.args.n, tt.args.e); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mod2() = % x, want % x", got, tt.want)
			}
		})
	}
}

func Test_isBetween(t *testing.T) {
	type args struct {
		val, start, end []byte
		intervalType    int
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"Unbounded#0", args{val: []byte{0x00}, start: []byte{0x00}, end: []byte{0xf0}, intervalType: intervUnbounded}, false},
		{"Unbounded#1", args{val: []byte{0xf0}, start: []byte{0x00}, end: []byte{0xf0}, intervalType: intervUnbounded}, false},
		{"L-Bounded#0", args{val: []byte{0xf0}, start: []byte{0x00}, end: []byte{0xf0}, intervalType: intervLBounded}, false},
		{"L-Bounded#1", args{val: []byte{0x00}, start: []byte{0x00}, end: []byte{0xf0}, intervalType: intervLBounded}, true},
		{"R-Bounded#0", args{val: []byte{0x00}, start: []byte{0x00}, end: []byte{0xf0}, intervalType: intervRBounded}, false},
		{"R-Bounded#1", args{val: []byte{0xf0}, start: []byte{0x00}, end: []byte{0xf0}, intervalType: intervRBounded}, true},
		{"Bounded#0", args{val: []byte{0x00}, start: []byte{0x00}, end: []byte{0xf0}, intervalType: intervBounded}, true},
		{"Bounded#1", args{val: []byte{0xf0}, start: []byte{0x00}, end: []byte{0xf0}, intervalType: intervBounded}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isBetween(tt.args.val, tt.args.start, tt.args.end, tt.args.intervalType); got != tt.want {
				t.Errorf("isBetween() = %t, want %t", got, tt.want)
			}
		})
	}
}
