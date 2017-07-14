package chord

import (
	"reflect"
	"testing"
)

func Test_subID(t *testing.T) {
	type args struct {
		a []byte
		b int
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{"#0", args{a: []byte{0x00, 0x00, 0xab, 0xcd}, b: 1}, []byte{0x00, 0x00, 0xab, 0xcc}},
		{"#1", args{a: []byte{0x00, 0x00, 0xab, 0xcd}, b: 256}, []byte{0x00, 0x00, 0xaa, 0xcd}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := subID(tt.args.a, tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("subID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_compareID(t *testing.T) {
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
			if got := compareID(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("compareID() = %v, want %v", got, tt.want)
			}
		})
	}
}
