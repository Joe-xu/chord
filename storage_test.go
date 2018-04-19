package chord

import (
	"testing"
)

func Test_storage_Store(t *testing.T) {

	testStore := &storage{}

	type args struct {
		key  string
		data string
	}
	tests := []struct {
		name string
		s    *storage
		args args
	}{
		{"#1", testStore, args{"test_key", "test_data"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s.Store(tt.args.key, tt.args.data)

			if tt.s.Get(tt.args.key) != tt.args.data {
				t.Error("fail")
			}

		})
	}
}
