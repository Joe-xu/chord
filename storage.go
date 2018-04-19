package chord

import (
	"sync"
)

type storage struct {
	datas sync.Map
}

func (s *storage) Store(key, data string) {
	s.datas.Store(key, data)
}

func (s *storage) Get(key string) string {

	data, ok := s.datas.Load(key)
	if !ok {
		return ""
	}
	return data.(string)
}
