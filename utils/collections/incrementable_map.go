package collections

import (
	"sync"
	"sync/atomic"
)

type IncrementableMap[K comparable] struct {
	entries map[K]*int64
	mutex   sync.Mutex
}

func NewIncrementableMap[K comparable]() IncrementableMap[K] {
	return IncrementableMap[K]{
		entries: make(map[K]*int64),
	}
}

func (m *IncrementableMap[K]) Contains(k K) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	_, ok := m.entries[k]
	return ok
}

func (m *IncrementableMap[K]) Put(k K, v V, forced bool) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if forced {
		m.entries[k] = v
		return nil
	}
	if m.Contains(k) {
		return ErrValueExisted
	}
	m.entries[k] = v
	return nil
}

func (m *IncrementableMap[K]) Get(k K) (v V, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	v, ok := m.entries[k]
	if !ok {
		return v, ErrValueNotExisted
	}
	return v, nil
}

func (m *IncrementableMap[K]) Delete(k K) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if !m.Contains(k) {
		return ErrValueNotExisted
	}
	delete(m.entries, k)
	return nil
}

func (m *IncrementableMap[K]) Size() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return len(m.entries)
}

func (m *IncrementableMap[K]) Keys() []K {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	arr := make([]K, 0, m.Size())
	for k := range m.entries {
		arr = append(arr, k)
	}
	return arr
}

func (m *IncrementableMap[K]) Values() []*int64 {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	arr := make([]*int64, 0, m.Size())
	for _, v := range m.entries {
		arr = append(arr, v)
	}
	return arr
}

func (m *IncrementableMap[K]) IncrementValueInt64(k K, incrementBy int64) int64 {
	return atomic.AddInt64(m.entries[k], incrementBy)
}
