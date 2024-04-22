/*
package collections

type hashMap[K comparable, V any] struct {
	entries map[K]V
}

func NewHashMap[K comparable, V any]() Map[K, V] {
	return &hashMap[K, V]{
		entries: make(map[K]V),
	}
}

func (m *hashMap[K, V]) Contains(k K) bool {
	if _, ok := m.entries[k]; ok {
		return true
	}
	return false
}

func (m *hashMap[K, V]) Put(k K, v V, forced bool) error {
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

func (m *hashMap[K, V]) Get(k K) (v V, err error) {
	if !m.Contains(k) {
		return v, ErrValueNotExisted
	}
	return m.entries[k], nil
}

func (m *hashMap[K, V]) Delete(k K) error {
	if !m.Contains(k) {
		return ErrValueNotExisted
	}
	delete(m.entries, k)
	return nil
}

func (m *hashMap[K, V]) Size() int {
	return len(m.entries)
}

func (m *hashMap[K, V]) Keys() []K {
	arr := make([]K, 0, m.Size())
	for k := range m.entries {
		arr = append(arr, k)
	}
	return arr
}

func (m *hashMap[K, V]) Values() []V {
	arr := make([]V, 0, m.Size())
	for _, v := range m.entries {
		arr = append(arr, v)
	}
	return arr
}
*/

package collections

import "sync"

type syncMap[K comparable, V any] struct {
	entries sync.Map
}

func NewHashMap[K comparable, V any]() Map[K, V] {
	return &syncMap[K, V]{
		entries: sync.Map{},
	}
}

func (m *syncMap[K, V]) Contains(k K) bool {
	_, ok := m.entries.Load(k)
	return ok
}

func (m *syncMap[K, V]) Put(k K, v V, forced bool) error {
	if forced {
		m.entries.Store(k, v)
		return nil
	}
	if m.Contains(k) {
		return ErrValueExisted
	}
	m.entries.Store(k, v)
	return nil
}

func (m *syncMap[K, V]) Get(k K) (v V, err error) {
	val, ok := m.entries.Load(k)
	if !ok {
		return v, ErrValueNotExisted
	}
	return val.(V), nil
}

func (m *syncMap[K, V]) Delete(k K) error {
	m.entries.Delete(k)
	return nil
}

func (m *syncMap[K, V]) Size() int {
	size := 0
	m.entries.Range(func(_, _ interface{}) bool {
		size++
		return true
	})
	return size
}

func (m *syncMap[K, V]) Keys() []K {
	keys := make([]K, 0, m.Size())
	m.entries.Range(func(key, _ interface{}) bool {
		keys = append(keys, key.(K))
		return true
	})
	return keys
}

func (m *syncMap[K, V]) Values() []V {
	values := make([]V, 0, m.Size())
	m.entries.Range(func(_, value interface{}) bool {
		values = append(values, value.(V))
		return true
	})
	return values
}
