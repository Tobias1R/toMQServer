package server

import (
	"fmt"
	"sync"
)

// NewTcpMessage creates a TcpMessage pointer.
func NewTcpMessage(id interface{}, data []byte) *TcpMessage {
	return &TcpMessage{
		id:   id,
		data: data,
	}
}

// TcpMessage is the abstract of inbound and outbound TcpMessage.
type TcpMessage struct {
	id      interface{}
	data    []byte
	storage map[string]interface{}
	mu      sync.RWMutex
}

// ID returns the id of current TcpMessage.
func (m *TcpMessage) ID() interface{} {
	return m.id
}

// Data returns the data part of current TcpMessage.
func (m *TcpMessage) Data() []byte {
	return m.data
}

// Set stores kv pair.
func (m *TcpMessage) Set(key string, value interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.storage == nil {
		m.storage = make(map[string]interface{})
	}
	m.storage[key] = value
}

// Get retrieves the value according to the key.
func (m *TcpMessage) Get(key string) (value interface{}, exists bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, exists = m.storage[key]
	return
}

// MustGet retrieves the value according to the key.
// Panics if key does not exist.
func (m *TcpMessage) MustGet(key string) interface{} {
	if v, ok := m.Get(key); ok {
		return v
	}
	panic(fmt.Errorf("key `%s` does not exist", key))
}

// Remove deletes the key from storage.
func (m *TcpMessage) Remove(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.storage, key)
}
