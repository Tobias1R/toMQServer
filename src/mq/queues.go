package mq

import (
	"errors"
	"strings"
	"sync"
	"tomqserver/src/server"
)

type queuesControl struct {
	queues    map[string]*queue
	m         sync.Mutex
	tcpServer *server.Server
}

type QueuesControl interface {
	NewQueue(queueName string) error
	Delete(queueName string) error
	GetOrCreate(queueName string) (*queue, error)
	GetQueue(queueName string) (*queue, error)
	List() []string
	UnregisterConsumer(sid string)
	ServerInfo() webInfo
	SetServerInstance(s *server.Server)
}

func (qc *queuesControl) SetServerInstance(s *server.Server) {
	qc.tcpServer = s
}
func (qc *queuesControl) UnregisterConsumer(sid string) {

	for _, q := range qc.queues {
		q.UnregisterConsumer(sid)
	}
}

func (qc *queuesControl) List() []string {
	keys := make([]string, len(qc.queues))

	i := 0
	for k := range qc.queues {
		keys[i] = k
		i++
	}
	return keys
}

func (qc *queuesControl) GetQueue(queueName string) (*queue, error) {
	qc.m.Lock()
	defer qc.m.Unlock()
	queueName = strings.ToUpper(queueName)
	if q, ok := qc.queues[queueName]; ok {
		return q, nil
	}

	return nil, errors.New("queue doesn't exists")
}

func (qc *queuesControl) GetOrCreate(queueName string) (*queue, error) {
	qc.m.Lock()
	defer qc.m.Unlock()
	queueName = strings.ToUpper(queueName)
	if q, ok := qc.queues[queueName]; ok {
		return q, nil
	}
	qc.queues[queueName] = newQueue(queueName)
	q := qc.queues[queueName]
	return q, nil
}

func InitQueuesControl() *queuesControl {
	q := &queuesControl{
		queues: map[string]*queue{},
		m:      sync.Mutex{},
	}
	return q
}

func (qc *queuesControl) NewQueue(queueName string) error {
	qc.m.Lock()
	defer qc.m.Unlock()
	queueName = strings.ToUpper(queueName)
	if _, ok := qc.queues[queueName]; ok {
		return errors.New("Queue already exists")
	}
	qc.queues[queueName] = newQueue(queueName)
	return nil
}

func (qc *queuesControl) Delete(queueName string) error {
	qc.m.Lock()
	defer qc.m.Unlock()
	queueName = strings.ToUpper(queueName)
	if _, ok := qc.queues[queueName]; ok {
		return errors.New("Queue already exists")
	}
	qc.queues[queueName] = newQueue(queueName)
	return nil
}
