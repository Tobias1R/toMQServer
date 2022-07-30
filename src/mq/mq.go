package mq

import (
	"container/list"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
	"tomqserver/config"
	"tomqserver/src/utils"
)

type queue struct {
	name          string
	consumers     []*consumer
	publishers    *list.List
	messagesOrder []string
	storage       map[string]*QMessage
	m             sync.Mutex
}

type Queue interface {
	Add(QMessage QMessage) error
	Ack(QMessageId string) error
	UnAck(QMessageId string) error
	Reject(QMessageId string) error
	GetQMessage() QMessage
	NewQueue(name string) (queue, error)
	RegisterConsumer(c consumer) error
	Publish(QMessage QMessage) error
	Persist() error
	ReadPersistence() error
	ListConsumers() []consumer
	UpdateConsumer(sid string, status int)
	UnregisterConsumer(sid string)
	CheckDistributeTime()
	GetStorageByteSize() int64
}

func (q *queue) GetStorageByteSize() int64 {
	var size int64 = 8
	for i, m := range q.storage {
		size += int64(len(i))
		size += int64(len(m.Data))
		size += int64(len(m.Id))
		size += int64(len(m.Header.Channel))
		size += 32
	}
	return size
}

func (q *queue) CheckDistributeTime() {
	q.m.Lock()
	defer q.m.Unlock()
	maxTime := config.MAX_TIME_DISTRIBUTED_ACK * 1000000000
	now := time.Now().UnixNano()
	for id, msg := range q.storage {
		if msg.Status == STATUS_MESSAGE_WAITING_NACK {
			diff := now - msg.Header.Timestamp
			if diff >= maxTime {
				// acknowledgment expired
				log.Println("Message ack expired")
				msg.Status = STATUS_MESSAGE_READY
				q.storage[id] = msg
			}
		}
	}
}

func (q *queue) UpdateConsumer(sid string, status int) {
	q.m.Lock()
	defer q.m.Unlock()
	for _, consumer := range q.consumers {
		if sid == fmt.Sprint(consumer.session.ID()) {
			consumer.status = status
			//log.Println("CONSUMER STATUS UPDATE", consumer.session.ID(), consumer.status)
			break
		}
	}
}

func (q *queue) UnregisterConsumer(sid string) {
	q.m.Lock()
	defer q.m.Unlock()
	for _, consumer := range q.consumers {
		if sid == fmt.Sprint(consumer.session.ID()) {
			consumer.status = 3
			fmt.Println("consumer unregistered?")
			break
		}
	}
}

func (q *queue) ListConsumers() []*consumer {
	return q.consumers
}

func (q *queue) RegisterConsumer(c consumer) error {
	q.consumers = append(q.consumers, &c)
	return nil
}

func (q *queue) Persist() error {

	dataPath := "/home/ozy/GO/toMQServer/data/"
	fileName := dataPath + q.name + ".mq"
	if err := Save(fileName, q); err != nil {
		log.Fatalln(err)
	}
	f, err := os.Open(fileName)
	f.Close()
	if err != nil {
		log.Println("OPEN ERR", err)
		f2, err2 := os.Create(fileName)
		f2.Close()
		if err2 != nil {
			return err2
		}
		log.Println("File", fileName, "created!")
	}
	for _, msg := range q.storage {
		if msg.Status != STATUS_MESSAGE_ACK && msg.Status != STATUS_MESSAGE_REJECTED {
			data := msg.Line()
			WriteFile(fileName, data)
		}
	}

	return nil
}

func (q *queue) TotalMesssages() int {
	return len(q.messagesOrder)
}

func (q *queue) GetQMessage() (*QMessage, error) {
	q.m.Lock()
	defer q.m.Unlock()
	for _, msgId := range q.messagesOrder {
		if msg, ok := q.storage[msgId]; ok {
			if msg.Status == STATUS_MESSAGE_READY {
				//msg.Status = STATUS_MESSAGE_UNACK
				q.storage[msgId] = msg
				q.Persist()
				return msg, nil
			}
		}
	}

	return &QMessage{}, errors.New("no new QMessages")
}

func newQueue(name string) *queue {
	nq := make(map[string]*QMessage)
	q := queue{
		name:          name,
		consumers:     []*consumer{},
		publishers:    list.New(),
		messagesOrder: []string{},
		storage:       nq,
	}

	f, err := os.OpenFile(config.DATA_DIR+"/"+name+".mq", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		f, err = os.Create(config.DATA_DIR + "/" + name + ".mq")
		if err != nil {
			log.Fatal(err)
		}
	}
	f.Close()
	//q.ReadFile()

	return &q
}

func (q *queue) Publish(msg QMessage) error {
	q.m.Lock()
	defer q.m.Unlock()
	q.messagesOrder = append(q.messagesOrder, msg.Id)
	msg.Status = STATUS_MESSAGE_READY
	q.storage[msg.Id] = &msg
	q.Persist()
	return nil
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func (q *queue) Ack(QMessageId string) error {
	q.m.Lock()
	defer q.m.Unlock()
	log.Println("[MQ] ACKING", QMessageId)
	if msg, ok := q.storage[QMessageId]; ok {
		msg.Status = STATUS_MESSAGE_ACK
		delete(q.storage, QMessageId) // = msg
		q.messagesOrder = utils.RemoveStringFromSlice(q.messagesOrder, QMessageId)
		log.Println("[MQ] QMessage acknowledged")
	} else {
		log.Println("[MQ] QMessage not found", QMessageId)
		if stringInSlice(QMessageId, q.messagesOrder) {
			log.Println("[MQ] Interesting fact!")
		}
		return errors.New("QMessage not found")
	}
	q.Persist()
	return nil
}

func (q *queue) UnAck(QMessageId string) error {
	q.m.Lock()
	defer q.m.Unlock()
	log.Println("[MQ] UNACKING", QMessageId)
	if msg, ok := q.storage[QMessageId]; ok {
		msg.Status = STATUS_MESSAGE_UNACK
		q.storage[QMessageId] = msg
		log.Println("[MQ] QMessage working")
	} else {
		log.Println("[MQ] QMessage not found", QMessageId)
		if stringInSlice(QMessageId, q.messagesOrder) {
			log.Println("[MQ] Interesting fact!")
		}
		return errors.New("QMessage not found")
	}
	q.Persist()
	return nil
}

func (q *queue) Reject(QMessageId string) error {
	q.m.Lock()
	defer q.m.Unlock()
	log.Println("[MQ] REJECTING", QMessageId)
	if msg, ok := q.storage[QMessageId]; ok {
		msg.Status = STATUS_MESSAGE_REJECTED
		q.storage[QMessageId] = msg
		q.messagesOrder = utils.RemoveStringFromSlice(q.messagesOrder, QMessageId)
		log.Println("[MQ] QMessage rejected")
	} else {
		log.Println("[MQ] QMessage not found", QMessageId)
		if stringInSlice(QMessageId, q.messagesOrder) {
			log.Println("[MQ] Interesting fact!")
		}
	}
	q.Persist()
	return nil
}
