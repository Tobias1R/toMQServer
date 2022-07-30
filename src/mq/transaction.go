package mq

import "errors"

/*
Server send messsage start a transaction
tcp ack means consumer received
consumer nack sets step 2
server send confirmation of step 2 (tcp ack)
step 3 is either mq ack or reject
expected tcpMessages order
snd >> 1019 distribute
rec << 1020 consumer informs received distribution
rec << 1003 consumer nack
snd >> 1004 inform consumer the server received nack
rec << 1005 consumer ack
snd >> 1006 inform consumer received ack
*/
type transaction struct {
	id          string
	consumer    int
	step        int
	tcpMessages []int
}

type transactionManager struct {
	storage           map[string]transaction
	totalTransactions int
}

type TransactionManager interface {
	Start(id string, consumer int)
	Update(id string)
}

func NewTM() transactionManager {
	return transactionManager{
		storage:           map[string]transaction{},
		totalTransactions: 0,
	}
}

func (tm *transactionManager) Start(id string, consumer int) {
	t := transaction{
		id:          id,
		consumer:    consumer,
		step:        1,
		tcpMessages: []int{1019},
	}
	tm.storage[id] = t
}

func (tm *transactionManager) Update(id string, tcpMsgId int) error {
	if t, ok := tm.storage[id]; ok {
		t.tcpMessages = append(t.tcpMessages, tcpMsgId)
		if tcpMsgId == 1020 {
			// msg Nack
			t.step = 2
		}
		if tcpMsgId == 1003 {
			// msg Nack
			t.step = 3
		}
		if tcpMsgId == 1003 {
			// msg Nack confirmation sent
			t.step = 4
		}
		if tcpMsgId == 1005 {
			// msg Ack
			t.step = 5
		}
		if tcpMsgId == 1006 {
			// msg Ack
			t.step = 6
		}
		if tcpMsgId == 1007 {
			// msg Reject
			t.step = 5
		}
		if tcpMsgId == 1008 {
			// msg Reject
			t.step = 6
		}
		if t.step < 6 {
			tm.storage[id] = t
		} else {
			delete(tm.storage, id)
			tm.totalTransactions++
		}
	} else {
		return errors.New("not found")
	}

	return nil
}
