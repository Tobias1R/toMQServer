package mq

import (
	"tomqserver/src/server"
)

const (
	CONSUMER_STATUS_IDLE         = 0
	CONSUMER_STATUS_WORKING      = 1
	CONSUMER_STATUS_WAITING_NACK = 2
)

type consumer struct {
	session     server.Session
	status      int
	lastMessage int64
}

func NewConsumer(s server.Session) *consumer {
	c := consumer{
		session:     s,
		status:      CONSUMER_STATUS_IDLE,
		lastMessage: 0,
	}
	return &c
}

func (c *consumer) Session() server.Session {
	return c.session
}
func (c *consumer) Status() int {
	return c.status
}
func (c *consumer) SetStatus(s int) {
	c.status = s
}
