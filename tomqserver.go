package main

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"
	"tomqserver/src/mq"
	easytcp "tomqserver/src/server"
	"tomqserver/src/web"
)

var sessions *SessionManager

var qc mq.QueuesControl

func init() {
	qc = mq.InitQueuesControl()
	sessions = &SessionManager{
		nextId:  1,
		storage: map[int64]easytcp.Session{},
	}
}

type SessionManager struct {
	nextId  int64
	lock    sync.Mutex
	storage map[int64]easytcp.Session
}

func main() {

	// Create a new server with options.
	s := easytcp.NewServer(&easytcp.ServerOption{
		Packer: easytcp.NewDefaultPacker(), // use default packer
		Codec:  nil,                        // don't use codec
	})
	qc.SetServerInstance(s)

	err := qc.NewQueue("general")
	if err != nil {
		log.Println("error on second")
	}

	s.AddRoute(ConsumerRegisterTcpReq, RegisterConsumer)
	s.AddRoute(MsgPublishTcpReq, PublishMsg)
	s.AddRoute(MsgAckTcpReq, AckMessage)
	s.AddRoute(MsgNAckTcpReq, NAckMessage)
	s.AddRoute(MsgRejectTcpReq, RejectMessage)

	s.OnSessionCreate = func(sess easytcp.Session) {
		// store session
		sessions.lock.Lock()
		defer sessions.lock.Unlock()
		sess.SetID(sessions.nextId)
		sessions.nextId++
		sessions.storage[sess.ID().(int64)] = sess
	}

	s.OnSessionClose = func(sess easytcp.Session) {
		// remove session
		delete(sessions.storage, sess.ID().(int64))
		qc.UnregisterConsumer(fmt.Sprint(sess.ID()))
	}

	go Distribute()
	go web.Serve(&qc)

	// Listen and serve.
	if err := s.Serve(":5896"); err != nil && err != easytcp.ErrServerStopped {
		fmt.Println("serve error: ", err.Error())
	}

}

func Distribute() {
	fmt.Println("Starting distribution")
	// wait for server
	time.Sleep(5000 * time.Millisecond)
	for {
		for _, queueName := range qc.List() {
			//fmt.Println("Checking queue", queueName)
			queue, err := qc.GetQueue(queueName)
			if err != nil {
				continue
			}
			queue.CheckDistributeTime()

			for _, consumer := range queue.ListConsumers() {
				msg, err2 := queue.GetQMessage()
				if err2 != nil {
					continue
				}
				//fmt.Println("found message", msg.Data)
				if consumer.Status() == mq.CONSUMER_STATUS_IDLE {
					respMsg := easytcp.NewTcpMessage(MsgDistributeTcpReq, msg.TcpData())
					targetSession := consumer.Session()
					msg.SetDistributed()
					go func() {
						fmt.Println("SENDING TO CONSUMER", targetSession.ID(), string(respMsg.Data()))
						targetSession.AllocateContext().SetResponseTcpMessage(respMsg).Send()
					}()
					queue.UpdateConsumer(fmt.Sprint(consumer.Session().ID()), mq.CONSUMER_STATUS_WAITING_NACK)
					continue
				}
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func RegisterConsumer(c easytcp.Context) {
	// acquire request
	req := c.Request()

	// do things...

	s := c.Session()
	consumer := mq.NewConsumer(s)
	channelName := bytes.ToUpper(bytes.TrimSpace(req.Data()))
	queue, _ := qc.GetOrCreate(string(channelName)) // get or create
	err := queue.RegisterConsumer(*consumer)
	if err != nil {
		c.SetResponseTcpMessage(easytcp.NewTcpMessage(ConsumerRegisterTcpAck, []byte("error")))
		return
	}
	sid := fmt.Sprint(s.ID())
	fmt.Println("[server] consumer registered: ", sid)
	// set response
	c.SetResponseTcpMessage(easytcp.NewTcpMessage(ConsumerRegisterTcpAck, []byte(sid)))
}

func PublishMsg(c easytcp.Context) {
	// acquire request
	req := c.Request()
	channel := bytes.ToUpper(bytes.TrimSpace(bytes.Split(req.Data(), []byte(" "))[0]))
	data := bytes.TrimSpace(bytes.Split(req.Data(), []byte(" "))[1])
	queue, _ := qc.GetOrCreate(string(channel)) // get or create
	// do things...
	fmt.Println("[server] new TcpMessage for channel " + string(channel))
	msg := mq.NewMessage(string(channel), data)
	err := queue.Publish(msg)
	if err != nil {
		c.SetResponseTcpMessage(easytcp.NewTcpMessage(MsgPublishTcpAck, []byte("error")))
		return
	}
	// set response
	c.SetResponseTcpMessage(easytcp.NewTcpMessage(MsgPublishTcpAck, []byte(msg.Id)))
}

func NAckMessage(c easytcp.Context) {
	// acquire request
	req := c.Request()
	channel := bytes.ToUpper(bytes.TrimSpace(bytes.Split(req.Data(), []byte(" "))[0]))
	msgId := bytes.TrimSpace(bytes.Split(req.Data(), []byte(" "))[1])
	queue, err := qc.GetQueue(string(channel)) // get or create
	if err != nil {
		c.SetResponseTcpMessage(easytcp.NewTcpMessage(MsgNAckTcpAck, []byte(err.Error())))
		return
	}
	err = queue.UnAck(string(msgId))
	queue.UpdateConsumer(fmt.Sprint(c.Session().ID()), mq.CONSUMER_STATUS_WORKING)
	if err != nil {
		c.SetResponseTcpMessage(easytcp.NewTcpMessage(MsgNAckTcpAck, []byte("error")))
		return
	}
	// set response
	c.SetResponseTcpMessage(easytcp.NewTcpMessage(MsgNAckTcpAck, []byte("OK")))

}

func AckMessage(c easytcp.Context) {
	// acquire request
	req := c.Request()
	channel := bytes.ToUpper(bytes.TrimSpace(bytes.Split(req.Data(), []byte(" "))[0]))
	msgId := bytes.TrimSpace(bytes.Split(req.Data(), []byte(" "))[1])
	queue, err := qc.GetQueue(string(channel)) // get or create
	if err != nil {
		c.SetResponseTcpMessage(easytcp.NewTcpMessage(MsgAckTcpAck, []byte(err.Error())))
		return
	}
	err = queue.Ack(string(msgId))
	queue.UpdateConsumer(fmt.Sprint(c.Session().ID()), mq.CONSUMER_STATUS_IDLE)
	if err != nil {
		c.SetResponseTcpMessage(easytcp.NewTcpMessage(MsgAckTcpAck, []byte("error")))
		return
	}

	// set response
	c.SetResponseTcpMessage(easytcp.NewTcpMessage(MsgAckTcpAck, []byte("OK")))
}

func RejectMessage(c easytcp.Context) {
	// acquire request
	req := c.Request()
	channel := bytes.ToUpper(bytes.TrimSpace(bytes.Split(req.Data(), []byte(" "))[0]))
	msgId := bytes.TrimSpace(bytes.Split(req.Data(), []byte(" "))[1])
	queue, err := qc.GetQueue(string(channel)) // get or create
	if err != nil {
		c.SetResponseTcpMessage(easytcp.NewTcpMessage(MsgRejectTcpAck, []byte(err.Error())))
		return
	}
	err = queue.Reject(string(msgId))
	queue.UpdateConsumer(fmt.Sprint(c.Session().ID()), mq.CONSUMER_STATUS_IDLE)
	if err != nil {
		c.SetResponseTcpMessage(easytcp.NewTcpMessage(MsgRejectTcpAck, []byte("error")))
		return
	}

	// set response
	c.SetResponseTcpMessage(easytcp.NewTcpMessage(MsgRejectTcpAck, []byte("OK")))
}
