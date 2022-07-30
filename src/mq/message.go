package mq

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"
	"tomqserver/src/utils"

	"github.com/google/uuid"
)

const (
	STATUS_MESSAGE_RECEIVED     = 0
	STATUS_MESSAGE_READY        = 1
	STATUS_MESSAGE_UNACK        = 2
	STATUS_MESSAGE_ACK          = 3
	STATUS_MESSAGE_REJECTED     = 4
	STATUS_MESSAGE_WAITING_NACK = 5
)

var StatusName = map[int]string{
	STATUS_MESSAGE_RECEIVED: "RECEIVED",
	STATUS_MESSAGE_ACK:      "ACKNOWLEDGED",
	STATUS_MESSAGE_READY:    "READY",
	STATUS_MESSAGE_REJECTED: "REJECTED",
	STATUS_MESSAGE_UNACK:    "UNACKNOWLEDGED",
}

type Header struct {
	Channel   string `json:"channel,omitempty"`
	Size      int    `json:"size,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
}

type QMessage struct {
	Header Header `json:"header,omitempty"`
	Id     string `json:"id"`
	Data   []byte `json:"data,omitempty"`
	Status int    `json:"status,omitempty"`
}

type IMessage interface {
	Marshal() ([]byte, error)
	Line() []byte
}

func (m *QMessage) Marshal() ([]byte, error) {

	return json.Marshal(&struct {
		Id     string `json:"id"`
		Data   string `json:"data,omitempty"`
		Status int    `json:"status,omitempty"`
	}{
		Id:     m.Id,
		Data:   string(m.Data),
		Status: m.Status,
	})
}

func (m *QMessage) ShowStatus() string {
	return StatusName[m.Status]
}

func (m *QMessage) SetDistributed() {
	m.Status = STATUS_MESSAGE_WAITING_NACK
	m.Header.Timestamp = time.Now().UnixNano()
}

func NewMessage(channel string, data []byte) QMessage {
	newId := utils.HashDigestString(uuid.New().String())

	msg := QMessage{
		Header: Header{
			Channel:   channel,
			Size:      len(data),
			Timestamp: time.Now().UnixNano(),
		},
		Id:     newId, //,
		Data:   data,
		Status: STATUS_MESSAGE_READY,
	}
	return msg
}

func (m *QMessage) TcpData() []byte {
	data := []byte(m.Id)
	data = append(data, []byte(" ")...)
	data = append(data, []byte(base64.StdEncoding.EncodeToString(m.Data))...)
	return data
}

func (m *QMessage) Line() []byte {
	var line []byte
	dataSize := fmt.Sprintf("%09d", len(m.Data))

	id := []byte(m.Id)
	timestamp := make([]byte, 18)
	binary.LittleEndian.PutUint64(timestamp, uint64(m.Header.Timestamp))
	line = append(line, []byte(dataSize)...)
	line = append(line, []byte(strconv.Itoa(int(m.Header.Timestamp)))...)
	line = append(line, []byte(strconv.Itoa(m.Status))...)
	line = append(line, id...)
	line = append(line, m.Data...)
	//println("LINE   ", string(line))
	//println("LINESTR", lineStr(line))
	return line
}

func LineSS(m QMessage) {
	var line []byte

	id := []byte(m.Id)
	timestamp := make([]byte, 18)
	binary.LittleEndian.PutUint64(timestamp, uint64(m.Header.Timestamp))
	line = append(line, []byte(strconv.Itoa(int(m.Header.Timestamp)))...)
	line = append(line, byte(m.Status))
	line = append(line, id...)
	line = append(line, m.Data...)
	println("LINESS ", string(line))

}

func lineStr(data []byte) string {
	var line string
	dataSize := data[:9]
	log.Println("DATASIZE", string(dataSize))
	timestamp := data[9:28]
	log.Println("TIME", string(timestamp))
	status := data[29:30]
	msgId := data[29:57]
	log.Println("ID", string(msgId))
	body := data[57:]
	line += string(dataSize)
	line += string(timestamp)
	line += string(status)
	line += string(msgId)
	line += string(body)
	// statusInt, _ := strconv.Atoi(string(status))
	// println("STATUS STR", StatusName[statusInt])
	msg2, err := ReadMessage(data, "general", 0)
	if err != nil {
		return err.Error()
	}
	LineSS(msg2)
	return line
}

func ReadMessage(data []byte, channel string, bytePos int) (QMessage, error) {
	headerSize := 57

	dataSizeBytes := 9
	timestampBytes := 18
	statusBytes := 1
	//msgIdBytes := 28
	dataSize, err := strconv.Atoi(string(data[:9]))
	bytePos = bytePos + dataSizeBytes
	if err != nil {
		return QMessage{}, errors.New("bad data size " + err.Error())
	}

	ts, err := strconv.ParseInt(string(data[9:28]), 10, 64)
	bytePos = bytePos + timestampBytes
	if err != nil {
		return QMessage{}, errors.New("bad timestamp " + err.Error())
	}
	if err != nil {
		return QMessage{}, errors.New("bad timestamp")
	}

	var st []byte = data[28:29]
	bytePos += statusBytes
	byteToInt, _ := strconv.Atoi(string(st))
	if len(data) <= 2 {
		return QMessage{}, errors.New("no data to parse")
	}
	body := data[57 : headerSize+dataSize]
	m := QMessage{
		Header: Header{
			Channel:   channel,
			Size:      0,
			Timestamp: ts,
		},
		Id:     string(data[29:57]),
		Data:   body,
		Status: byteToInt,
	}
	return m, nil
}
