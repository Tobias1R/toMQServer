package mq

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"tomqserver/config"
)

var lock sync.Mutex

//this type represnts a record with three fields
type payload struct {
	One   float32
	Two   float64
	Three uint32
}

func (q *queue) ReadFile() {
	dataDir := config.DATA_DIR
	file, err := os.Open(dataDir + "/" + q.name[1:] + ".mq")

	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	//headerSize := 57

	dataSizeBytes := 9
	timestampBytes := 18
	statusBytes := 1
	msgIdBytes := 28
	for {
		ds, err := readNextBytes(file, dataSizeBytes)
		if err == io.EOF {
			break
		}
		dataSize, err := strconv.Atoi(string(ds))

		timestamp, err := readNextBytes(file, timestampBytes)
		if err == io.EOF {
			break
		}
		status, err := readNextBytes(file, statusBytes)
		if err == io.EOF {
			break
		}
		msgId, err := readNextBytes(file, msgIdBytes)
		if err == io.EOF {
			break
		}
		body, err := readNextBytes(file, dataSize+1)
		if err == io.EOF {
			break
		}
		var data []byte
		data = append(data, ds...)
		data = append(data, timestamp...)
		data = append(data, status...)
		data = append(data, msgId...)
		data = append(data, body...)
		println("READ DATA", string(data))
		msg, errMsg := ReadMessage(data, q.name, 0)
		if errMsg != nil {
			log.Println("Invalid persisted message")
		}
		println("MSG  DATA", strconv.Itoa(int(msg.Header.Timestamp)), string(msg.Id), msg.ShowStatus(), string(msg.Data))
		q.messagesOrder = append(q.messagesOrder, msg.Id)
		msg.Status = STATUS_MESSAGE_READY
		q.storage[msg.Id] = &msg
	}

}

func readNextBytes(file *os.File, number int) ([]byte, error) {
	bytes := make([]byte, number)

	_, err := file.Read(bytes)
	if err != nil {
		return []byte(""), err
	}

	return bytes, nil
}

func WriteFile(filename string, data []byte) {
	//filename = config.DATA_DIR + "/" + filename + ".mq"
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)

	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	var bin_buf bytes.Buffer
	binary.Write(&bin_buf, binary.BigEndian, &data)
	writeNextBytes(file, bin_buf.Bytes())

}
func writeNextBytes(file *os.File, bytes []byte) {

	_, err := file.Write(bytes)

	if err != nil {
		log.Fatal(err)
	}
	// if n, err = file.WriteString("\n"); err != nil {
	// 	log.Fatal(n, err)
	// }

}

func Seek(file *os.File, key []byte) ([]byte, int) {

	return []byte(""), -1
}

func Load(path string, q *queue) error {
	lock.Lock()
	defer lock.Unlock()
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return nil
}

// Save saves a representation of v to the file at path.
func Save(path string, q *queue) error {
	lock.Lock()
	defer lock.Unlock()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, msgId := range q.messagesOrder {
		if msg, ok := q.storage[msgId]; ok {
			if msg.Status != STATUS_MESSAGE_ACK && msg.Status != STATUS_MESSAGE_REJECTED {
				data := msg.Line()
				WriteFile(path, data)
			}
		}
	}
	return err
}
