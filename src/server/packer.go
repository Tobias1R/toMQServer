package server

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/spf13/cast"
)

//go:generate mockgen -destination ./packer_mock.go -package easytcp . Packer

// Packer is a generic interface to pack and unpack TcpMessage packet.
type Packer interface {
	// Pack packs TcpMessage into the packet to be written.
	Pack(msg *TcpMessage) ([]byte, error)

	// Unpack unpacks the TcpMessage packet from reader,
	// returns the TcpMessage, and error if error occurred.
	Unpack(reader io.Reader) (*TcpMessage, error)
}

var _ Packer = &DefaultPacker{}

// NewDefaultPacker create a *DefaultPacker with initial field value.
func NewDefaultPacker() *DefaultPacker {
	return &DefaultPacker{
		MaxDataSize: 1 << 10 << 10, // 1MB
	}
}

// DefaultPacker is the default Packer used in session.
// Treats the packet with the format:
//
// dataSize(4)|id(4)|data(n)
//
// | segment    | type   | size    | remark                  |
// | ---------- | ------ | ------- | ----------------------- |
// | `dataSize` | uint32 | 4       | the size of `data` only |
// | `id`       | uint32 | 4       |                         |
// | `data`     | []byte | dynamic |                         |
// .
type DefaultPacker struct {
	// MaxDataSize represents the max size of `data`
	MaxDataSize int
}

func (d *DefaultPacker) bytesOrder() binary.ByteOrder {
	return binary.LittleEndian
}

// Pack implements the Packer Pack method.
func (d *DefaultPacker) Pack(msg *TcpMessage) ([]byte, error) {
	dataSize := len(msg.Data())
	buffer := make([]byte, 4+4+dataSize)
	d.bytesOrder().PutUint32(buffer[:4], uint32(dataSize)) // write dataSize
	id, err := cast.ToUint32E(msg.ID())
	if err != nil {
		return nil, fmt.Errorf("invalid type of msg.ID: %s", err)
	}
	d.bytesOrder().PutUint32(buffer[4:8], id) // write id
	copy(buffer[8:], msg.Data())              // write data
	return buffer, nil
}

// Unpack implements the Packer Unpack method.
// Unpack returns the TcpMessage whose ID is type of int.
// So we need use int id to register routes.
func (d *DefaultPacker) Unpack(reader io.Reader) (*TcpMessage, error) {
	headerBuffer := make([]byte, 4+4)
	if _, err := io.ReadFull(reader, headerBuffer); err != nil {
		return nil, fmt.Errorf("read size and id err: %s", err)
	}
	dataSize := d.bytesOrder().Uint32(headerBuffer[:4])
	if d.MaxDataSize > 0 && int(dataSize) > d.MaxDataSize {
		return nil, fmt.Errorf("the dataSize %d is beyond the max: %d", dataSize, d.MaxDataSize)
	}
	id := d.bytesOrder().Uint32(headerBuffer[4:8])
	data := make([]byte, dataSize)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, fmt.Errorf("read data err: %s", err)
	}
	return NewTcpMessage(int(id), data), nil
}
