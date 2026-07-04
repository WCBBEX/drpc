package drpc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/WCBBEX/drpc/codec"
	"time"
)

const HandshakeSize = 16

type OptionPacket struct {
	MagicNumber    uint32
	CodecType      uint8
	Reserved1      uint8
	ConnectTimeout uint32
	HandleTimeout  uint32
	Reserved2      uint16
}

func PackOption(opt *Option) ([]byte, error) {
	packet := OptionPacket{
		MagicNumber:    uint32(opt.MagicNumber),
		CodecType:      codec.TypeToID(opt.CodecType),
		ConnectTimeout: uint32(opt.ConnectTimeout / time.Millisecond),
		HandleTimeout:  uint32(opt.HandleTimeout / time.Millisecond),
	}

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, &packet); err != nil {
		return nil, fmt.Errorf("rpc protocol: pack option failed: %w", err)
	}
	return buf.Bytes(), nil
}

func UnpackOption(buf []byte) (*Option, error) {
	var packet OptionPacket
	r := bytes.NewReader(buf)

	if err := binary.Read(r, binary.BigEndian, &packet); err != nil {
		return nil, fmt.Errorf("rpc protocol: unpack option failed: %w", err)
	}

	codecType := codec.IDToType(packet.CodecType)
	if codecType == "" {
		return nil, fmt.Errorf("rpc protocol: unknown codec id %d", packet.CodecType)
	}

	return &Option{
		MagicNumber:    int(packet.MagicNumber),
		CodecType:      codecType,
		ConnectTimeout: time.Duration(packet.ConnectTimeout) * time.Millisecond,
		HandleTimeout:  time.Duration(packet.HandleTimeout) * time.Millisecond,
	}, nil
}
