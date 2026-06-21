package codec

import "io"

type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json"
)

func TypeToID(t Type) uint8 {
	switch t {
	case GobType:
		return 1
	case JsonType:
		return 2
	default:
		return 0
	}
}

func IDToType(id uint8) Type {
	switch id {
	case 1:
		return GobType
	case 2:
		return JsonType
	default:
		return ""
	}
}

type NewCodecFunc func(io.ReadWriteCloser) Codec

var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}

type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(any) error
	Write(*Header, any) error
}

type Header struct {
	ServiceMethod string
	Seq           uint64
	Error         string
}
