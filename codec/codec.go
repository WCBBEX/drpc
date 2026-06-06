package codec

import "io"

type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json"
)

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
