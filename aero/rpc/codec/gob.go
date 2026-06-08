package codec

import (
	"encoding/gob"
	"io"
)

func init() {
	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
}

type GobCodec struct{}

func NewGobCodec() *GobCodec {
	return &GobCodec{}
}

func (c *GobCodec) Encode(w io.Writer, v interface{}) error {
	return gob.NewEncoder(w).Encode(v)
}

func (c *GobCodec) Decode(r io.Reader, v interface{}) error {
	return gob.NewDecoder(r).Decode(v)
}

func (c *GobCodec) Name() string { return "gob" }
