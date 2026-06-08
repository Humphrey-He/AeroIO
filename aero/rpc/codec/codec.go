package codec

import "io"

// Codec is the interface for RPC message serialization.
type Codec interface {
	Encode(w io.Writer, v interface{}) error
	Decode(r io.Reader, v interface{}) error
	Name() string
}
