package tunnel

import (
	"bytes"
	"compress/flate"
	"io"
)

// Message type for compressed data
const MsgCompress = 0x08

// Compression level constants
const (
	CompressionLevelDefault = flate.DefaultCompression
	CompressionLevelBest    = flate.BestCompression
	CompressionLevelFast    = flate.BestSpeed
	CompressionLevelNone    = flate.NoCompression
)

// Compression flag bits
const (
	FlagCompressed = 1 << iota // bit 0 = compressed
)

// Compression configuration
type CompressionConfig struct {
	Level     int  // Compression level (flate.DefaultCompression, BestCompression, BestSpeed, NoCompression)
	Enabled   bool // Whether compression is enabled
}

// DefaultCompressionConfig returns default compression settings
func DefaultCompressionConfig() CompressionConfig {
	return CompressionConfig{
		Level:   CompressionLevelDefault,
		Enabled: false, // Must be explicitly enabled with -compress flag
	}
}

// CompressWriter wraps an io.Writer with gzip compression using flate
type CompressWriter struct {
	w         io.Writer
	flateW    *flate.Writer
	enabled   bool
}

// NewCompressWriter creates a new compression writer
func NewCompressWriter(w io.Writer, level int) (*CompressWriter, error) {
	flateW, err := flate.NewWriter(w, level)
	if err != nil {
		return nil, err
	}
	return &CompressWriter{
		w:       w,
		flateW:  flateW,
		enabled: true,
	}, nil
}

// Write writes compressed data to the underlying writer
func (cw *CompressWriter) Write(p []byte) (int, error) {
	if !cw.enabled {
		return cw.w.Write(p)
	}
	n, err := cw.flateW.Write(p)
	if err != nil {
		return n, err
	}
	return n, nil
}

// Flush flushes any buffered data to the underlying writer
func (cw *CompressWriter) Flush() error {
	if !cw.enabled {
		return nil
	}
	return cw.flateW.Flush()
}

// Close closes the compression writer and finalizes any remaining data
func (cw *CompressWriter) Close() error {
	if !cw.enabled {
		return nil
	}
	return cw.flateW.Close()
}

// Reset resets the compression writer to write to a new underlying writer
func (cw *CompressWriter) Reset(w io.Writer) {
	if !cw.enabled {
		cw.w = w
		return
	}
	cw.flateW.Reset(w)
	cw.w = w
}

// Enable enables compression
func (cw *CompressWriter) Enable() {
	cw.enabled = true
}

// Disable disables compression (pass-through mode)
func (cw *CompressWriter) Disable() {
	cw.enabled = false
}

// IsEnabled returns whether compression is enabled
func (cw *CompressWriter) IsEnabled() bool {
	return cw.enabled
}

// CompressReader wraps an io.Reader with flate decompression
type CompressReader struct {
	r        io.Reader
	flateR   io.ReadCloser
	enabled  bool
}

// NewCompressReader creates a new decompression reader
func NewCompressReader(r io.Reader) (*CompressReader, error) {
	flateR := flate.NewReader(r)
	return &CompressReader{
		r:       r,
		flateR:  flateR,
		enabled: true,
	}, nil
}

// Read reads and decompresses data from the underlying reader
func (cr *CompressReader) Read(p []byte) (int, error) {
	if !cr.enabled {
		return cr.r.Read(p)
	}
	return cr.flateR.Read(p)
}

// Close closes the decompression reader
func (cr *CompressReader) Close() error {
	if !cr.enabled {
		return nil
	}
	return cr.flateR.Close()
}

// Reset resets the decompression reader to read from a new underlying reader
func (cr *CompressReader) Reset(r io.Reader) error {
	if !cr.enabled {
		cr.r = r
		return nil
	}
	cr.flateR = flate.NewReader(r)
	cr.r = r
	return nil
}

// Enable enables decompression
func (cr *CompressReader) Enable() {
	cr.enabled = true
}

// Disable disables decompression (pass-through mode)
func (cr *CompressReader) Disable() {
	cr.enabled = false
}

// IsEnabled returns whether decompression is enabled
func (cr *CompressReader) IsEnabled() bool {
	return cr.enabled
}

// CompressedReadWriter is a wrapper that provides compressed read/write streams
type CompressedReadWriter struct {
	Reader   *CompressReader
	Writer   *CompressWriter
	enabled  bool
}

// NewCompressedReadWriter creates a new compressed read/write wrapper
func NewCompressedReadWriter(r io.Reader, w io.Writer, level int) (*CompressedReadWriter, error) {
	reader, err := NewCompressReader(r)
	if err != nil {
		return nil, err
	}
	writer, err := NewCompressWriter(w, level)
	if err != nil {
		return nil, err
	}
	return &CompressedReadWriter{
		Reader:  reader,
		Writer:  writer,
		enabled: true,
	}, nil
}

// Enable enables both compression and decompression
func (crw *CompressedReadWriter) Enable() {
	crw.enabled = true
	crw.Reader.Enable()
	crw.Writer.Enable()
}

// Disable disables both compression and decompression (pass-through mode)
func (crw *CompressedReadWriter) Disable() {
	crw.enabled = false
	crw.Reader.Disable()
	crw.Writer.Disable()
}

// IsEnabled returns whether compression is enabled
func (crw *CompressedReadWriter) IsEnabled() bool {
	return crw.enabled
}

// Close closes both the reader and writer
func (crw *CompressedReadWriter) Close() error {
	if err := crw.Writer.Close(); err != nil {
		return err
	}
	return crw.Reader.Close()
}

// CompressData compresses data using flate compression
func CompressData(data []byte, level int) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// Use a pipe to avoid allocating intermediate buffers
	pr, pw := io.Pipe()
	flateW, err := flate.NewWriter(pw, level)
	if err != nil {
		return nil, err
	}

	go func() {
		defer func() {
			flateW.Close()
			pw.Close()
		}()
		flateW.Write(data)
	}()

	result, err := io.ReadAll(pr)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// DecompressData decompresses data using flate decompression
func DecompressData(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	flateR := flate.NewReader(bytes.NewReader(data))
	defer flateR.Close()

	return io.ReadAll(flateR)
}

// ShouldCompress returns true if the data should be compressed
// Small payloads are not compressed as compression overhead may exceed savings
func ShouldCompress(data []byte) bool {
	// Don't compress small payloads (< 64 bytes) as compression overhead may exceed savings
	if len(data) < 64 {
		return false
	}
	return true
}
