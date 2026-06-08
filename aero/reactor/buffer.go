package reactor

import (
	"sync"
)

// BufferPool manages a pool of byte buffers for zero-copy I/O.
type BufferPool struct {
	sizes   []int
	pools   []sync.Pool
	maxSize int
}

// Default buffer sizes: 256B, 1KB, 4KB, 16KB, 64KB, 256KB
var defaultSizes = []int{256, 1024, 4096, 16384, 65536, 262144}

func NewBufferPool(sizes ...int) *BufferPool {
	if len(sizes) == 0 {
		sizes = defaultSizes
	}
	bp := &BufferPool{sizes: sizes, maxSize: sizes[len(sizes)-1]}
	bp.pools = make([]sync.Pool, len(sizes))
	for i := range bp.pools {
		size := sizes[i]
		bp.pools[i] = sync.Pool{
			New: func() interface{} {
				buf := make([]byte, size)
				return &buf
			},
		}
	}
	return bp
}

func (bp *BufferPool) Get(minSize int) *[]byte {
	for i, sz := range bp.sizes {
		if sz >= minSize {
			buf := bp.pools[i].Get().(*[]byte)
			return buf
		}
	}
	// Too large for pool — allocate directly
	buf := make([]byte, minSize)
	return &buf
}

func (bp *BufferPool) Put(buf *[]byte) {
	size := cap(*buf)
	for i, sz := range bp.sizes {
		if size <= sz {
			// Reset length
			*buf = (*buf)[:cap(*buf)]
			bp.pools[i].Put(buf)
			return
		}
	}
	// Too large for pool — let GC collect
}

// PooledWriter implements io.Writer using buffer pool.
type PooledWriter struct {
	buf  []byte
	pool *BufferPool
}

func (w *PooledWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *PooledWriter) Bytes() []byte  { return w.buf }
func (w *PooledWriter) Reset()         { w.buf = w.buf[:0] }
func (w *PooledWriter) Release()       { w.pool.Put(&w.buf) }
