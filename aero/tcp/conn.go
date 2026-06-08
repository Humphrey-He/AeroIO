package tcp

import (
	"bufio"
	"net"
	"time"
)

// Conn 是对 net.Conn 的轻量封装，提供带缓冲的读写器并保留原始连接用于底层控制。
// 目的：在上层协议解析中避免每次小读/小写造成大量系统调用，同时暴露常用的 deadline/addr 操作。
type Conn struct {
	raw    net.Conn      // 底层原始网络连接（实现实际的 Read/Write/Close 等）
	reader *bufio.Reader // 带缓冲的读取器，用于高效的逐字节/按行/按分隔符读取
	writer *bufio.Writer // 带缓冲的写入器，用于聚合小片段并在 Flush 时一次性写出
}

// NewConn 使用指定缓冲区大小创建 Conn。
// bufio.NewReaderSize/WriterSize 可以通过调整 4096 来优化不同负载下的性能。
func NewConn(raw net.Conn) *Conn {
	return &Conn{
		raw:    raw,
		reader: bufio.NewReaderSize(raw, 4096),
		writer: bufio.NewWriterSize(raw, 4096),
	}
}

// Read 从内部的 bufio.Reader 读取数据到 p。
// 注意：这是阻塞调用，直到有数据或发生错误。
// 推荐上层使用短超时或 SetReadDeadline 配合以防止无限阻塞。
func (c *Conn) Read(p []byte) (int, error) { return c.reader.Read(p) }

// ReadByte 从缓冲区读取单字节（比直接每次从 raw 读取更高效）。
func (c *Conn) ReadByte() (byte, error) { return c.reader.ReadByte() }

// ReadBytes 按照指定分隔符读取直到包含该分隔符（包含分隔符本身）。
// 常用于读取以 '\n' 结尾的行（注意返回的切片可能包含 '\r'）。
func (c *Conn) ReadBytes(delim byte) ([]byte, error) { return c.reader.ReadBytes(delim) }

// Write 将数据写入缓冲区并立即 Flush（将缓冲区内容写到底层连接）。
// 设计选择：为了简化语义，Write 在返回前执行 Flush，确保数据立即发送。
// 对高吞吐场景可改为仅写入缓冲并在合适时机批量 Flush（以减少系统调用）。
func (c *Conn) Write(p []byte) (int, error) {
	n, err := c.writer.Write(p)
	if err != nil {
		return n, err
	}
	return n, c.writer.Flush()
}

// WriteString 与 Write 类似，但接受字符串以避免中间转换分配。
func (c *Conn) WriteString(s string) (int, error) {
	n, err := c.writer.WriteString(s)
	if err != nil {
		return n, err
	}
	return n, c.writer.Flush()
}

// Flush 显式把缓冲区内容写到底层连接；上层在需要批量写或延迟 Flush 时调用。
func (c *Conn) Flush() error { return c.writer.Flush() }

// Close 关闭底层连接；注意：bufio.Writer 缓冲未 Flush 时数据会丢失，
// 因此在关闭前应调用 Flush（或让 Write 已经 Flush）。
func (c *Conn) Close() error { return c.raw.Close() }

// RemoteAddr 返回远程地址，常用于日志或访问控制。
func (c *Conn) RemoteAddr() net.Addr { return c.raw.RemoteAddr() }

// SetDeadline/SetReadDeadline/SetWriteDeadline 直接代理到底层连接，
// 用于设置读写超时，以防止慢速或挂起的客户端占用资源过久。
func (c *Conn) SetDeadline(t time.Time) error      { return c.raw.SetDeadline(t) }
func (c *Conn) SetReadDeadline(t time.Time) error  { return c.raw.SetReadDeadline(t) }
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.raw.SetWriteDeadline(t) }
