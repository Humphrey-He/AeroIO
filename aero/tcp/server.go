package tcp

import (
	"context"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// HandlerFunc 定义处理连接的回调类型，接收自定义的 *Conn
type HandlerFunc func(conn *Conn)

// Server 表示一个简单的 TCP 服务器
type Server struct {
	addr    string       // 监听地址，例如 ":8080"
	handler HandlerFunc  // 每个连接的处理函数
	ln      net.Listener // 底层监听器

	readTimeout  time.Duration // 读超时时间
	writeTimeout time.Duration // 写超时时间（当前未使用，但预留）

	wg     sync.WaitGroup     // 跟踪活动连接的 WaitGroup
	ctx    context.Context    // 用于取消服务器操作的上下文
	cancel context.CancelFunc // 取消函数
}

// NewServer 创建并初始化一个 Server 实例
func NewServer(addr string, handler HandlerFunc) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		addr:    addr,
		handler: handler,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// SetReadTimeout 设置连接的读超时时间
func (s *Server) SetReadTimeout(d time.Duration) { s.readTimeout = d }

// SetWriteTimeout 设置连接的写超时时间（当前未在代码中应用）
func (s *Server) SetWriteTimeout(d time.Duration) { s.writeTimeout = d }

// ListenAndServe 在指定地址上开始监听并提供服务
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln
	return s.serve()
}

// serve 接受连接并为每个连接启动一个独立的 goroutine 处理
func (s *Server) serve() error {
	for {
		raw, err := s.ln.Accept()
		if err != nil {
			// 如果服务器已被取消，返回 nil 表示正常关闭
			select {
			case <-s.ctx.Done():
				return nil
			default:
				// 对于临时性错误，继续 Accept 循环；否则返回错误
				if ne, ok := err.(net.Error); ok && ne.Temporary() {
					continue
				}
				return err
			}
		}
		s.wg.Add(1)
		go s.handleConn(raw)
	}
}

// handleConn 在单独的 goroutine 中处理单个连接
func (s *Server) handleConn(raw net.Conn) {
	defer s.wg.Done()
	defer raw.Close()

	conn := NewConn(raw) // 将 net.Conn 包装为自定义 Conn（假定在别处实现）
	if s.readTimeout > 0 {
		// 如果设置了读超时，为底层连接设置读取截止时间
		raw.SetReadDeadline(time.Now().Add(s.readTimeout))
	}
	s.handler(conn) // 调用用户提供的处理函数
}

// Shutdown 优雅关闭服务器：取消上下文、关闭监听器，并等待活动连接结束或超时
func (s *Server) Shutdown(timeout time.Duration) error {
	s.cancel()
	s.ln.Close()

	done := make(chan struct{})
	go func() {
		s.wg.Wait() // 等待所有连接处理完毕
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return context.DeadlineExceeded
	}
}

// Addr 返回当前监听器的地址（可能为 nil，如果未监听）
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// ListenAndServeWithGracefulShutdown 在后台运行 ListenAndServe，并监听 SIGINT/SIGTERM 做优雅关机
func (s *Server) ListenAndServeWithGracefulShutdown() error {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return err
	case <-sigCh:
		return s.Shutdown(30 * time.Second) // 收到信号后以 30 秒超时优雅关闭
	}
}
