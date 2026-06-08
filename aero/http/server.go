package http

import (
	"fmt"
	"log"
	"time"

	"github.com/AeroIO/aero/tcp"
)

type Server struct {
	router  *Router
	tcpSrv  *tcp.Server
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func NewServer(addr string) *Server {
	s := &Server{
		router: NewRouter(),
	}
	s.tcpSrv = tcp.NewServer(addr, s.handleConn)
	s.tcpSrv.SetReadTimeout(30 * time.Second)
	s.tcpSrv.SetWriteTimeout(30 * time.Second)
	return s
}

func (s *Server) Router() *Router {
	return s.router
}

func (s *Server) handleConn(conn *tcp.Conn) {
	parser := NewParser()

	for {
		req, err := parser.Parse(conn)
		if err != nil {
			break
		}

		resp := s.router.ServeHTTP(req)

		conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
		_, err = conn.Write(resp.Encode())
		if err != nil {
			break
		}

		if !req.KeepAlive {
			break
		}
	}
}

func (s *Server) ListenAndServe() error {
	fmt.Printf("AeroIO HTTP/WebSocket Server listening on http://%s\n", s.tcpSrv.Addr())
	fmt.Println("Press Ctrl+C to stop.")
	return s.tcpSrv.ListenAndServeWithGracefulShutdown()
}

func init() {
	log.SetFlags(log.Ltime | log.Lshortfile)
}
