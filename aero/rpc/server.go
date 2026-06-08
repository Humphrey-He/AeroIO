package rpc

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/AeroIO/aero/rpc/protocol"
	"github.com/AeroIO/aero/tcp"
)

// Server is the RPC server.
type Server struct {
	registry *Registry
	addr     string
	reqID    atomic.Uint64
}

func NewServer(addr string) *Server {
	return &Server{
		registry: NewRegistry(),
		addr:     addr,
	}
}

func (s *Server) Registry() *Registry { return s.registry }

// RegisterService registers a service.
func (s *Server) RegisterService(name string, receiver interface{}) error {
	return s.registry.Register(name, receiver)
}

// Serve starts the RPC server.
func (s *Server) Serve() error {
	srv := tcp.NewServer(s.addr, s.handleConn)
	fmt.Printf("AeroIO RPC Server listening on tcp://%s\n", s.addr)
	return srv.ListenAndServeWithGracefulShutdown()
}

func (s *Server) handleConn(conn *tcp.Conn) {
	for {
		msg, err := protocol.Decode(conn)
		if err != nil {
			return // connection closed or broken
		}

		resp := s.dispatch(msg)
		if err := resp.Encode(conn); err != nil {
			return
		}
	}
}

func (s *Server) dispatch(msg *protocol.Message) *protocol.Message {
	serviceMethod := string(msg.Header)

	// Decode arguments
	var args interface{}
	buf := bytes.NewBuffer(msg.Payload)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&args); err != nil {
		log.Printf("RPC decode args error: %v", err)
		return protocol.NewError(msg.RequestID, err.Error())
	}

	reply, err := s.registry.Call(serviceMethod, args)
	if err != nil {
		return protocol.NewError(msg.RequestID, err.Error())
	}

	// Encode reply
	var outBuf bytes.Buffer
	enc := gob.NewEncoder(&outBuf)
	if err := enc.Encode(reply); err != nil {
		return protocol.NewError(msg.RequestID, err.Error())
	}

	return protocol.NewResponse(msg.RequestID, outBuf.Bytes())
}
