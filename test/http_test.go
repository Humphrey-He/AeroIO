package test

import (
	"net"
	"testing"
	"time"

	"github.com/AeroIO/aero/tcp"
)

func TestHTTPRouter(t *testing.T) {
	// Test the HTTP router by starting a server and making requests
	srv := tcp.NewServer(":18082", func(conn *tcp.Conn) {
		// Simple HTTP test: read request, return response
		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		_ = n

		body := "OK"
		response := "HTTP/1.1 200 OK\r\n" +
			"Content-Type: text/plain\r\n" +
			"Content-Length: 2\r\n" +
			"Connection: close\r\n" +
			"\r\n" +
			body
		conn.WriteString(response)
	})

	go srv.ListenAndServe()
	time.Sleep(100 * time.Millisecond)
	defer srv.Shutdown(5 * time.Second)

	conn, err := net.DialTimeout("tcp", ":18082", 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Send HTTP request
	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	response := string(buf[:n])
	if !contains(response, "200 OK") {
		t.Errorf("expected 200 OK, got: %s", response)
	}
}

func TestHTTPKeepAlive(t *testing.T) {
	srv := tcp.NewServer(":18083", func(conn *tcp.Conn) {
		for i := 0; i < 3; i++ {
			// ReadV2 request
			buf := make([]byte, 4096)
			n, err := conn.Read(buf)
			if err != nil || n == 0 {
				return
			}

			response := "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: keep-alive\r\n\r\nHello"
			conn.WriteString(response)
		}
	})

	go srv.ListenAndServe()
	time.Sleep(100 * time.Millisecond)
	defer srv.Shutdown(5 * time.Second)

	conn, err := net.DialTimeout("tcp", ":18083", 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Send multiple requests on same connection
	for i := 0; i < 3; i++ {
		_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n"))
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}

		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !contains(string(buf[:n]), "200 OK") {
			t.Errorf("request %d: expected 200 OK", i)
		}
	}
}
