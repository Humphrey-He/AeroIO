package test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/AeroIO/aero/tcp"
)

func TestTCPServerHelloWorld(t *testing.T) {
	srv := tcp.NewServer(":18080", func(conn *tcp.Conn) {
		response := "HTTP/1.1 200 OK\r\n" +
			"Content-Type: text/html; charset=utf-8\r\n" +
			"Content-Length: 13\r\n" +
			"Connection: close\r\n" +
			"\r\n" +
			"Hello, World!"
		conn.WriteString(response)
	})

	go srv.ListenAndServe()
	time.Sleep(100 * time.Millisecond)
	defer srv.Shutdown(5 * time.Second)

	conn, err := net.DialTimeout("tcp", ":18080", 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

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
	if !contains(response, "Hello, World!") {
		t.Errorf("expected Hello, World!, got: %s", response)
	}
}

func TestTCPServerConcurrent(t *testing.T) {
	srv := tcp.NewServer(":18081", func(conn *tcp.Conn) {
		conn.WriteString("OK\n")
	})

	go srv.ListenAndServe()
	time.Sleep(100 * time.Millisecond)
	defer srv.Shutdown(5 * time.Second)

	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			conn, err := net.DialTimeout("tcp", ":18081", 3*time.Second)
			if err != nil {
				t.Errorf("dial: %v", err)
				done <- false
				return
			}
			defer conn.Close()

			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err != nil || string(buf[:n]) != "OK\n" {
				t.Errorf("unexpected response: %q, err=%v", string(buf[:n]), err)
				done <- false
				return
			}
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		if !<-done {
			t.Fatal("concurrent test failed")
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func init() {
	fmt.Println("TCP tests initialized")
}
