package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

// This demo shows the Reactor pattern using Go's built-in netpoller
// through goroutines and channels, demonstrating the event-driven architecture.

type connEvent struct {
	conn net.Conn
	data []byte
	err  error
}

func main() {
	ln, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()

	fmt.Println("=== AeroIO Reactor Echo Server ===")
	fmt.Println("Listening on :9000")
	fmt.Println("Connect with: telnet localhost 9000")
	fmt.Println("Press Ctrl+C to stop.")
	fmt.Println()

	// Reactor: event channel for new connections
	acceptCh := make(chan net.Conn, 128)
	done := make(chan struct{})

	// Main event loop goroutine (the "Reactor")
	go eventLoop(acceptCh, done)

	// Acceptor goroutine — registers listener with the reactor
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-done:
					return
				default:
					continue
				}
			}
			fmt.Printf("[Reactor] New connection from %s\n", conn.RemoteAddr())
			acceptCh <- conn
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
	close(done)
	ln.Close()
}

func eventLoop(acceptCh chan net.Conn, done chan struct{}) {
	// Channel-based demultiplexing: read events, write events, timer events
	readCh := make(chan connEvent, 128)

	for {
		select {
		case conn := <-acceptCh:
			// Register read handler for this connection
			go handleRead(conn, readCh)

		case ev := <-readCh:
			if ev.err != nil {
				fmt.Printf("[Reactor] Connection closed: %s (%v)\n", ev.conn.RemoteAddr(), ev.err)
				ev.conn.Close()
				continue
			}
			// Echo back
			fmt.Printf("[Reactor] Echoing %d bytes to %s\n", len(ev.data), ev.conn.RemoteAddr())
			ev.conn.Write(ev.data)
			// Re-register for more reads
			go handleRead(ev.conn, readCh)

		case <-done:
			return
		}
	}
}

func handleRead(conn net.Conn, readCh chan connEvent) {
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		readCh <- connEvent{conn: conn, err: err}
		return
	}
	readCh <- connEvent{conn: conn, data: buf[:n]}
}
