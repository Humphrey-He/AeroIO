package main

import (
	"fmt"
	"log"

	"github.com/AeroIO/aero/tcp"
)

func main() {
	srv := tcp.NewServer(":8080", func(conn *tcp.Conn) {
		// Hand-crafted HTTP/1.1 response — no net/http used.
		response := "HTTP/1.1 200 OK\r\n" +
			"Content-Type: text/html; charset=utf-8\r\n" +
			"Content-Length: 13\r\n" +
			"Connection: close\r\n" +
			"\r\n" +
			"Hello, World!"

		conn.WriteString(response)
	})

	fmt.Println("AeroIO TCP Hello Server listening on http://localhost:8080")
	fmt.Println("Open your browser and visit the address above.")
	fmt.Println("Press Ctrl+C to stop.")

	if err := srv.ListenAndServeWithGracefulShutdown(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Server stopped gracefully.")
}
