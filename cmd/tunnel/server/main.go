package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/AeroIO/aero/tunnel"
	"github.com/AeroIO/cmd/tunnel/wsserver"
)

var (
	flagAddr      = flag.String("addr", ":8888", "Server listen address")
	flagWSAddr    = flag.String("ws-addr", "", "WebSocket server listen address (default: disabled)")
	flagMinPort   = flag.Uint("min-port", 10000, "Minimum port for dynamic allocation")
	flagMaxPort   = flag.Uint("max-port", 60000, "Maximum port for dynamic allocation")
	flagToken     = flag.String("token", "", "Authentication token (required if set)")
	flagCompress  = flag.Bool("compress", false, "Enable traffic compression")
	flagTransport = flag.String("transport", "tcp", "Transport type: tcp, websocket, or both")
)

func main() {
	flag.Parse()

	transport := strings.ToLower(*flagTransport)

	if transport == "tcp" || transport == "both" {
		go runTCPServer()
	}

	if transport == "websocket" || transport == "both" {
		wsAddr := *flagWSAddr
		if wsAddr == "" {
			_, port, err := net.SplitHostPort(*flagAddr)
			if err != nil {
				wsAddr = *flagAddr + ":8889"
			} else {
				wsAddr = "localhost:" + port
			}
		}
		go runWSServer(wsAddr)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println()
	fmt.Println("Server is running. Press Ctrl+C to stop.")
	fmt.Println()

	<-sigCh
}

func runTCPServer() {
	config := tunnel.ServerConfig{
		ListenAddr: *flagAddr,
		PortRange:  [2]uint16{uint16(*flagMinPort), uint16(*flagMaxPort)},
		Timeout:    60 * time.Second,
	}

	if *flagToken != "" {
		config.AuthTokens = []string{*flagToken}
	}

	if *flagCompress {
		config.Compression = tunnel.CompressionConfig{
			Level:   tunnel.CompressionLevelDefault,
			Enabled: true,
		}
	}

	server := tunnel.NewServer(config)

	server.SetOnPortOpen(func(binding *tunnel.PortBinding) {
		fmt.Printf("📦 [TCP] Port opened: :%d → %s (session: %s)\n",
			binding.Port, binding.TargetAddr, binding.SessionID)
	})

	server.SetOnPortClose(func(binding *tunnel.PortBinding) {
		fmt.Printf("📤 [TCP] Port closed: :%d\n", binding.Port)
	})

	fmt.Println("╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║            AeroIO Tunnel Server (TCP)                       ║")
	fmt.Println("╠════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  Listen: %-50s ║\n", config.ListenAddr)
	fmt.Printf("║  Port Range: %d - %d                                   ║\n", config.PortRange[0], config.PortRange[1])
	fmt.Println("╚════════════════════════════════════════════════════════════╝")

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("[TCP] Server error: %v", err)
	}
}

func runWSServer(addr string) {
	config := wsserver.Config{
		ListenAddr: addr,
		PortRange:  [2]uint16{uint16(*flagMinPort), uint16(*flagMaxPort)},
		Timeout:    60 * time.Second,
	}

	if *flagToken != "" {
		config.AuthTokens = []string{*flagToken}
	}

	server := wsserver.NewServer(config)

	server.SetOnPortOpen(func(binding *wsserver.PortBinding) {
		fmt.Printf("📦 [WS] Port opened: :%d → %s (session: %s)\n",
			binding.Port, binding.TargetAddr, binding.SessionID)
	})

	server.SetOnPortClose(func(binding *wsserver.PortBinding) {
		fmt.Printf("📤 [WS] Port closed: :%d\n", binding.Port)
	})

	fmt.Println("╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║         AeroIO Tunnel Server (WebSocket)                    ║")
	fmt.Println("╠════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  Listen: %-50s ║\n", addr)
	fmt.Printf("║  Port Range: %d - %d                                   ║\n", config.PortRange[0], config.PortRange[1])
	fmt.Println("╚════════════════════════════════════════════════════════════╝")

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("[WS] Server error: %v", err)
	}
}
