package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/AeroIO/aero/tunnel"
)

var (
	flagServer    = flag.String("server", "localhost:8888", "Tunnel server address")
	flagAgentID   = flag.String("id", "", "Agent ID (auto-generated if empty)")
	flagToken     = flag.String("token", "", "Authentication token")
	flagLocal     = flag.String("local", "", "Local service to expose (format: publicPort:target)")
	flagMappings  = flag.String("map", "", "Port mappings (format: public:local, comma separated)")
	flagTransport = flag.String("transport", "tcp", "Transport type: tcp or websocket (with auto-fallback)")
	flagWSPath    = flag.String("ws-path", "/tunnel", "WebSocket path (for websocket transport)")
	flagUseTLS    = flag.Bool("tls", false, "Use TLS for WebSocket transport")
	flagCompress  = flag.Bool("compress", false, "Enable traffic compression")
)

type LocalPortMapping struct {
	Public uint16
	Local  string
}

func main() {
	flag.Parse()

	// Parse transport type
	transportType, err := tunnel.ParseTransportType(*flagTransport)
	if err != nil {
		log.Fatalf("Invalid transport type: %v", err)
	}

	// Generate agent ID if not provided
	agentID := *flagAgentID
	if agentID == "" {
		b := make([]byte, 8)
		rand.Read(b)
		agentID = hex.EncodeToString(b)
	}

	// Parse local mappings
	var localPorts []LocalPortMapping

	if *flagMappings != "" {
		for _, mapping := range strings.Split(*flagMappings, ",") {
			parts := strings.Split(strings.TrimSpace(mapping), ":")
			if len(parts) != 2 {
				log.Fatalf("Invalid mapping format: %s (expected public:local)", mapping)
			}
			var publicPort uint16
			fmt.Sscanf(parts[0], "%d", &publicPort)
			localPorts = append(localPorts, LocalPortMapping{
				Public: publicPort,
				Local:  strings.TrimSpace(parts[1]),
			})
		}
	}

	if *flagLocal != "" {
		parts := strings.Split(*flagLocal, ":")
		if len(parts) != 2 {
			log.Fatalf("Invalid local format: %s (expected port:target)", *flagLocal)
		}
		var publicPort uint16
		fmt.Sscanf(parts[0], "%d", &publicPort)
		localPorts = append(localPorts, LocalPortMapping{
			Public: publicPort,
			Local:  parts[1],
		})
	}

	if len(localPorts) == 0 {
		fmt.Println("No local ports specified. Use -local or -map to specify port mappings.")
		fmt.Println("Example: -local 2222:localhost:22 -map 8080:localhost:8080,9000:localhost:9000")
		os.Exit(1)
	}

	// Create tunnel config
	config := tunnel.TunnelConfig{
		ServerAddr: *flagServer,
		AgentID:    agentID,
		Token:      *flagToken,
		Timeout:    30 * time.Second,
		Heartbeat:  30 * time.Second,
		Transport:  transportType,
		WSPath:     *flagWSPath,
		UseTLS:     *flagUseTLS,
	}

	if *flagCompress {
		config.Compression = tunnel.CompressionConfig{
			Level:   tunnel.CompressionLevelDefault,
			Enabled: true,
		}
		fmt.Printf("🔐 Compression enabled\n")
	}

	fmt.Printf("🔌 Connecting to tunnel server %s (transport: %s)...\n", *flagServer, transportType)

	var client *tunnel.Client
	var wsTransport *tunnel.WSTransport

	// Try connecting with auto-fallback
	client, wsTransport, err = tunnel.ClientWithAutoFallback(config)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	var transportName string
	if client != nil {
		transportName = "TCP"
		// Connect (already connected in ClientWithAutoFallback)
		fmt.Printf("✅ Connected via TCP as agent: %s\n", agentID)
	} else {
		transportName = "WebSocket"
		fmt.Printf("✅ Connected via WebSocket as agent: %s\n", agentID)
	}

	// Open ports
	for _, mapping := range localPorts {
		var channelID uint64
		var err error

		if client != nil {
			channelID, err = client.OpenPort(mapping.Public, mapping.Local)
		} else {
			channelID, err = wsTransport.OpenPort(mapping.Public, mapping.Local)
		}

		if err != nil {
			log.Printf("Failed to open port %d: %v", mapping.Public, err)
			continue
		}
		fmt.Printf("🌐 Port forwarded: :%d → %s (channel: %d)\n", mapping.Public, mapping.Local, channelID)
	}

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start local proxies
	if client != nil {
		go startLocalProxiesTCP(client, localPorts)
	} else {
		go startLocalProxiesWS(wsTransport, localPorts)
	}

	fmt.Println()
	fmt.Printf("Tunnel is active via %s. Press Ctrl+C to disconnect.\n", transportName)
	fmt.Println()

	// Wait for shutdown
	<-sigCh
	fmt.Println("\n🛑 Disconnecting...")
	if client != nil {
		client.Close()
	} else {
		wsTransport.Close()
	}
	fmt.Println("Disconnected.")
}

func startLocalProxiesTCP(client *tunnel.Client, ports []LocalPortMapping) {
	for _, mapping := range ports {
		go func(m LocalPortMapping) {
			listenAddr := fmt.Sprintf("127.0.0.1:%d", m.Public)
			ln, err := net.Listen("tcp", listenAddr)
			if err != nil {
				log.Printf("Failed to listen on %s: %v", listenAddr, err)
				return
			}
			fmt.Printf("👂 Listening on %s for tunnel\n", listenAddr)

			for {
				localConn, err := ln.Accept()
				if err != nil {
					continue
				}
				go handleLocalConnectionTCP(client, localConn, m.Local)
			}
		}(mapping)
	}

	// Block forever
	select {}
}

func startLocalProxiesWS(wsTransport *tunnel.WSTransport, ports []LocalPortMapping) {
	for _, mapping := range ports {
		go func(m LocalPortMapping) {
			listenAddr := fmt.Sprintf("127.0.0.1:%d", m.Public)
			ln, err := net.Listen("tcp", listenAddr)
			if err != nil {
				log.Printf("Failed to listen on %s: %v", listenAddr, err)
				return
			}
			fmt.Printf("👂 Listening on %s for tunnel\n", listenAddr)

			for {
				localConn, err := ln.Accept()
				if err != nil {
					continue
				}
				go handleLocalConnectionWS(wsTransport, localConn, m.Local)
			}
		}(mapping)
	}

	// Block forever
	select {}
}

func handleLocalConnectionTCP(client *tunnel.Client, localConn net.Conn, target string) {
	defer localConn.Close()

	// Connect to target
	targetConn, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		log.Printf("Failed to connect to target %s: %v", target, err)
		return
	}
	defer targetConn.Close()

	// Bidirectional copy
	go func() {
		io.Copy(targetConn, localConn)
		targetConn.Close()
	}()
	io.Copy(localConn, targetConn)
}

func handleLocalConnectionWS(wsTransport *tunnel.WSTransport, localConn net.Conn, target string) {
	defer localConn.Close()

	// Connect to target
	targetConn, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		log.Printf("Failed to connect to target %s: %v", target, err)
		return
	}
	defer targetConn.Close()

	// Bidirectional copy
	go func() {
		io.Copy(targetConn, localConn)
		targetConn.Close()
	}()
	io.Copy(localConn, targetConn)
}
