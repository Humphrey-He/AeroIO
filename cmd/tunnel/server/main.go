package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/AeroIO/aero/tunnel"
)

var (
	flagAddr    = flag.String("addr", ":8888", "Server listen address")
	flagMinPort = flag.Uint("min-port", 10000, "Minimum port for dynamic allocation")
	flagMaxPort = flag.Uint("max-port", 60000, "Maximum port for dynamic allocation")
)

func main() {
	flag.Parse()

	config := tunnel.ServerConfig{
		ListenAddr: *flagAddr,
		PortRange:  [2]uint16{uint16(*flagMinPort), uint16(*flagMaxPort)},
		Timeout:    60,
	}

	server := tunnel.NewServer(config)

	// Set up callbacks
	server.SetOnPortOpen(func(binding *tunnel.PortBinding) {
		fmt.Printf("📦 Port opened: :%d → %s (session: %s)\n",
			binding.Port, binding.TargetAddr, binding.SessionID)
	})

	server.SetOnPortClose(func(binding *tunnel.PortBinding) {
		fmt.Printf("📤 Port closed: :%d\n", binding.Port)
	})

	// Handle shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\n🛑 Shutting down server...")
		server.Stop()
		os.Exit(0)
	}()

	fmt.Println("╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║            AeroIO Tunnel Server                              ║")
	fmt.Println("╠════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  Listen: %-50s ║\n", config.ListenAddr)
	fmt.Printf("║  Port Range: %d - %d                                   ║\n", config.PortRange[0], config.PortRange[1])
	fmt.Println("╚════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("Server is running. Press Ctrl+C to stop.")
	fmt.Println()

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
