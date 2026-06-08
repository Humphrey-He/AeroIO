package main

import (
	"fmt"
	"log"

	"github.com/AeroIO/aero/rpc"
)

func main() {
	client := rpc.NewClient(":9001")
	defer client.Close()

	fmt.Println("=== AeroIO RPC Client ===")
	fmt.Println("Connecting to tcp://localhost:9001...")
	fmt.Println()

	// Call Arith.Add
	{
		args := &Args{A: 10, B: 20}
		var reply Reply
		if err := client.Call("Arith.Add", args, &reply); err != nil {
			log.Fatal("Add failed:", err)
		}
		fmt.Printf("Arith.Add(%d, %d) = %d\n", args.A, args.B, reply.Result)
	}

	// Call Arith.Multiply
	{
		args := &Args{A: 7, B: 8}
		var reply Reply
		if err := client.Call("Arith.Multiply", args, &reply); err != nil {
			log.Fatal("Multiply failed:", err)
		}
		fmt.Printf("Arith.Multiply(%d, %d) = %d\n", args.A, args.B, reply.Result)
	}
}

// These types must match the server's registered types for gob encoding.
type Args struct {
	A, B int
}

type Reply struct {
	Result int
}
