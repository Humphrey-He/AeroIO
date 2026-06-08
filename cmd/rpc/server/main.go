package main

import (
	"fmt"
	"log"

	"github.com/AeroIO/aero/rpc"
)

// Arith is an example RPC service.
type Arith struct{}

type Args struct {
	A, B int
}

type Reply struct {
	Result int
}

func (a *Arith) Add(args *Args, reply *Reply) error {
	reply.Result = args.A + args.B
	return nil
}

func (a *Arith) Multiply(args *Args, reply *Reply) error {
	reply.Result = args.A * args.B
	return nil
}

func main() {
	srv := rpc.NewServer(":9001")

	if err := srv.RegisterService("Arith", &Arith{}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("=== AeroIO RPC Server ===")
	fmt.Println("Listening on tcp://localhost:9001")
	fmt.Println("Registered services:", srv.Registry().List())
	fmt.Println()

	if err := srv.Serve(); err != nil {
		log.Fatal(err)
	}
}
