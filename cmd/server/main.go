package main

import (
	"fmt"
	"net"
	"strconv"

	"tstream"
	"tstream/rpc"

	"google.golang.org/grpc"
)

func main() {

	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}

	srv := grpc.NewServer()
	rpc.RegisterTriggeredStreamServer(srv, tstream.NewServer(data{}))

	fmt.Println("Serving on 8080")
	srv.Serve(lis)
}

type data struct{}

func (d data) Get(in []string) ([]uint32, error) {
	out := make([]uint32, 0)

	for _, s := range in {
		x, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		out = append(out, uint32(x))
	}

	return out, nil
}
