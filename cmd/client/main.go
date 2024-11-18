package main

import (
	"fmt"
	"math/rand"
	"time"
	"tstream"
)

func main() {
	c := tstream.NewClient()

	err := c.Connect("", 8080)
	if err != nil {
		fmt.Println("could not connect")
		panic(err)
	}

	numStrs := make([]string, 0)
	count := 5 + rand.Int31n(5)
	for i := int32(0); i < count; i++ {
		numStrs = append(numStrs, fmt.Sprintf("%d", rand.Int31n(20)))
	}

	stream, err := c.NewStreamWithGlobalTrigger(numStrs)

	last := time.Now()
	for i := 0; i < 10; i++ {
		d, err := stream.Recv()
		if err != nil {
			panic(err)
		}

		fmt.Printf("%v: %v\n", time.Since(last), d)
		last = time.Now()
	}

}
