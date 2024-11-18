package main

import (
	"fmt"
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

	last := time.Now()
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 10; i++ {
		c.PullGlobalTrigger()

		fmt.Printf("%v: trigger\n", time.Since(last))
		last = time.Now()
		time.Sleep(100 * time.Millisecond)
	}

}
