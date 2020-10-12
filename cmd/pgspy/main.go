package main

import (
	"flag"
	"fmt"

	pgspy "github.com/jpoz/pgspy/pkg"
)

func main() {
	flag.Parse()
	fmt.Println("Running")
	pgspy.Start()
}
