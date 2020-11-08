package main

import (
	"flag"

	pgspy "github.com/jpoz/pgspy/pkg"
)

func main() {
	flag.Parse()
	pgspy.Start()
}
