package main

import (
	"fmt"
	"os"

	"github.com/gholt/cpcp"
)

func main() {
	if err := cpcp.CPCP(os.Args[1:]); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
