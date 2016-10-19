package main

import (
	"fmt"
	"os"

	"github.com/gholt/dudu"
)

func main() {
	if err := dudu.DUDU(os.Args[1:]); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
