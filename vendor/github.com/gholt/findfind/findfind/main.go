package main

import (
	"fmt"
	"os"

	"github.com/gholt/findfind"
)

func main() {
	if err := findfind.FindFind(os.Args[1:]); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
