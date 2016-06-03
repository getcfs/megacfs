package main

import (
	"io/ioutil"
	"os"

	"github.com/gholt/blackfridaytext"
)

func main() {
	opt := &blackfridaytext.Options{Color: true}
	if len(os.Args) == 2 && os.Args[1] == "--no-color" {
		opt.Color = false
	}
	markdown, _ := ioutil.ReadAll(os.Stdin)
	metadata, output := blackfridaytext.MarkdownToText(markdown, opt)
	for _, item := range metadata {
		name, value := item[0], item[1]
		os.Stdout.WriteString(name)
		os.Stdout.WriteString(":\n    ")
		os.Stdout.WriteString(value)
		os.Stdout.WriteString("\n")
	}
	os.Stdout.WriteString("\n")
	os.Stdout.Write(output)
	os.Stdout.WriteString("\n")
}
