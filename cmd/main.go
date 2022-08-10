package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	rdapp "github.com/kishaningithub/rdapp/pkg"
)

const examples = `
examples:
	rdapp`

func main() {
	var options rdapp.Options
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: rdapp [options]")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, examples)
	}
	flag.StringVar(&options.ListenAddress, "listen", "127.0.0.1:25432", "Listen address")
	flag.Parse()
	err := rdapp.RunPostgresRedshiftProxy(options)
	if err != nil {
		log.Fatal(err)
	}
}
