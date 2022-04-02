package main

import (
	"flag"
	"log"

	"github.com/yyancy/go-queue/integration"
)

var (
	dirname = flag.String("dirname", "", "the dirname where to put all data")
	port    = flag.Uint("port", 8080, "The port where the server listen to")
)

func main() {
	log.SetFlags(log.Llongfile | log.LstdFlags)
	flag.Parse()

	if *dirname == "" {
		log.Fatalf("The flag --dirname must be provided")
	}
	if err := integration.InitAndServe(*dirname, *port); err != nil {
		log.Fatalf("InitAndServe(%s, %d): %v", *dirname, *port, err)
	}
}
