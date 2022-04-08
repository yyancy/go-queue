package main

import (
	"flag"
	"log"

	"github.com/yyancy/go-queue/integration"
)

var (
	dirname  = flag.String("dirname", "", "the dirname where to put all data")
	port     = flag.Uint("port", 8080, "The port where the server listen to")
	etcdAddr = flag.String("etcd", "http://127.0.0.1:2379", "etcd listen to")
)

func main() {
	log.SetFlags(log.Llongfile | log.LstdFlags)
	flag.Parse()

	if *dirname == "" {
		log.Fatalf("The flag --dirname must be provided")
	}
	if *etcdAddr == "" {
		log.Fatalf("The flag --etcd must be provided")
	}
	if err := integration.InitAndServe(*etcdAddr, *dirname, *port); err != nil {
		log.Fatalf("InitAndServe(%s, %d): %v", *dirname, *port, err)
	}
}
