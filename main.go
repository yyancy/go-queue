package main

import (
	"flag"
	"log"

	"github.com/yyancy/go-queue/integration"
)

var (
	instanceName = flag.String("instance-name", "moscow", "the instance uniue name")
	dirname      = flag.String("dirname", "", "the dirname where to put all data")
	listenAddr   = flag.String("listen", "127.0.0.1:8080", "Network adddress to listen on")
	etcdAddr     = flag.String("etcd", "127.0.0.1:2379", "etcd listen to")
)

func main() {
	log.SetFlags(log.Llongfile | log.LstdFlags)
	flag.Parse()

	if *instanceName == "" {
		log.Fatalf("The flag --instanceName must be provided")
	}
	if *dirname == "" {
		log.Fatalf("The flag --dirname must be provided")
	}
	if *etcdAddr == "" {
		log.Fatalf("The flag --etcd must be provided")
	}
	if err := integration.InitAndServe(*etcdAddr, *instanceName, *dirname, *listenAddr); err != nil {
		log.Fatalf("InitAndServe failed: %v", err)
	}
}
