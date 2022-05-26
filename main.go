package main

import (
	"flag"
	"log"
	"strings"

	"github.com/yyancy/go-queue/integration"
)

var (
	instanceName = flag.String("instance-name", "moscow", "the instance uniue name")
	clusterName  = flag.String("cluster", "default", "The name of the cluster (must specify if sharing a single etcd instance with several Chukcha instances)")
	dirname      = flag.String("dirname", "", "the dirname where to put all data")
	listenAddr   = flag.String("listen", "127.0.0.1:8080", "Network adddress to listen on")
	etcdAddr     = flag.String("etcd", "127.0.0.1:2379", "etcd listen to")
)

func main() {
	log.SetFlags(log.Llongfile | log.LstdFlags)
	flag.Parse()

	if *clusterName == "" {
		log.Fatalf("The flag `--cluster` must not be empty")
	}

	if *instanceName == "" {
		log.Fatalf("The flag --instance-name must be provided")
	}
	if *dirname == "" {
		log.Fatalf("The flag --dirname must be provided")
	}
	if *etcdAddr == "" {
		log.Fatalf("The flag --etcd must be provided")
	}
	a := integration.InitArgs{
		EtcdAddr:     strings.Split(*etcdAddr, ","),
		ClusterName:  *clusterName,
		InstanceName: *instanceName,
		DirName:      *dirname,
		ListenAddr:   *listenAddr,
	}

	if err := integration.InitAndServe(a); err != nil {
		log.Fatalf("InitAndServe failed: %v", err)
	}
}
