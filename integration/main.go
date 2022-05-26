package integration

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/yyancy/go-queue/server/replication"
	"github.com/yyancy/go-queue/web"
)

type InitArgs struct {
	EtcdAddr []string

	ClusterName  string
	InstanceName string

	DirName    string
	ListenAddr string
}

func writeHander(ctx *fasthttp.RequestCtx) {
	ctx.WriteString("Hello, world!")
}

func InitAndServe(a InitArgs) error {
	client, err := replication.NewClient(a.EtcdAddr, a.ClusterName)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	if err := client.RegisterNewPeer(ctx, replication.Peer{
		InstanceName: a.InstanceName,
		ListenAddr:   a.ListenAddr,
	}); err != nil {
		return fmt.Errorf("could not register peer address in etcd: %w", err)
	}

	filename := filepath.Join(a.DirName, "write_test")
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("could not create test file %q, %s", filename, err)
	}
	fp.Close()
	os.Remove(fp.Name())

	repl := replication.NewStorage(client, a.InstanceName)
	w := web.NewWeb(client, a.InstanceName, a.DirName, a.ListenAddr, repl)
	log.Printf("Listening connections")
	return w.Serve()
}
