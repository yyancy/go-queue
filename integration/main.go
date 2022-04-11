package integration

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/yyancy/go-queue/server/replication"
	"github.com/yyancy/go-queue/web"

	"go.etcd.io/etcd/clientv3"
)

func writeHander(ctx *fasthttp.RequestCtx) {
	ctx.WriteString("Hello, world!")
}

func InitAndServe(etcdAddr, instanceName, dirname string, listenAddr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(etcdAddr, ","),
		DialTimeout: 5 * time.Second,
	})
	// log.Printf("cli= %v", cli)
	if err != nil {
		return err
	}
	defer client.Close()

	_, err = client.Put(ctx, "test", "sample_value")
	if err != nil {
		return fmt.Errorf("could not set test key to etcd: %v", err)
	}
	_, err = client.Put(ctx, "peers/"+instanceName, listenAddr)
	if err != nil {
		return fmt.Errorf("could not set test key to etcd: %v", err)
	}

	fp, err := os.OpenFile(filepath.Join(dirname, "write_test"), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("could not create test file %q, %s", dirname, err)
	}
	fp.Close()
	os.Remove(fp.Name())

	w := web.NewWeb(client, instanceName, dirname, listenAddr, replication.NewStorage(client, instanceName))
	log.Printf("Listening connections")
	return w.Serve()
}
