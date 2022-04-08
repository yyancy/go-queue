package integration

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/yyancy/go-queue/web"
	"go.etcd.io/etcd/client"
)

func writeHander(ctx *fasthttp.RequestCtx) {
	ctx.WriteString("Hello, world!")
}

func InitAndServe(etcdAddr, dirname string, port uint) error {
	cfg := client.Config{
		Endpoints: []string{"http://127.0.0.1:2379"},
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := client.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	kapi := client.NewKeysAPI(c)

	fp, err := os.OpenFile(filepath.Join(dirname, "write_test"), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("could not create test file %q, %s", dirname, err)
	}
	fp.Close()
	os.Remove(fp.Name())

	w, _ := web.NewWeb(dirname, port)
	log.Printf("Listening connections")
	return w.Serve()
}
