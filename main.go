package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/valyala/fasthttp"
	"github.com/yyancy/go-queue/server"
	"github.com/yyancy/go-queue/web"
)

func writeHander(ctx *fasthttp.RequestCtx) {
	ctx.WriteString("Hello, world!")
}

var (
	dirname = flag.String("dirname", "", "the dirname where to put all data")
	inmem   = flag.Bool("inmem", false, "Whether or not use in-memory storage instead of a disk-based one")
	port    = flag.Uint("port", 8080, "The port where the server listen to")
)

func main() {
	log.SetFlags(log.Llongfile | log.LstdFlags)
	flag.Parse()
	var storage web.Storage
	if *inmem {
		storage = &server.InMemory{}
	} else {
		if *dirname == "" {
			log.Fatalf("the flag --dirname must be provided")
		}
		fp, err := os.OpenFile(filepath.Join(*dirname, "write_test"), os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			log.Fatalf("could not create test file %q, %s", *dirname, err)
		}
		fp.Close()
		os.Remove(fp.Name())
		storage, err = server.NewOnDisk(*dirname)
		if err != nil {
			log.Fatalf("server.NewOnDisk: %v", err)
		}
	}

	w, _ := web.NewWeb(storage, []string{"localhost"}, *port)
	err := w.Serve()
	if err != nil {
		log.Fatalf("web.Serve(): got err %v", err)
	}
}
