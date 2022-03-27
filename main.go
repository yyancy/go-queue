package main

import (
	"flag"
	"log"

	"github.com/valyala/fasthttp"
	"github.com/yyancy/go-queue/server"
	"github.com/yyancy/go-queue/web"
)

func writeHander(ctx *fasthttp.RequestCtx) {
	ctx.WriteString("Hello, world!")
}

var (
	filename = flag.String("filename", "", "the filename where to put all data")
	inmem    = flag.Bool("inmem", false, "Whether or not use in-memory storage instead of a disk-based one")
	port     = flag.Uint("port", 8080, "The port where the server listen to")
)

func main() {
	log.SetFlags(log.Llongfile | log.LstdFlags)
	flag.Parse()
	// fasthttp.ListenAndServe(":8080", writeHander)
	var storage web.Storage
	if *inmem {
		storage = &server.InMemory{}
	} else {

		// if *filename == "" {
		// 	log.Fatalf("the flag --filename must be provided")
		// }
		// fp, err := os.OpenFile(*filename, os.O_CREATE|os.O_RDWR, 0666)
		// if err != nil {
		// 	log.Fatalf("could not open the filename %q, %s", *filename, err)
		// }
		// defer fp.Close()
		// storage = server.NewOnDisk(fp)
	}

	w, _ := web.NewWeb(storage, []string{"localhost"}, *port)
	err := w.Serve()
	if err != nil {
		log.Fatalf("web.Serve(): got err %v", err)
	}
}
