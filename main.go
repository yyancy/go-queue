package main

import (
	"log"

	"github.com/valyala/fasthttp"
	"github.com/yyancy/go-queue/web"
)

func writeHander(ctx *fasthttp.RequestCtx) {
	ctx.WriteString("Hello, world!")
}
func main() {
	log.SetFlags(log.Llongfile | log.LstdFlags)
	// fasthttp.ListenAndServe(":8080", writeHander)
	w, _ := web.NewWeb([]string{"localhost"})
	err := w.Serve()
	if err != nil {
		log.Fatalf("web.Serve(): got err %v", err)
	}
}
