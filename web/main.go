package web

import (
	"io"
	"log"
	"net/http"

	"github.com/valyala/fasthttp"
	"github.com/yyancy/go-queue/server"
)

const defaultBufferSize = 512 * 1024

type Web struct {
	addrs  []string
	server server.InMemory
}

func NewWeb(addrs []string) (w *Web, err error) {
	return &Web{addrs: addrs}, nil
}
func (w *Web) errorHandler(err error, ctx *fasthttp.RequestCtx) {
	if err != io.EOF {
		ctx.SetStatusCode(http.StatusInternalServerError)
		ctx.WriteString("internal server error: " + err.Error())
	}
}
func (w *Web) readHandler(ctx *fasthttp.RequestCtx) {
	buf := make([]byte, defaultBufferSize)
	res, err := w.server.Recv(buf)
	if err != nil {
		w.errorHandler(err, ctx)
	}
	if len(res) == 0 {
		ctx.SetConnectionClose()
		return
	}
	log.Printf("read(): got %q", string(res))
	ctx.WriteString(string(res))
}
func (w *Web) writeHandler(ctx *fasthttp.RequestCtx) {
	b := ctx.PostBody()
	log.Printf("write(): recieved %q", string(b))
	err := w.server.Send(b)
	if err != nil {
		w.errorHandler(err, ctx)
	}
	ctx.WriteString("successful")
}

func (w *Web) httpHander(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/read":
		w.readHandler(ctx)

	case "/write":
		w.writeHandler(ctx)
	}
}
func (w *Web) Serve() error {

	log.Printf("The server is running at %d port", 8080)
	err := fasthttp.ListenAndServe(w.addrs[0]+":8080", w.httpHander)
	return err
}
