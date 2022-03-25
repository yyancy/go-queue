package web

import (
	"io"
	"log"
	"net/http"
	"runtime/debug"

	"github.com/valyala/fasthttp"
)

const defaultBufferSize = 512 * 1024

type Web struct {
	addrs  []string
	server Storage
}

type Storage interface {
	Recv(off uint, maxSize uint, w io.Writer) error
	Send(msg []byte) error
	Ack() error
}

func NewWeb(s Storage, addrs []string) (w *Web, err error) {
	return &Web{server: s, addrs: addrs}, nil
}
func (w *Web) errorHandler(err error, ctx *fasthttp.RequestCtx) {
	if err != io.EOF {
		ctx.SetStatusCode(http.StatusInternalServerError)
		ctx.WriteString("internal server error: " + err.Error())
		debug.PrintStack()
	}
}
func (w *Web) readHandler(ctx *fasthttp.RequestCtx) {
	off, err := ctx.QueryArgs().GetUint("off")
	if err != nil {
		w.errorHandler(err, ctx)
		return
	}
	maxSize, err := ctx.QueryArgs().GetUint("maxSize")
	if err != nil {
		w.errorHandler(err, ctx)
		return
	}
	err = w.server.Recv(uint(off), uint(maxSize), ctx)
	if err != nil {
		w.errorHandler(err, ctx)
		return
	}

}
func (w *Web) writeHandler(ctx *fasthttp.RequestCtx) {
	b := ctx.PostBody()
	// log.Printf("write(): recieved %q", string(b))
	err := w.server.Send(b)
	if err != nil {
		w.errorHandler(err, ctx)
	}
	ctx.WriteString("successful")
}

func (w *Web) ackHandler(ctx *fasthttp.RequestCtx) {
	log.Printf("ack(): recieved %q", "yummy")
	err := w.server.Ack()
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
	case "/ack":
		w.ackHandler(ctx)
	}
}
func (w *Web) Serve() error {

	log.Printf("The server is running at %d port", 8080)
	err := fasthttp.ListenAndServe(w.addrs[0]+":8080", w.httpHander)
	return err
}
