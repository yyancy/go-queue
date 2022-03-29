package web

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/valyala/fasthttp"
	"github.com/yyancy/go-queue/server"
)

const defaultBufferSize = 512 * 1024

type Web struct {
	addrs  []string
	port   uint
	server Storage
}

type Storage interface {
	Send(msg []byte) error
	Recv(chunk string, off uint, maxSize uint, w io.Writer) error
	Ack(chunk string) error
	ListChunks() ([]server.Chunk, error)
}

func NewWeb(s Storage, addrs []string, port uint) (w *Web, err error) {
	return &Web{server: s, addrs: addrs, port: port}, nil
}
func (w *Web) errorHandler(err error, ctx *fasthttp.RequestCtx) {
	if err != io.EOF {
		ctx.SetStatusCode(http.StatusInternalServerError)
		ctx.WriteString("internal server error:" + err.Error())
		log.Printf("internal server error:" + err.Error())
		// debug.PrintStack()
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
	chunk := ctx.QueryArgs().Peek("chunk")
	err = w.server.Recv(string(chunk), uint(off), uint(maxSize), ctx)
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
	ctx.WriteString("successful\n")
}

func (w *Web) listChunksHandler(ctx *fasthttp.RequestCtx) {
	chunks, err := w.server.ListChunks()
	if err != nil {
		w.errorHandler(err, ctx)
	}
	json.NewEncoder(ctx).Encode(chunks)
}

func (w *Web) ackHandler(ctx *fasthttp.RequestCtx) {
	chunk := ctx.QueryArgs().Peek("chunk")
	log.Printf("ack(): recieved chunk=`%s`", chunk)
	err := w.server.Ack(string(chunk))
	if err != nil {
		w.errorHandler(err, ctx)
	} else {
		ctx.WriteString("successful\n")
	}
}
func (w *Web) httpHander(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/read":
		w.readHandler(ctx)
	case "/write":
		w.writeHandler(ctx)
	case "/ack":
		w.ackHandler(ctx)
	case "/listChunks":
		w.listChunksHandler(ctx)
	}
}
func (w *Web) Serve() error {

	log.Printf("The server is running at %d port", w.port)
	err := fasthttp.ListenAndServe(w.addrs[0]+":"+fmt.Sprint(w.port), w.httpHander)
	return err
}
