package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/valyala/fasthttp"
	"github.com/yyancy/go-queue/protocol"
	"github.com/yyancy/go-queue/server"
	"go.etcd.io/etcd/client"
)

const defaultBufferSize = 512 * 1024

type Web struct {
	port     uint
	dirname  string
	kapi     client.KeysAPI
	m        sync.Mutex
	storages map[string]*server.OnDisk
}

type Storage interface {
	Send(msg []byte) error
	Recv(chunk string, off uint, maxSize uint, w io.Writer) error
	Ack(chunk string, size uint64) error
	ListChunks() ([]protocol.Chunk, error)
}

func NewWeb(kapi client.KeysAPI, dirname string, port uint) (w *Web, err error) {
	return &Web{
			kapi:     kapi,
			dirname:  dirname,
			port:     port,
			storages: make(map[string]*server.OnDisk)},
		nil
}
func (w *Web) errorHandler(err error, ctx *fasthttp.RequestCtx) {
	if err != io.EOF {
		ctx.SetStatusCode(http.StatusInternalServerError)
		ctx.WriteString("internal server error:" + err.Error())
		// log.Printf("internal server error:" + err.Error())
		// debug.PrintStack()
	}
}

func isValidCategory(category string) bool {
	if category == "" {
		return false
	}
	cleanPath := filepath.Clean(category)
	if cleanPath != category {
		return false
	}
	if strings.ContainsAny(category, `/\.`) {
		return false
	}
	return true
}
func (w *Web) getStorageByCategory(category string) (*server.OnDisk, error) {
	if !isValidCategory(category) {
		return nil, errors.New("Invalid category: " + category)
	}
	w.m.Lock()
	defer w.m.Unlock()

	storage, ok := w.storages[category]
	if ok {
		return storage, nil
	}
	dir := filepath.Join(w.dirname, category)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, fmt.Errorf("creating directory for the category: %v", err)
	}
	storage, err := server.NewOnDisk(dir)
	if err != nil {
		return nil, err
	}
	w.storages[category] = storage
	return storage, nil
}

func (w *Web) readHandler(ctx *fasthttp.RequestCtx) {
	storage, err := w.getStorageByCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		w.errorHandler(err, ctx)
		return
	}
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
	err = storage.Recv(string(chunk), uint(off), uint(maxSize), ctx)
	if err != nil {
		w.errorHandler(err, ctx)
		return
	}

}
func (w *Web) writeHandler(ctx *fasthttp.RequestCtx) {
	storage, err := w.getStorageByCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		w.errorHandler(err, ctx)
		return
	}
	b := ctx.PostBody()
	// log.Printf("write(): recieved %q", string(b))
	err = storage.Send(b)
	if err != nil {
		w.errorHandler(err, ctx)
	}
	ctx.WriteString("successful\n")
}

func (w *Web) listChunksHandler(ctx *fasthttp.RequestCtx) {
	storage, err := w.getStorageByCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		w.errorHandler(err, ctx)
		return
	}

	chunks, err := storage.ListChunks()
	// log.Printf("chunks=%v", chunks)
	if err != nil {
		w.errorHandler(err, ctx)
	}
	json.NewEncoder(ctx).Encode(chunks)
}

func (w *Web) ackHandler(ctx *fasthttp.RequestCtx) {
	storage, err := w.getStorageByCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		w.errorHandler(err, ctx)
		return
	}

	chunk := ctx.QueryArgs().Peek("chunk")
	if len(chunk) == 0 {
		w.errorHandler(errors.New("not found `chunk` param"), ctx)
		return
	}
	size, err := ctx.QueryArgs().GetUint("size")
	if err != nil {
		w.errorHandler(errors.New("not found `size` param"), ctx)
		return
	}
	// log.Printf("ack(): recieved chunk=`%s`", chunk)
	if err := storage.Ack(string(chunk), uint64(size)); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
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
	err := fasthttp.ListenAndServe("localhost:"+fmt.Sprint(w.port), w.httpHander)
	return err
}
