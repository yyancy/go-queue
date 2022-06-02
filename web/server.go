package web

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"sync"

	"github.com/valyala/fasthttp"
	"github.com/yyancy/go-queue/server"
	"github.com/yyancy/go-queue/server/replication"
)

const defaultBufferSize = 512 * 1024

type Web struct {
	instanceName string
	dirname      string
	listenAddr   string

	replClient  *replication.State
	replStorage *replication.Storage
	getOnDisk   GetOnDiskFn
	m           sync.Mutex
	storages    map[string]*server.OnDisk
}

type GetOnDiskFn func(category string) (*server.OnDisk, error)

// type Storage interface {
// 	Send(msg []byte) error
// 	Recv(chunk string, off uint, maxSize uint, w io.Writer) error
// 	Ack(chunk string, size uint64) error
// 	ListChunks() ([]protocol.Chunk, error)
// }

func NewWeb(
	replClient *replication.State,
	instanceName, dirname string,
	listenAddr string,
	replStorage *replication.Storage,
	getOnDisk GetOnDiskFn,
) (w *Web) {
	return &Web{
		replStorage:  replStorage,
		instanceName: instanceName,
		listenAddr:   listenAddr,
		replClient:   replClient,
		dirname:      dirname,
		getOnDisk:    getOnDisk,
		storages:     make(map[string]*server.OnDisk)}
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
	return w.getOnDisk(category)
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
	err = storage.Send(ctx, b)
	if err != nil {
		w.errorHandler(err, ctx)
		return
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
		return
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

	log.Printf("The server is running at %s port", w.listenAddr)
	err := fasthttp.ListenAndServe(w.listenAddr, w.httpHander)
	return err
}
