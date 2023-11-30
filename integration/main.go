package integration

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/valyala/fasthttp"
	"github.com/yyancy/go-queue/server"
	"github.com/yyancy/go-queue/web"
)

func writeHander(ctx *fasthttp.RequestCtx) {
	ctx.WriteString("Hello, world!")
}

// InitAndServe checks validity of the suppliied arguments and
// the web server on the specificed port
func InitAndServe(dirname string, port uint) error {
	var storage web.Storage
	fp, err := os.OpenFile(filepath.Join(dirname, "write_test"), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("could not create test file %q, %s", dirname, err)
	}
	fp.Close()
	os.Remove(fp.Name())

	storage, err = server.NewOnDisk(dirname)
	if err != nil {
		return fmt.Errorf("server.NewOnDisk: %v", err)
	}

	w, _ := web.NewWeb(storage, []string{"localhost"}, port)
	return w.Serve()
}
