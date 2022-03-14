package main

import "github.com/valyala/fasthttp"

func writeHander(ctx *fasthttp.RequestCtx) {
	ctx.WriteString("Hello, world!")
}
func main() {
	fasthttp.ListenAndServe(":8080", writeHander)
}
