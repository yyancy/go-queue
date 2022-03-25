package client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"

	"github.com/valyala/fasthttp"
)

const defaultBufferSize = 64 * 1024

type Client struct {
	addrs  []string
	c      *fasthttp.Client
	buf    bytes.Buffer
	resbuf bytes.Buffer
	off    uint
}

func NewClient(addrs []string) (*Client, error) {
	return &Client{addrs: addrs,
		c: &fasthttp.Client{}}, nil
}

func (c *Client) Send(msg []byte) error {
	if len(msg) == 0 {
		return errors.New("no content to send")
	}
	// _, err := c.buf.Write(msg)
	req := fasthttp.AcquireRequest()
	req.SetRequestURI("http://localhost:8080/write")
	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetBody(msg)
	resp := fasthttp.AcquireResponse()
	err := c.c.Do(req, resp)
	fasthttp.ReleaseRequest(req)
	if err != nil {
		fasthttp.ReleaseResponse(resp)
		log.Fatalf("ERR Connection error: %s\n", err)
	}
	// log.Printf("Send Response: %s\n", resp.Body())
	fasthttp.ReleaseResponse(resp)
	return nil
}

func (c *Client) Recv(buf []byte) ([]byte, error) {
	if buf == nil {
		buf = make([]byte, defaultBufferSize)
	}
	req := fasthttp.AcquireRequest()
	addrIdx := rand.Intn(len(c.addrs))
	readURL := c.addrs[addrIdx]
	addr := fmt.Sprintf("%s/read?off=%d&maxSize=%d", readURL, c.off, uint(len(buf)))
	req.SetRequestURI(addr)
	req.Header.SetMethod(fasthttp.MethodGet)
	resp := fasthttp.AcquireResponse()
	err := c.c.Do(req, resp)
	fasthttp.ReleaseRequest(req)

	if err != nil {
		fasthttp.ReleaseResponse(resp)
		log.Fatalf("ERR Connection error: %s\n", err)
	}
	b := resp.Body()
	if len(b) == 0 {
		if err := c.ackCurrentChunk(readURL); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	c.off += uint(len(b))
	// log.Printf("Send Response: %s\n", b)
	fasthttp.ReleaseResponse(resp)

	return b, nil
}
func (c *Client) ackCurrentChunk(addr string) error {
	req := fasthttp.AcquireRequest()
	req.SetRequestURI(addr + "/ack")
	req.Header.SetMethod(fasthttp.MethodGet)
	resp := fasthttp.AcquireResponse()
	err := c.c.Do(req, resp)
	fasthttp.ReleaseRequest(req)

	if err != nil {
		fasthttp.ReleaseResponse(resp)
		log.Fatalf("ERR Connection error: %s\n", err)
	}
	return nil
}

func cutLast(buf []byte) (msg []byte, rest []byte, err error) {
	n := len(buf)
	if n == 0 || buf[n-1] == '\n' {
		return buf, nil, nil
	}

	lastI := bytes.LastIndexByte(buf, '\n')
	if lastI == -1 {
		return nil, nil, errors.New("the buffer is too small to fit the message")
	}
	return buf[:lastI+1], buf[lastI+1:], nil
}
