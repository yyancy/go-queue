package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"

	"github.com/valyala/fasthttp"
	"github.com/yyancy/go-queue/server"
)

const defaultBufferSize = 64 * 1024

type Client struct {
	addrs     []string
	c         *fasthttp.Client
	buf       bytes.Buffer
	resbuf    bytes.Buffer
	off       uint
	lastChunk string
	curChunk  string
}

func NewClient(addrs []string) (*Client, error) {
	return &Client{addrs: addrs,
		c: &fasthttp.Client{}}, nil
}

func (c *Client) listChunks(addr string) ([]server.Chunk, error) {
	req := fasthttp.AcquireRequest()
	req.SetRequestURI(addr + "/listChunks")
	req.Header.SetMethod(fasthttp.MethodGet)
	resp := fasthttp.AcquireResponse()
	err := c.c.Do(req, resp)
	fasthttp.ReleaseRequest(req)

	if err != nil {
		fasthttp.ReleaseResponse(resp)
		log.Fatalf("ERR Connection error: %s\n", err)
	}
	var res []server.Chunk
	if err := json.NewDecoder(bytes.NewReader(resp.Body())).Decode(&res); err != nil {
		return nil, err
	}
	return res, nil
}

func (c *Client) Send(msg []byte) error {
	if len(msg) == 0 {
		return errors.New("no content to send")
	}
	// _, err := c.buf.Write(msg)
	addrIdx := rand.Intn(len(c.addrs))
	readURL := c.addrs[addrIdx]
	req := fasthttp.AcquireRequest()
	req.SetRequestURI(fmt.Sprintf("%s/write", readURL))
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

func (c *Client) updateCurrentChunk(addr string) error {
	if c.curChunk != "" {
		return nil
	}
	chunks, err := c.listChunks(addr)
	// log.Printf("chunks=%v", chunks)
	if err != nil {
		return fmt.Errorf("listChunks failed: %v", err)
	}

	if len(chunks) == 0 {
		return io.EOF
	}
	c.curChunk = chunks[0].Name
	return nil
}

func (c *Client) Recv(buf []byte) ([]byte, error) {
	if buf == nil {
		buf = make([]byte, defaultBufferSize)
	}
	req := fasthttp.AcquireRequest()
	addrIdx := rand.Intn(len(c.addrs))
	readURL := c.addrs[addrIdx]

	if err := c.updateCurrentChunk(readURL); err != nil {
		return nil, fmt.Errorf("updateCurrentChunk %w", err)
	}

	addr := fmt.Sprintf("%s/read?off=%d&maxSize=%d&chunk=%s", readURL, c.off, uint(len(buf)), c.curChunk)
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
			return nil, fmt.Errorf("ack current chunk %v:", err)
		}
		c.curChunk = ""
		c.off = 0
		return c.Recv(buf)

	}
	c.off += uint(len(b))
	// log.Printf("Send Response: %s\n", b)
	fasthttp.ReleaseResponse(resp)

	return b, nil
}

func (c *Client) ackCurrentChunk(addr string) error {
	req := fasthttp.AcquireRequest()
	req.SetRequestURI(fmt.Sprintf(addr+"/ack?chunk=%s", c.curChunk))
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
