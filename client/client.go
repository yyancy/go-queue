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
	"github.com/yyancy/go-queue/protocol"
)

const defaultBufferSize = 64 * 1024

type Client struct {
	addrs    []string
	c        *fasthttp.Client
	off      uint
	curChunk protocol.Chunk
}

func NewClient(addrs []string) (*Client, error) {
	return &Client{addrs: addrs,
		c: &fasthttp.Client{}}, nil
}

func (c *Client) listChunks(addr string) ([]protocol.Chunk, error) {
	req := fasthttp.AcquireRequest()
	req.SetRequestURI(addr + "/listChunks")
	req.Header.SetMethod(fasthttp.MethodGet)
	resp := fasthttp.AcquireResponse()
	err := c.c.Do(req, resp)
	fasthttp.ReleaseRequest(req)
	// log.Printf("received chunks %v", string(resp.Body()))
	if err != nil {
		fasthttp.ReleaseResponse(resp)
		log.Fatalf("ERR Connection error: %s\n", err)
	}
	var res []protocol.Chunk
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
	if c.curChunk.Name != "" {
		return nil
	}
	// log.Printf("updateCurrentChunk %s", addr)
	chunks, err := c.listChunks(addr)
	// log.Printf("chunks=%v", chunks)
	if err != nil {
		return fmt.Errorf("listChunks failed: %v", err)
	}
	// there is no chunk
	if len(chunks) == 0 {
		return io.EOF
	}
	// We need to prioritise the chunks that are complete
	// so that we ack them.
	for _, ch := range chunks {
		if ch.Complete {
			c.curChunk = ch
			return nil
		}
	}
	c.curChunk = chunks[0]
	return nil
}

func (c *Client) updateCurrentChunkCompleteStatus(addr string) error {
	chunks, err := c.listChunks(addr)
	// log.Printf("chunks=%v", chunks)
	if err != nil {
		return fmt.Errorf("listChunks failed: %v", err)
	}

	// We need to prioritise the chunks that are complete
	// so that we ack them.
	for _, ch := range chunks {
		if c.curChunk.Name == ch.Name {
			c.curChunk = ch
			return nil
		}
	}
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

	addr := fmt.Sprintf("%s/read?off=%d&maxSize=%d&chunk=%s", readURL, c.off, uint(len(buf)), c.curChunk.Name)
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
		if !c.curChunk.Complete {
			if err := c.updateCurrentChunkCompleteStatus(readURL); err != nil {
				return nil, fmt.Errorf("updateCurrentChunkCompleteStatus(%s) failed %v", addr, err)
			}
		}
		if !c.curChunk.Complete {
			return nil, io.EOF
		}
		if err := c.ackCurrentChunk(readURL); err != nil {
			return nil, fmt.Errorf("ack current chunk %w:", err)
		}
		c.curChunk = protocol.Chunk{}
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
	// log.Printf("curChunk=%q", c.curChunk)
	req.SetRequestURI(fmt.Sprintf(addr+"/ack?chunk=%s&size=%d", c.curChunk.Name, c.off))
	req.Header.SetMethod(fasthttp.MethodGet)
	resp := fasthttp.AcquireResponse()
	err := c.c.Do(req, resp)
	fasthttp.ReleaseRequest(req)
	if resp.StatusCode() != fasthttp.StatusOK {
		return fmt.Errorf("http code %d, %s", resp.StatusCode(), string(resp.Body()))
		// return io.EOF
	}
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
