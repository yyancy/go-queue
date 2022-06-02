package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/url"
	"strconv"

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

// ListChunks return the list of chunks for the appropriate
// TODO extract
func (c *Client) ListChunks(category, addr string) ([]protocol.Chunk, error) {
	req := fasthttp.AcquireRequest()
	u := url.Values{}
	u.Add("category", category)
	req.SetRequestURI(addr + "/listChunks?" + u.Encode())
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
	body := resp.Body()
	b := make([]byte, len(body))
	copy(b, body)

	// log.Printf("buffer = %s, resp.body = %s", b, resp.Body())
	if err := json.NewDecoder(bytes.NewReader(b)).Decode(&res); err != nil {
		return nil, err
	}
	return res, nil
}

func (c *Client) Send(category string, msg []byte) error {
	if len(msg) == 0 {
		return errors.New("no content to send")
	}
	// _, err := c.buf.Write(msg)
	addrIdx := rand.Intn(len(c.addrs))
	readURL := c.addrs[addrIdx]
	u := url.Values{}
	u.Add("category", category)
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI(fmt.Sprintf("%s/write?%s", readURL, u.Encode()))
	req.Header.SetMethod(fasthttp.MethodPost)
	req.SetBody(msg)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	err := c.c.Do(req, resp)
	if err != nil {
		log.Fatalf("ERR Connection error: %s\n", err)
		return err
	}

	if resp.StatusCode() != fasthttp.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, bytes.NewReader(resp.Body()))
		return fmt.Errorf("http code %d, %s", resp.StatusCode(), b.String())
	}
	// log.Printf("Send Response: %s\n", resp.Body())
	return nil
}

func (c *Client) updateCurrentChunk(category, addr string) error {
	if c.curChunk.Name != "" {
		return nil
	}
	// log.Printf("updateCurrentChunk %s", addr)
	chunks, err := c.ListChunks(category, addr)
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

func (c *Client) updateCurrentChunkCompleteStatus(category, addr string) error {
	chunks, err := c.ListChunks(category, addr)
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
func (c *Client) Process(category string, buf []byte, processFn func([]byte) error) error {
	if buf == nil {
		buf = make([]byte, defaultBufferSize)
	}
	req := fasthttp.AcquireRequest()
	addrIdx := rand.Intn(len(c.addrs))
	readURL := c.addrs[addrIdx]

	if err := c.updateCurrentChunk(category, readURL); err != nil {
		return fmt.Errorf("updateCurrentChunk %w", err)
	}
	u := url.Values{}
	u.Add("off", strconv.Itoa(int(c.off)))
	u.Add("maxSize", strconv.Itoa(len(buf)))
	u.Add("chunk", c.curChunk.Name)
	u.Add("category", category)
	addr := fmt.Sprintf("%s/read?%s", readURL, u.Encode())
	req.SetRequestURI(addr)
	req.Header.SetMethod(fasthttp.MethodGet)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	err := c.c.Do(req, resp)
	if err != nil {
		return fmt.Errorf("read %q: %v", readURL, err)
	}
	fasthttp.ReleaseRequest(req)

	if resp.StatusCode() != fasthttp.StatusOK {
		var b bytes.Buffer
		r := bytes.NewReader(resp.Body())
		io.Copy(&b, r)
		return fmt.Errorf("http code %d, %s", resp.StatusCode(), b.String())
	}
	body := resp.Body()
	b := make([]byte, len(body))
	copy(b, body)
	if len(b) == 0 {
		if !c.curChunk.Complete {
			if err := c.updateCurrentChunkCompleteStatus(category, readURL); err != nil {
				return fmt.Errorf("updateCurrentChunkCompleteStatus(%s) failed %v", addr, err)
			}
		}
		if !c.curChunk.Complete {
			return io.EOF
		}
		if err := c.ackCurrentChunk(category, readURL); err != nil {
			return fmt.Errorf("ack current chunk %w:", err)
		}
		c.curChunk = protocol.Chunk{}
		c.off = 0
		return c.Process(category, buf, processFn)

	}
	if err := processFn(b); err == nil {
		c.off += uint(len(b))
	}

	return nil
}

func (c *Client) ackCurrentChunk(category, addr string) error {
	req := fasthttp.AcquireRequest()
	// log.Printf("curChunk=%q", c.curChunk)

	u := url.Values{}
	u.Add("chunk", c.curChunk.Name)
	u.Add("size", strconv.Itoa(int(c.off)))
	u.Add("category", category)
	req.SetRequestURI(fmt.Sprintf(addr+"/ack?%s", u.Encode()))
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
