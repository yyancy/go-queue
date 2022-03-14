package client

import (
	"bytes"
	"errors"
)

const defaultBufferSize = 64 * 1024

type Client struct {
	addrs []string

	buf    bytes.Buffer
	resbuf bytes.Buffer
}

func NewClient(addrs []string) (*Client, error) {
	return &Client{addrs: addrs}, nil
}

func (c *Client) Send(msg []byte) error {
	_, err := c.buf.Write(msg)
	return err
}

func (c *Client) Recv(buf []byte) ([]byte, error) {
	if buf == nil {
		buf = make([]byte, defaultBufferSize)
	}
	curbuf := buf
	curLen := 0
	if c.resbuf.Len() > 0 {
		n, err := c.resbuf.Read(curbuf)
		curLen = n
		if err != nil {
			return nil, err
		}
		c.resbuf.Reset()
		curbuf = buf[n:]
	}
	n, err := c.buf.Read(curbuf)
	curLen += n
	if err != nil {
		return nil, err
	}
	res := buf[0:curLen]

	truncated, rest, err := cutLast(res)
	if len(rest) > 0 {
		_, err := c.resbuf.Write(rest)
		if err != nil {
			return nil, err
		}
	}
	// log.Printf("len(rest): %d", len(rest))
	// 123
	return truncated, nil
}

func cutLast(buf []byte) (msg []byte, rest []byte, err error) {
	n := len(buf)
	if n == 0 || buf[n-1] == '\n' {
		return buf, nil, nil
	}

	lastI := bytes.LastIndexByte(buf, '\n')
	if lastI == -1 {
		return nil, nil, errors.New("too small buffer")
	}

	// log.Printf("lastI: %d, unread: %d", lastI, n-lastI+1)
	// // 123
	// for i := 0; i < n-lastI+1; i++ {
	// 	c.buf.UnreadByte()
	// }

	return buf[:lastI+1], buf[lastI+1:], nil
}
