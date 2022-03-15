package server

import (
	"bytes"
	"errors"
)

const defaultBufferSize = 64 * 1024

type Server struct {
	buf    bytes.Buffer
	resbuf bytes.Buffer
}

func NewServer() (*Server, error) {
	return &Server{}, nil
}

func (c *Server) Send(msg []byte) error {
	if len(msg) == 0 {
		return errors.New("no content to send")
	}
	_, err := c.buf.Write(msg)
	return err
}

func (c *Server) Recv(buf []byte) ([]byte, error) {
	if buf == nil {
		buf = make([]byte, defaultBufferSize)
	}
	curbuf := buf
	curLen := 0
	if c.resbuf.Len() > 0 {
		if c.resbuf.Len() > len(curbuf) {
			return nil, errors.New("The buffer is too small to fit the message")
		}
		n, err := c.resbuf.Read(curbuf)
		curLen = n
		if err != nil {
			return nil, err
		}
		c.resbuf.Reset()
		curbuf = buf[n:]
	}
	n, err := c.buf.Read(curbuf)
	if err != nil {
		return nil, err
	}

	curLen += n
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
	return buf[:lastI+1], buf[lastI+1:], nil
}
