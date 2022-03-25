package server

import (
	"bytes"
	"errors"
	"io"
)

var errSmallBuffer = errors.New("too small buffer")

const defaultBufferSize = 64 * 1024

type InMemory struct {
	buf []byte
}

func NewServer() (*InMemory, error) {
	return &InMemory{}, nil
}

func (c *InMemory) Send(msg []byte) error {
	if len(msg) == 0 {
		return errors.New("no content to send")
	}
	c.buf = append(c.buf, msg...)
	return nil
}

func (c *InMemory) Recv(off uint, maxSize uint, w io.Writer) error {
	if off > uint(len(c.buf)) {
		return nil
	}
	if off+maxSize > uint(len(c.buf)) {
		w.Write(c.buf[off:])
		return nil
	}

	truncated, _, err := cutLast(c.buf[off : off+maxSize])
	if err != nil {
		return err
	}
	if _, err := w.Write(truncated); err != nil {
		return err
	}
	return nil

}
func (c *InMemory) Ack() error {
	c.buf = c.buf[0:0]
	return nil
}

func cutLast(buf []byte) (msg []byte, rest []byte, err error) {
	n := len(buf)
	if n == 0 || buf[n-1] == '\n' {
		return buf, nil, nil
	}

	lastI := bytes.LastIndexByte(buf, '\n')
	if lastI == -1 {
		return nil, nil, errSmallBuffer
	}
	return buf[:lastI+1], buf[lastI+1:], nil
}
