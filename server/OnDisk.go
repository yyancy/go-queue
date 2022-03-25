package server

import (
	"bytes"
	"errors"
)

type OnDisk struct {
	buf    bytes.Buffer
	resbuf bytes.Buffer
}

func NewOnDisk() (*OnDisk, error) {
	return &OnDisk{}, nil
}

func (c *OnDisk) Send(msg []byte) error {
	if len(msg) == 0 {
		return errors.New("no content to send")
	}
	_, err := c.buf.Write(msg)
	return err
}

func (c *OnDisk) Recv(buf []byte) ([]byte, error) {
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
	return truncated, nil
}
