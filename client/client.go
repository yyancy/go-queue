package client

import "bytes"

const defaultBufferSize = 64 * 1024

type Client struct {
	addrs []string

	buf bytes.Buffer
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

	n, err := c.buf.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[0:n], nil
}
