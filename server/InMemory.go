package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
)

var errSmallBuffer = errors.New("too small buffer")

const defaultBufferSize = 64 * 1024
const maxInMemoryChunkSize = 1024 * 1024

type InMemory struct {
	sync.RWMutex
	lastChunk     string
	lastChunkSize uint64
	lastChunkIdx  uint64
	bufs          map[string][]byte
}

func NewServer() (*InMemory, error) {
	return &InMemory{}, nil
}

func (c *InMemory) Send(msg []byte) error {

	if len(msg) == 0 {
		return errors.New("no content to send")
	}
	c.Lock()
	defer c.Unlock()
	if c.lastChunk == "" || (c.lastChunkSize+uint64(len(msg))) > maxInMemoryChunkSize {
		c.lastChunk = fmt.Sprintf("chunk%d", c.lastChunkIdx)
		c.lastChunkSize = 0
		c.lastChunkIdx++
	}
	if c.bufs == nil {
		c.bufs = make(map[string][]byte)
	}
	c.bufs[c.lastChunk] = append(c.bufs[c.lastChunk], msg...)
	c.lastChunkSize += uint64(len(msg))
	return nil
}

func (c *InMemory) Recv(chunk string, off uint, maxSize uint, w io.Writer) error {
	c.RLock()
	defer c.RUnlock()
	buf, ok := c.bufs[chunk]
	if !ok {
		return fmt.Errorf("chunk %q does not exist", chunk)
	}
	if off > uint(len(buf)) {
		return nil
	}
	if off+maxSize > uint(len(buf)) {
		w.Write(buf[off:])
		return nil
	}

	truncated, _, err := cutLast(buf[off : off+maxSize])
	if err != nil {
		return err
	}
	if _, err := w.Write(truncated); err != nil {
		return err
	}
	return nil

}
func (c *InMemory) Ack(chunk string) error {
	c.Lock()
	defer c.Unlock()
	_, ok := c.bufs[chunk]
	if !ok {
		return fmt.Errorf("chunk %q does not exist", chunk)
	}
	delete(c.bufs, chunk)
	return nil
}

func (c *InMemory) ListChunks() ([]Chunk, error) {
	res := make([]Chunk, 0, len(c.bufs))
	for chunk := range c.bufs {
		var ch Chunk
		ch.Complete = c.lastChunk != chunk
		ch.Name = chunk

		res = append(res, ch)
	}
	return res, nil
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
