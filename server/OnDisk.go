package server

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
)

const defaultBlockSize = 8 * 1024 * 1024
const maxFileChunkSize = 20 * 1024 * 1024

type OnDisk struct {
	dirname string

	sync.RWMutex
	lastChunk     string
	lastChunkSize uint64
	lastChunkIdx  uint64
	fps           map[string]*os.File
}

var filenameRegexp = regexp.MustCompile("^chunk([0-9]+)$")

func NewOnDisk(dirname string) (*OnDisk, error) {
	s := &OnDisk{dirname: dirname, fps: make(map[string]*os.File)}
	files, err := os.ReadDir(dirname)
	if err != nil {
		return nil, fmt.Errorf("ReadDir: %v", err)
	}
	// find the existing maximum index of chunks
	for _, file := range files {
		res := filenameRegexp.FindStringSubmatch(file.Name())
		if res == nil {
			continue
		}
		chunkIdx, err := strconv.Atoi(res[1])
		if err != nil {
			return nil, fmt.Errorf("strconv.atoi(%s): %v", res[1], err)
		}
		// log.Printf("chunkIdx=%d", chunkIdx)
		if uint64(chunkIdx)+1 > s.lastChunkIdx {
			s.lastChunkIdx = uint64(chunkIdx) + 1
		}
	}

	return s, nil
}

func (c *OnDisk) Send(msg []byte) error {
	if len(msg) == 0 {
		return errors.New("no content to send")
	}
	c.Lock()
	defer c.Unlock()
	if c.lastChunk == "" || (c.lastChunkSize+uint64(len(msg))) > maxFileChunkSize {
		c.lastChunk = fmt.Sprintf("chunk%d", c.lastChunkIdx)
		c.lastChunkSize = 0
		c.lastChunkIdx++
	}
	fp, err := c.getFileDecriptor(c.lastChunk)
	if err != nil {
		return err
	}
	_, err = fp.Write(msg)
	c.lastChunkSize += uint64(len(msg))
	return err
}

func (c *OnDisk) getFileDecriptor(chunk string) (*os.File, error) {
	fp, ok := c.fps[chunk]
	if ok {
		return fp, nil
	}
	fp, err := os.OpenFile(filepath.Join(c.dirname, chunk), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("Could not create chunk file %q: %s", fp.Name(), err)
	}
	c.fps[chunk] = fp
	return fp, nil
}

func (c *OnDisk) Recv(chunk string, off uint, maxSize uint, w io.Writer) error {
	c.RLock()
	defer c.RUnlock()
	chunk = filepath.Clean(chunk)
	_, err := os.Stat(filepath.Join(c.dirname, chunk))
	if err != nil {
		return fmt.Errorf("stat %q: %w", chunk, err)
	}
	fp, err := c.getFileDecriptor(chunk)
	if err != nil {
		return fmt.Errorf("getFileDecriptor(%q): %v", chunk, err)
	}
	buf := make([]byte, defaultBlockSize)
	var alreadySendByte uint = 0
	curOff := off
	for {

		n, err := fp.ReadAt(buf, int64(curOff))
		// there is occaion: err== io.EOF but n != 0,
		// which means it has read to the end but has data not sent
		if n == 0 {
			if err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
		}
		// log.Printf("alreadySendByte = %d,maxSize = %d", alreadySendByte, maxSize)
		if alreadySendByte+uint(n) > maxSize {
			toSend := maxSize - alreadySendByte
			truncated, _, err := cutLast(buf[0:toSend])
			if err == errSmallBuffer {
				return nil
			} else if err != nil {
				return err
			}
			if _, err = w.Write(truncated); err != nil {
				return err
			}
			// already send to maxSize return to main frame
			return nil
		}
		truncated, _, err := cutLast(buf[0:n])
		if err == errSmallBuffer {
			return nil
		} else if err != nil {
			return err
		}
		if _, err = w.Write(truncated); err != nil {
			return err
		}
		alreadySendByte += uint(len(truncated))
		curOff += uint(len(truncated))

	}

}
func (c *OnDisk) ListChunks() ([]Chunk, error) {
	var res []Chunk

	dis, err := os.ReadDir(c.dirname)
	if err != nil {
		return nil, err
	}

	for _, di := range dis {
		c := Chunk{
			Name:     di.Name(),
			Complete: (di.Name() != c.lastChunk),
		}
		res = append(res, c)
	}
	// log.Printf("chunks %v", res)
	return res, nil
}

func (c *OnDisk) Ack(chunk string) error {
	c.Lock()
	defer c.Unlock()

	if chunk == c.lastChunk {
		return fmt.Errorf("could not delete incomplete chunk %q", chunk)
	}

	chunkFilename := filepath.Join(c.dirname, chunk)

	fp, ok := c.fps[chunk]
	if ok {
		fp.Close()
	}
	_, err := os.Stat(chunkFilename)
	if err != nil {
		return fmt.Errorf("stat %q: %w", chunk, err)
	}

	if err := os.Remove(chunkFilename); err != nil {
		return fmt.Errorf("removing %q: %v", chunk, err)
	}

	delete(c.fps, chunk)
	return nil
}
