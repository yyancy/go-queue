package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"

	"github.com/yyancy/go-queue/protocol"
)

const defaultBlockSize = 8 * 1024 * 1024
const maxFileChunkSize = 20 * 1024 * 1024

var errSmallBuffer = errors.New("too small buffer")

type OnDisk struct {
	dirname string

	writeMu       sync.Mutex
	lastChunk     string
	lastChunkSize uint64
	lastChunkIdx  uint64
	fpsMu         sync.Mutex
	fps           map[string]*os.File
}

var filenameRegexp = regexp.MustCompile("^chunk([0-9]+)$")

func NewOnDisk(dirname string) (*OnDisk, error) {
	s := &OnDisk{dirname: dirname, fps: make(map[string]*os.File)}

	if err := s.initLastChunkIdx(); err != nil {
		return nil, err
	}
	return s, nil
}
func (c *OnDisk) initLastChunkIdx() error {
	files, err := os.ReadDir(c.dirname)
	if err != nil {
		return fmt.Errorf("ReadDir: %v", err)
	}
	// find the existing maximum index of chunks
	for _, file := range files {
		res := filenameRegexp.FindStringSubmatch(file.Name())
		if res == nil {
			continue
		}
		chunkIdx, err := strconv.Atoi(res[1])
		if err != nil {
			return fmt.Errorf("strconv.atoi(%s): %v", res[1], err)
		}
		// log.Printf("chunkIdx=%d", chunkIdx)
		if uint64(chunkIdx)+1 > c.lastChunkIdx {
			c.lastChunkIdx = uint64(chunkIdx) + 1
		}
	}
	return err
}

func (c *OnDisk) Send(msg []byte) error {
	// time.Sleep(time.Millisecond * 100)

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.lastChunk == "" || (c.lastChunkSize+uint64(len(msg))) > maxFileChunkSize {
		c.lastChunk = fmt.Sprintf("chunk%d", c.lastChunkIdx)
		c.lastChunkSize = 0
		c.lastChunkIdx++
	}
	fp, err := c.getFileDecriptor(c.lastChunk, true)

	if err != nil {
		return err
	}

	_, err = fp.Write(msg)
	c.lastChunkSize += uint64(len(msg))
	return err
}

func (c *OnDisk) getFileDecriptor(chunk string, write bool) (*os.File, error) {
	c.fpsMu.Lock()
	defer c.fpsMu.Unlock()

	fp, ok := c.fps[chunk]
	if ok {
		return fp, nil
	}

	fl := os.O_RDONLY
	if write {
		fl = os.O_CREATE | os.O_RDWR | os.O_EXCL
	}

	filename := filepath.Join(c.dirname, chunk)
	fp, err := os.OpenFile(filename, fl, 0666)
	if err != nil {
		return nil, fmt.Errorf("Could not create chunk file %q: %s", filename, err)
	}

	c.fps[chunk] = fp
	return fp, nil
}

func (c *OnDisk) Recv(chunk string, off uint, maxSize uint, w io.Writer) error {
	// time.Sleep(time.Millisecond * 100)
	chunk = filepath.Clean(chunk)
	_, err := os.Stat(filepath.Join(c.dirname, chunk))
	if err != nil {
		return fmt.Errorf("stat %q: %w", chunk, err)
	}

	fp, err := c.getFileDecriptor(chunk, false)
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
func (c *OnDisk) forgetFileDescriptor(chunk string) {
	c.fpsMu.Lock()
	defer c.fpsMu.Unlock()

	fp, ok := c.fps[chunk]
	if !ok {
		return
	}
	fp.Close()
	delete(c.fps, chunk)
}
func (c *OnDisk) ListChunks() ([]protocol.Chunk, error) {
	var res []protocol.Chunk

	dis, err := os.ReadDir(c.dirname)
	if err != nil {
		return nil, err
	}

	for _, di := range dis {
		fi, err := di.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("reading directory: %v", err)
		}
		c.writeMu.Lock()
		ch := protocol.Chunk{
			Name:     di.Name(),
			Complete: (di.Name() != c.lastChunk),
			Size:     uint64(fi.Size()),
		}
		c.writeMu.Unlock()

		res = append(res, ch)
	}
	// log.Printf("chunks %v", res)
	return res, nil
}
func (s *OnDisk) isLastChunk(chunk string) bool {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	return chunk == s.lastChunk
}
func (c *OnDisk) Ack(chunk string, size uint64) error {
	if c.isLastChunk(chunk) {
		return fmt.Errorf("Could not delete incomplete chunk %q", chunk)
	}

	chunkFilename := filepath.Join(c.dirname, chunk)

	_, err := os.Stat(chunkFilename)
	if err != nil {
		return fmt.Errorf("stat %q: %w", chunk, err)
	}

	if err := os.Remove(chunkFilename); err != nil {
		return fmt.Errorf("removing %q: %v", chunk, err)
	}

	c.forgetFileDescriptor(chunk)
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
