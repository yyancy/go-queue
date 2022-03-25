package server

import (
	"errors"
	"io"
	"os"
)

const defaultBlockSize = 8 * 1024 * 1024

type OnDisk struct {
	fp *os.File
}

func NewOnDisk(fp *os.File) *OnDisk {
	return &OnDisk{fp}
}

func (c *OnDisk) Send(msg []byte) error {
	if len(msg) == 0 {
		return errors.New("no content to send")
	}
	_, err := c.fp.Write(msg)
	return err
}

func (c *OnDisk) Recv(off uint, maxSize uint, w io.Writer) error {
	buf := make([]byte, defaultBlockSize)
	var alreadySendByte uint = 0
	curOff := off
	for {

		n, err := c.fp.ReadAt(buf, int64(curOff))
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
func (c *OnDisk) Ack() error {
	c.fp.Truncate(0)
	c.fp.Seek(0, 0)
	return nil
}
