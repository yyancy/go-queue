package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/yyancy/go-queue/client"
)

const maxN = 10000000
const maxBufferSize = 1024 * 1024

func main() {
	c, _ := client.NewClient([]string{"yancy"})
	want, err := send(c)
	if err != nil {
		log.Fatalf("Send error: %v", err)
	}
	got, err := recv(c)
	if err != nil {
		log.Fatalf("Recv error: %v", err)
	}

	if want != got {
		log.Fatalf("The expected sum %d is not equal to the actual sum %d", want, got)
	}
	log.Printf("The test passed")
}

func send(c *client.Client) (sum int64, err error) {
	var b bytes.Buffer

	for i := 0; i < maxN; i++ {
		sum += int64(i)
		fmt.Fprintf(&b, "%d\n", i)

		if b.Len() >= maxBufferSize {
			if err := c.Send(b.Bytes()); err != nil {
				return 0, err
			}
			b.Reset()
		}
	}
	log.Printf("%d", b.Len())
	if b.Len() > 0 {
		if err := c.Send(b.Bytes()); err != nil {
			return 0, err
		}
	}
	return sum, nil

}
func recv(c *client.Client) (sum int64, err error) {
	buf := make([]byte, maxBufferSize)
	sum = 0
	for {
		res, err := c.Recv(buf)
		if err == io.EOF {
			return sum, nil
		} else if err != nil {
			return 0, err
		}
		ints := strings.Split(string(res), "\n")
		for _, ish := range ints {
			if ish == "" {
				continue
			}
			i, err := strconv.Atoi(ish)
			if err != nil {
				return 0, err
			}
			sum += int64(i)
		}
	}

}
