package integration

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/phayes/freeport"
	"github.com/yyancy/go-queue/client"
)

const (
	maxN          = 10000000
	maxBufferSize = 1024 * 1024
)

func TestSimpleClientAndServer(t *testing.T) {

	p, err := freeport.GetFreePort()
	if err != nil {
		log.Fatal(err)
	}
	port := uint(p)
	dbPath, err := os.MkdirTemp(os.TempDir(), "go-queue")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	os.RemoveAll(dbPath)
	os.Mkdir(dbPath, 0777)

	ioutil.WriteFile(filepath.Join(dbPath, "chunk1"), []byte("12345\n"), 0666)

	errCh := make(chan error, 1)
	go func() {
		errCh <- InitAndServe(dbPath, port)
	}()

	log.Printf("Waiting for the port 127.0.0.1:%d to open", port)
	for i := 0; i <= 100; i++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("InitAndServe(%s, %d): %v", dbPath, port, err)
			}
		default:
		}

		timeout := time.Millisecond * 50
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", fmt.Sprint(port)), timeout)
		if err != nil {
			time.Sleep(timeout)
			continue
		}
		conn.Close()
		break
	}
	log.Printf("Starting the test")

	c, _ := client.NewClient([]string{"http://127.0.0.1:" + fmt.Sprint(port)})

	want, got, err := sendAndRecvConcurrently(c)
	if err != nil {
		t.Fatalf("sendAndRecvConcurrently(): %v", err)
	}
	want += 12345
	if want != got {
		t.Fatalf("The expected sum %d is not equal to the actual sum %d (delivered %1.f%%)", want, got, (float64(got)/float64(want))*100)
	}
}

type SumAndError struct {
	sum int64
	err error
}

func sendAndRecvConcurrently(c *client.Client) (want, got int64, err error) {
	wantCh := make(chan SumAndError, 1)
	gotCh := make(chan SumAndError, 1)
	completeCh := make(chan bool, 1)
	// produrer
	go func() {
		want, err := send(c)
		wantCh <- SumAndError{
			sum: want,
			err: err,
		}
		// log.Print("already send all data")
		completeCh <- true
	}()
	// cumsomer
	go func() {
		// time.Sleep(1000 * time.Millisecond)
		got, err := recv(c, completeCh)
		gotCh <- SumAndError{
			sum: got,
			err: err,
		}
	}()

	wantRes := <-wantCh
	if wantRes.err != nil {
		return 0, 0, fmt.Errorf("send: %v", wantRes.err)
	}
	gotRes := <-gotCh
	if gotRes.err != nil {
		return 0, 0, fmt.Errorf("send: %v", gotRes.err)
	}
	return wantRes.sum, gotRes.sum, nil
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
	// log.Printf("%d", b.Len())
	if b.Len() > 0 {
		if err := c.Send(b.Bytes()); err != nil {
			return 0, err
		}
	}
	return sum, nil

}
func recv(c *client.Client, completeCh chan bool) (sum int64, err error) {
	buf := make([]byte, maxBufferSize)
	sum = 0
	sendFinished := false
	for {
		select {
		case <-completeCh:
			// log.Printf("Receive: got information that send finished")
			sendFinished = true
		default:
		}
		res, err := c.Recv(buf)
		if errors.Is(err, io.EOF) {
			// log.Printf("EOF happened")
			if sendFinished {
				return sum, nil
			}
			// time.Sleep(100 * time.Millisecond)
			continue
		} else if err != nil {
			return 0, err
		}
		// log.Printf("received %s", res)
		ints := strings.Split(string(res), "\n")
		for _, ish := range ints {
			if ish == "" {
				continue
			}
			i, err := strconv.Atoi(ish)
			if err != nil {
				return 0, err
			}
			// if idx < 2 {
			// 	log.Printf("recived number=%d", i)
			// }
			sum += int64(i)
		}
	}

}
