package integration

import (
	"errors"
	"fmt"
	"io"
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
	sendFmt       = "Send: net %13s, cpu %13s (%.1f MiB)"
	recvFmt       = "Recv: net %13s, cpu %13s"
)

func TestClientClientAndServerConcurently(t *testing.T) {
	t.Parallel()
	ClientClientAndServerTest(t, true)
}

func TestClientClientAndServerSequentially(t *testing.T) {
	t.Parallel()
	ClientClientAndServerTest(t, false)
}

func ClientClientAndServerTest(t *testing.T, concurrent bool) {
	t.Helper()
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

	os.WriteFile(filepath.Join(dbPath, "chunk1"), []byte("12345\n"), 0666)

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
	var want, got int64
	if concurrent {

		want, got, err = sendAndReceiveConcurrently(c)
		if err != nil {
			t.Fatalf("sendAndRecvConcurrently(): %v", err)
		}
	} else {
		want, err = send(c)
		if err != nil {
			t.Fatalf("send failed: %v", err)
		}
		ch := make(chan bool, 1)
		ch <- true
		got, err = receive(c, ch)
		if err != nil {
			t.Fatalf("recv failed: %v", err)
		}

	}
	want += 12345
	if want != got {
		t.Fatalf("The expected sum %d is not equal to the actual sum %d (delivered %1.f%%)", want, got, (float64(got)/float64(want))*100)
	}
}

type sumAndErr struct {
	err error
	sum int64
}

func sendAndReceiveConcurrently(s *client.Client) (want, got int64, err error) {
	wantCh := make(chan sumAndErr, 1)
	gotCh := make(chan sumAndErr, 1)
	sendFinishedCh := make(chan bool, 1)

	go func() {
		want, err := send(s)
		log.Printf("Send finished")

		wantCh <- sumAndErr{
			sum: want,
			err: err,
		}
		sendFinishedCh <- true
	}()

	go func() {
		got, err := receive(s, sendFinishedCh)
		gotCh <- sumAndErr{
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
		return 0, 0, fmt.Errorf("receive: %v", gotRes.err)
	}

	return wantRes.sum, gotRes.sum, err
}

func send(s *client.Client) (sum int64, err error) {
	sendStart := time.Now()
	var networkTime time.Duration
	var sentBytes int

	defer func() {
		log.Printf(sendFmt, networkTime, time.Since(sendStart)-networkTime, float64(sentBytes)/1024/1024)
	}()

	buf := make([]byte, 0, maxBufferSize)

	for i := 0; i <= maxN; i++ {
		sum += int64(i)

		buf = strconv.AppendInt(buf, int64(i), 10)
		buf = append(buf, '\n')

		if len(buf) >= maxBufferSize {
			start := time.Now()
			if err := s.Send(buf); err != nil {
				return 0, err
			}
			networkTime += time.Since(start)
			sentBytes += len(buf)

			buf = buf[0:0]
		}
	}

	if len(buf) != 0 {
		start := time.Now()
		if err := s.Send(buf); err != nil {
			return 0, err
		}
		networkTime += time.Since(start)
		sentBytes += len(buf)
	}

	return sum, nil
}

func receive(s *client.Client, sendFinishedCh chan bool) (sum int64, err error) {
	buf := make([]byte, maxBufferSize)

	var parseTime time.Duration
	receiveStart := time.Now()
	defer func() {
		log.Printf(recvFmt, time.Since(receiveStart)-parseTime, parseTime)
	}()

	trimNL := func(r rune) bool { return r == '\n' }

	sendFinished := false

	for {
		select {
		case <-sendFinishedCh:
			log.Printf("Receive: got information that send finished")
			sendFinished = true
		default:
		}

		res, err := s.Recv(buf)
		if errors.Is(err, io.EOF) {
			if sendFinished {
				return sum, nil
			}

			time.Sleep(time.Millisecond * 10)
			continue
		} else if err != nil {
			return 0, err
		}

		start := time.Now()

		ints := strings.Split(strings.TrimRightFunc(string(res), trimNL), "\n")
		for _, str := range ints {
			i, err := strconv.Atoi(str)
			if err != nil {
				return 0, err
			}

			sum += int64(i)
		}

		parseTime += time.Since(start)
	}
}
