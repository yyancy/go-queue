package main

import (
	"bytes"
	"errors"
	"fmt"
	"go/build"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/yyancy/go-queue/client"
)

const maxN = 10000000
const maxBufferSize = 1024 * 1024

func main() {
	if err := runTest(); err != nil {
		log.Fatalf("Test failed: %v", err)
	}
	log.Printf("Test passed")
}

func runTest() error {

	log.SetFlags(log.Llongfile | log.LstdFlags)
	goPath := os.Getenv("GOPATH")
	if goPath == "" {
		goPath = build.Default.GOPATH
	}
	log.Printf("compiling go-queue")
	out, err := exec.Command("go", "install", "-v", "github.com/yyancy/go-queue").CombinedOutput()
	log.Printf("%s", string(out))
	if err != nil {
		log.Printf("Failed to build: %v", err)
		return fmt.Errorf("compilation failed: %v (out: %s)", err, string(out))
	}
	port := 8080
	dbPath := "/tmp/yancy.db"
	os.RemoveAll(dbPath)
	os.Mkdir(dbPath, 0777)

	ioutil.WriteFile(filepath.Join(dbPath, "chunk1"), []byte("12345\n"), 0666)

	log.Printf("Running go-queue on port %d, GOPATH=%s", port, goPath)
	cmd := exec.Command(goPath+"/bin/go-queue", "-dirname="+dbPath, fmt.Sprintf("-port=%d", port))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		if err != nil {
			return err
		}

	}
	defer cmd.Process.Kill()
	log.Printf("Waiting for the port 127.0.0.1:%d to open", port)
	for i := 0; i <= 100; i++ {
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
	cmd.Process.Kill()
	if err != nil {
		return err
	}
	want += 12345
	if want != got {
		log.Fatalf("The expected sum %d is not equal to the actual sum %d (delivered %1.f%%)", want, got, (float64(got)/float64(want))*100)
	}
	return nil
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
