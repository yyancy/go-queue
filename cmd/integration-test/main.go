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
	dbPath := "E:/yancy/"
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
	log.Printf("Waiting for the port localhost:%d to open", port)
	for i := 0; i <= 100; i++ {
		timeout := time.Millisecond * 50
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", fmt.Sprint(port)), timeout)
		if err != nil {
			time.Sleep(timeout)
			continue
		}
		conn.Close()
		break
	}
	log.Printf("Starting the test")

	c, _ := client.NewClient([]string{"http://localhost:" + fmt.Sprint(port)})
	want, err := send(c)
	if err != nil {
		log.Fatalf("Send error: %v", err)
	}
	got, err := recv(c)
	if err != nil {
		log.Fatalf("Recv error: %v", err)
	}
	cmd.Process.Kill()

	want += 12345
	if want != got {
		log.Fatalf("The expected sum %d is not equal to the actual sum %d (delivered %1.f%%)", want, got, (float64(got)/float64(want))*100)
	}
	return nil
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
func recv(c *client.Client) (sum int64, err error) {
	buf := make([]byte, maxBufferSize)
	sum = 0
	for {
		res, err := c.Recv(buf)
		if errors.Is(err, io.EOF) {
			return sum, nil
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
