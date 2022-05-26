package integration

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
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

func TestSimpleClientAndServerConcurently(t *testing.T) {
	t.Parallel()
	SimpleClientAndServerTest(t, true)
}

func TestSimpleClientAndServerSequentially(t *testing.T) {
	t.Parallel()
	SimpleClientAndServerTest(t, false)

}

func SimpleClientAndServerTest(t *testing.T, concurrent bool) {
	t.Helper()

	log.SetFlags(log.Flags() | log.Lmicroseconds)

	etcdPeerPort, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port for etcd peer: %v", err)
	}

	etcdPort, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port for etcd: %v", err)
	}

	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}

	etcdPath, err := os.MkdirTemp(os.TempDir(), "etcd")
	if err != nil {
		t.Fatalf("Failed to create temp dir for etcd: %v", err)
	}

	dbPath, err := os.MkdirTemp(os.TempDir(), "go-queue")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	t.Cleanup(func() { os.RemoveAll(dbPath) })
	t.Cleanup(func() { os.RemoveAll(etcdPath) })

	categoryPath := filepath.Join(dbPath, "numbers")
	os.MkdirAll(categoryPath, 0777)

	// Initialise the database contents with
	// a not easy-to-guess contents that must
	// be preserved when writing to this directory.
	ioutil.WriteFile(filepath.Join(categoryPath, fmt.Sprintf("moscow-chunk%09d", 1)), []byte("12345\n"), 0666)

	etcdArgs := []string{"--data-dir", etcdPath,
		"--listen-client-urls", fmt.Sprintf("http://localhost:%d", etcdPort),
		"--advertise-client-urls", fmt.Sprintf("http://localhost:%d", etcdPort),
		"--listen-peer-urls", fmt.Sprintf("http://localhost:%d", etcdPeerPort)}

	log.Printf("Running `etcd %s`", strings.Join(etcdArgs, " "))

	cmd := exec.Command("etcd", etcdArgs...)
	cmd.Env = append(os.Environ(), "ETCD_UNSUPPORTED_ARCH=arm64")
	if err := cmd.Start(); err != nil {
		t.Fatalf("Could not run etcd: %v", err)
	}

	t.Cleanup(func() { cmd.Process.Kill() })

	log.Printf("Waiting for the etcd port localhost:%d to open", etcdPort)

	waitForPort(t, etcdPort, make(chan error, 1))

	log.Printf("Running chukcha on port %d", port)

	errCh := make(chan error, 1)
	go func() {
		errCh <- InitAndServe(InitArgs{
			EtcdAddr:     []string{fmt.Sprintf("localhost:%d", etcdPort)},
			InstanceName: "moscow",
			ClusterName:  "test",
			DirName:      dbPath,
			ListenAddr:   fmt.Sprintf("localhost:%d", port),
		})
	}()

	log.Printf("Waiting for the Chukcha port localhost:%d to open", port)
	waitForPort(t, port, make(chan error, 1))

	log.Printf("Starting the test")

	s, _ := client.NewClient([]string{fmt.Sprintf("http://localhost:%d", port)})

	var want, got int64

	if concurrent {
		want, got, err = sendAndRecvConcurrently(s)
		if err != nil {
			t.Fatalf("sendAndReceiveConcurrently: %v", err)
		}
	} else {
		want, err = send(s)
		if err != nil {
			t.Fatalf("send error: %v", err)
		}

		sendFinishedCh := make(chan bool, 1)
		sendFinishedCh <- true
		got, err = recv(s, sendFinishedCh)
		if err != nil {
			t.Fatalf("receive error: %v", err)
		}
	}

	// The contents of the chunk that already existed.
	want += 12345

	if want != got {
		t.Errorf("the expected sum %d is not equal to the actual sum %d (delivered %1.f%%)", want, got, (float64(got)/float64(want))*100)
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
			if err := c.Send("numbers", buf); err != nil {
				return 0, err
			}
			networkTime += time.Since(start)
			sentBytes += len(buf)

			buf = buf[0:0]
		}
	}

	if len(buf) != 0 {
		start := time.Now()
		if err := c.Send("numbers", buf); err != nil {
			return 0, err
		}
		networkTime += time.Since(start)
		sentBytes += len(buf)
	}

	return sum, nil
}

var errTmpRandom = errors.New("random error tests")

func recv(c *client.Client, completeCh chan bool) (sum int64, err error) {
	buf := make([]byte, maxBufferSize)
	sum = 0
	sendFinished := false
	longCtn := 0
	for {
		longCtn++
		select {
		case <-completeCh:
			// log.Printf("Receive: got information that send finished")
			sendFinished = true
		default:
		}
		err := c.Process("numbers", buf, func(res []byte) error {
			if longCtn%10 == 0 {
				return errTmpRandom
			}
			ints := strings.Split(string(res), "\n")
			for _, i := range ints {
				if i == "" {
					continue
				}
				i, err := strconv.Atoi(i)
				if err != nil {
					return err
				}
				// if idx < 2 {
				// 	log.Printf("recived number=%d", i)
				// }
				sum += int64(i)
			}

			return nil
		})
		if errors.Is(err, errTmpRandom) {
			continue
		} else if errors.Is(err, io.EOF) {
			if sendFinished {
				return sum, nil
			}
			// time.Sleep(100 * time.Millisecond)
			continue
		} else if err != nil {
			return 0, err
		}
	}

}

func waitForPort(t *testing.T, port int, errCh chan error) {
	t.Helper()

	for i := 0; i <= 100; i++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("InitAndServe failed: %v", err)
			}
		default:
		}

		timeout := time.Millisecond * 50
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", fmt.Sprint(port)), timeout)
		if err != nil {
			time.Sleep(timeout)
			continue
		}
		conn.Close()
		break
	}
}
