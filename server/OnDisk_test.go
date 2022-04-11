package server

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLastChunkIdx(t *testing.T) {
	dir := getTempDir(t)

	testCreateFile(t, filepath.Join(dir, "moscow-chunk1"))
	srv := testNewOnDisk(t, dir)

	want := uint64(2)
	got := srv.lastChunkIdx

	if got != want {
		t.Fatalf("last chunk index = %d, want %d", got, want)
	}
}
func TestGetFileDescriptor(t *testing.T) {
	dir := getTempDir(t)

	testCreateFile(t, filepath.Join(dir, "moscow-chunk1"))
	srv := testNewOnDisk(t, dir)
	testCases := []struct {
		desc     string
		filename string
		write    bool
		wantErr  bool
	}{
		{
			desc:     "Read from already existing file should not fail",
			filename: "moscow-chunk1",
			write:    false,
			wantErr:  false,
		},
		{
			desc:     "Should not orverwrite existing files",
			filename: "moscow-chunk1",
			write:    true,
			wantErr:  true,
		},
		{
			desc:     "Should not be to read from file that don't exist",
			filename: "moscow-chunk2",
			write:    false,
			wantErr:  true,
		},
		{
			desc:     "Should be able to craete files that don't exist",
			filename: "moscow-chunk2",
			write:    true,
			wantErr:  false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := srv.getFileDecriptor(tc.filename, tc.write)
			srv.forgetFileDescriptor(tc.filename)
			if tc.wantErr && err == nil {
				t.Fatalf("wanted error, got not error")
			} else if !tc.wantErr && err != nil {
				t.Fatalf("want no error, got err %v", err)

			}
		})
	}

}

func TestReadWrite(t *testing.T) {
	srv := testNewOnDisk(t, getTempDir(t))

	want := "one\ntwo\nthree\nfour\n"
	if err := srv.Send(context.Background(), []byte(want)); err != nil {
		t.Fatalf("Write failed %v", err)
	}

	chunks, err := srv.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks failed %v", err)
	}
	if got, want := len(chunks), 1; want != got {
		t.Fatalf("len(Listchunks) = %d, want %d", got, want)
	}
	chunk := chunks[0].Name
	var b bytes.Buffer
	if err := srv.Recv(chunk, 0, uint(len(want)), &b); err != nil {
		t.Fatalf("Read(%q)=%v, want no errors", chunk, err)
	}

	if b.String() != want {
		t.Fatalf("Read(%q) = %q, want %q", chunk, b.String(), want)
	}

	// test the cut last message function
	want = "one\ntwo\nthree\n"
	b.Reset()
	if err := srv.Recv(chunk, 0, uint(len(want)+1), &b); err != nil {
		t.Fatalf("Read(%q)=%v, want no errors", chunk, err)
	}

	if b.String() != want {
		t.Fatalf("Read(%q) = %q, want %q", chunk, b.String(), want)
	}

}
func TestAckOfTheLastChunk(t *testing.T) {
	srv := testNewOnDisk(t, getTempDir(t))

	want := "one\ntwo\nthree\nfour\n"
	if err := srv.Send(context.Background(), []byte(want)); err != nil {
		t.Fatalf("Write failed %v", err)
	}

	chunks, err := srv.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks failed %v", err)
	}
	if got, want := len(chunks), 1; want != got {
		t.Fatalf("len(Listchunks) = %d, want %d", got, want)
	}
	chunk := chunks[0].Name
	if err := srv.Ack(chunk, chunks[0].Size); err == nil {
		t.Fatalf("Ack(last chunk): got no error, expected an error")
	}
}
func TestAckOfTheCompleteChunk(t *testing.T) {
	dir := getTempDir(t)
	srv := testNewOnDisk(t, dir)
	testCreateFile(t, filepath.Join(dir, "moscow-chunk1"))

	if err := srv.Ack("moscow-chunk1", 10000); err != nil {
		t.Errorf("Ack(chunk1) = %v, expected no errors", err)
	}
}
func getTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp(os.TempDir(), "lastchunkidx")
	if err != nil {
		t.Fatalf("MkdirTemp() failed: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

type nilHooks struct{}

func (s *nilHooks) BeforeCreatingChunk(ctx context.Context, category, filename string) error {
	return nil
}

func testNewOnDisk(t *testing.T, dir string) *OnDisk {
	t.Helper()

	srv, err := NewOnDisk(dir, "numbers", "moscow", &nilHooks{})
	if err != nil {
		t.Fatalf("NewOnDisk failed: %v", err)
	}
	return srv
}
func testCreateFile(t *testing.T, path string) {
	t.Helper()

	_, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create() failed: %v", err)
	}
}
func TestCutLast(t *testing.T) {
	buf := []byte("100\n200\n300\n10")
	wantRes, wantRest := []byte("100\n200\n300\n"), []byte("10")
	gotRes, gotRest, err := cutLast(buf)
	if err != nil {
		t.Errorf("cutLast(%v): got error %v; want no error", buf, err)
	}

	if !bytes.Equal(wantRes, gotRes) || !bytes.Equal(wantRest, gotRest) {
		t.Errorf("cutLast(%q): got %q, %q; want %q, %q",
			string(buf), string(gotRes), string(gotRest), string(wantRes), string(wantRest))
	}

}
func TestCutLastErrors(t *testing.T) {
	buf := []byte("100")
	_, _, err := cutLast(buf)
	if err == nil {
		t.Errorf("TestCutLastErrors(%q): want error; but no error", string(buf))
	}
}
