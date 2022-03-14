package client

import (
	"bytes"
	"testing"
)

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
