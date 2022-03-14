package main

import (
	"bytes"
	"fmt"
	"log"
)

func main() {
	fmt.Println("vim-go")
	buf := bytes.NewBuffer([]byte("abcde"))
	buf.ReadByte()
	buf.ReadByte()
	log.Printf("len(buf): %d", buf.Len())
	buf.UnreadByte()
	err := buf.UnreadByte()
	if err != nil {
		log.Fatalf("get error: %v", err)
	}
	buf.UnreadByte()
	buf.UnreadByte()
	buf.UnreadByte()
	buf.UnreadByte()
	log.Printf("len(buf): %d", buf.Len())
}
