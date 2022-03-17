package easydb

import (
	"os"
	"testing"
)

func TestPut(t *testing.T) {
	os.RemoveAll("./data")
	opt := &Option{
		Directory:       "./data",
		DataFileMaxSize: 1024,
	}
	Open(opt)
	Put([]byte("1234"), []byte("sssss"))
}
