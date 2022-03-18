package easydb

import (
	"testing"
)

func TestPut(t *testing.T) {
	// os.RemoveAll("./data")
	opt := &Option{
		Directory:       "./data",
		DataFileMaxSize: 102400,
		Enable:          true,
		Secret:          "1111111111111111",
	}
	Open(opt)
	Put([]byte("12343"), []byte("sssss"))
	Get([]byte("12343"))
	Close()
}
