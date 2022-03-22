package easydb

import (
	"strconv"
	"testing"
)

func TestPutAndGet(t *testing.T) {
	// os.RemoveAll("./data")
	opt := &Option{
		Directory:       "./data",
		DataFileMaxSize: 10240,
		Enable:          true,
		Secret:          "1111111111111111",
	}
	Open(opt)
	for i := 0; i < 10000; i++ {
		// Put([]byte(strconv.Itoa(i)), []byte("sssss"))
		Get([]byte(strconv.Itoa(i)))
	}

	// migrate()
	Close()
}
