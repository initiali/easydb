package easydb

import (
	"os"
	"strconv"
	"testing"
)

var opt = &Option{
	Directory:       "./data",
	DataFileMaxSize: 102400,
	Enable:          true,
	Secret:          "1111111111111111",
}

func TestPut(t *testing.T) {
	os.RemoveAll("./data")

	Open(opt)
	Put([]byte("12343"), []byte("sssss"))
	Close()
}

func BenchmarkPut(b *testing.B) {
	Open(opt)
	defer func() {
		migrate()
		Close()
	}()
	for i := 0; i < b.N; i++ {
		Put([]byte(strconv.Itoa(i*1000)), []byte("sssss"))
	}
}
