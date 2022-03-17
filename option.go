package easydb

import (
	"fmt"
	"os"
	"strings"
)

type Option struct {
	Directory       string
	DataFileMaxSize int64
	Enable          bool
	Secret          string
}

var (
	DefaultOption = Option{
		Directory:       "./easydb",
		DataFileMaxSize: 1024,
	}
)

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (o *Option) Validation() {
	if o.Directory == "" {
		panic("the data file directory cannot be empty")
	}

	o.Directory = pathBackslashes(o.Directory)
	o.Directory = strings.TrimSpace(o.Directory)

	Root = o.Directory

	if o.DataFileMaxSize != 0 {
		defaultMaxFileSize = o.DataFileMaxSize
	}

	dataDirectory = fmt.Sprintf("%sdata/", Root)

	indexDirectory = fmt.Sprintf("%sindex/", Root)

	if o.DataFileMaxSize != 0 {
		defaultMaxFileSize = o.DataFileMaxSize
	}
}

func pathBackslashes(path string) string {
	if !strings.HasSuffix(path, "/") {
		return fmt.Sprintf("%s/", path)
	}
	return path
}
