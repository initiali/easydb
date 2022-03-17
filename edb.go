package easydb

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type record struct {
	FID        int64  // data file id
	Size       uint32 // data record size
	Offset     uint32 // data record offset
	Timestamp  uint32 // data record create timestamp
	ExpireTime uint32 // data record expire time
}

type indexItem struct {
	idx uint64
	*record
}

var (
	mutex              sync.RWMutex
	dataFileVersion    int64  = 0
	Root               string = "./data"
	active             *os.File
	FRW                = os.O_RDWR | os.O_APPEND | os.O_CREATE
	FR                 = os.O_RDONLY
	Perm               = os.FileMode(0750)
	dataFileSuffix     = ".data"
	indexFileSuffix    = ".idx"
	dataDirectory      string
	indexDirectory     string
	writeOffset        uint32 = 0
	fileList           map[int64]*os.File
	defaultMaxFileSize int64 = 2 << 8 << 20
	HashedFunc         Hashed
	encoder            *Encoder
	Secret                    = []byte("ME:QQ:644728872")
	itemPadding        uint32 = 20
	index              map[uint64]*record
	totalDataSize      int64 = 2 << 8 << 20 << 1 // 1GB
)

var (
	openDataFile = func(flag int, dataFileIdentifier int64) (*os.File, error) {
		return os.OpenFile(dataSuffixFunc(dataFileIdentifier), flag, Perm)
	}
	dataSuffixFunc = func(dataFileIdentifier int64) string {
		return fmt.Sprintf("%s%d%s", dataDirectory, dataFileIdentifier, dataFileSuffix)
	}
	openIndexFile = func(flag int, indexFileIdentifier int64) (*os.File, error) {
		return os.OpenFile(dataSuffixFunc(indexFileIdentifier), flag, Perm)
	}
	indexSuffixFunc = func(indexFileIdentifier int64) string {
		return fmt.Sprintf("%s%d%s", indexDirectory, indexFileIdentifier, indexFileSuffix)
	}
)

type Action struct {
	TTL time.Time
}

func initialize() {
	if HashedFunc == nil {
		HashedFunc = DefaultHashFunc()
	}
	if encoder == nil {
		encoder = DefaultEncoder()
	}
	if index == nil {
		index = make(map[uint64]*record)
	}
	fileList = make(map[int64]*os.File, 5)
}

func Open(opt *Option) error {
	opt.Validation()
	initialize()

	if ok, err := pathExists(Root); ok {
		// The directory has recovered data. Procedure
		return recoverData()
	} else if err != nil {
		// If there is an error, the file is not a directory or is invalid
		panic("The current path is invalid!!!")
	}
	// Create folder if it does not exist
	if err := os.MkdirAll(dataDirectory, Perm); err != nil {
		panic("Failed to create a working directory!!!")
	}

	if err := os.MkdirAll(indexDirectory, Perm); err != nil {
		panic("Failed to create a working directory!!!")
	}

	// Once the directory is created, you can create active files to write data
	return createActiveFile()
}

func createActiveFile() error {
	mutex.Lock()
	defer mutex.Unlock()

	dataFileVersion++
	writeOffset = 0

	if file, err := openDataFile(FRW, dataFileVersion); err == nil {
		active = file
		fileList[dataFileVersion] = active
		return nil
	}

	return errors.New("createActiveFile error")
}

func closeActiveFile() error {
	mutex.Lock()
	defer mutex.Unlock()

	if err := active.Sync(); err != nil {
		return err
	}

	if err := active.Close(); err != nil {
		return err
	}

	if file, err := openDataFile(FR, dataFileVersion); err == nil {
		fileList[dataFileVersion] = file
		return nil
	}

	return errors.New("error opening write only file")
}

func Put(key, value []byte, actionFunc ...func(action *Action)) error {
	var (
		action Action
		size   int
	)
	if len(actionFunc) > 0 {
		for _, fn := range actionFunc {
			fn(&action)
		}
	}

	fileInfo, _ := active.Stat()

	if fileInfo.Size() >= defaultMaxFileSize {
		if err := closeActiveFile(); err != nil {
			return err
		}
		if err := createActiveFile(); err != nil {
			return err
		}
	}

	sum64 := HashedFunc.Sum64(key)
	mutex.Lock()
	defer mutex.Unlock()

	timestamp := time.Now().Unix()

	size, err := encoder.Write(NewItem(key, value, uint64(timestamp)), active)
	if err != nil {
		return err
	}

	index[sum64] = &record{
		FID:        dataFileVersion,
		Size:       uint32(size),
		Offset:     writeOffset,
		Timestamp:  uint32(timestamp),
		ExpireTime: uint32(action.TTL.Unix()),
	}
	writeOffset += uint32(size)

	return nil
}

func recoverData() error {
	if dataTotalSize() >= totalDataSize {
		return errors.New("data size to bigger")
	}
	if file, err := findLatestDataFile(); err == nil {
		info, _ := file.Stat()
		if info.Size() >= defaultMaxFileSize {
			if err := createActiveFile(); err != nil {
				return err
			}
			return buildIndex()
		}
		active = file
		if offset, err := file.Seek(0, os.SEEK_END); err == nil {
			writeOffset = uint32(offset)
		}
		return buildIndex()
	}
	return errors.New("failed to restore data")
}

func findLatesIndexFile() (*os.File, error) {
	files, err := ioutil.ReadDir(indexDirectory)
	if err != nil {
		return nil, err
	}

	var indexes []fs.FileInfo

	for _, file := range files {
		if path.Ext(file.Name()) == indexFileSuffix {
			indexes = append(indexes, file)
		}
	}

	var ids []int

	for _, info := range indexes {
		id := strings.Split(info.Name(), ".")[0]
		i, err := strconv.Atoi(id)
		if err != nil {
			return nil, err
		}
		ids = append(ids, i)
	}
	sort.Ints(ids)

	return openIndexFile(FR, int64(ids[len(ids)-1]))
}

func findLatestDataFile() (*os.File, error) {
	version()
	return openDataFile(FRW, dataFileVersion)
}

func version() {
	files, _ := ioutil.ReadDir(dataDirectory)
	var datafiles []fs.FileInfo

	for _, file := range files {
		if path.Ext(file.Name()) == dataFileSuffix {
			datafiles = append(datafiles, file)
		}
	}
	var ids []int
	for _, info := range datafiles {
		id := strings.Split(info.Name(), ".")[0]
		i, _ := strconv.Atoi(id)
		ids = append(ids, i)
	}
	sort.Ints(ids)
	dataFileVersion = int64(ids[len(ids)-1])
}

func buildIndex() error {
	if err := readIndexItem(); err != nil {
		return err
	}

	for _, record := range index {
		if fileList[record.FID] == nil {
			file, err := openDataFile(FR, record.FID)
			if err != nil {
				return err
			}
			fileList[record.FID] = file
		}
	}
	return nil
}

func readIndexItem() error {
	if file, err := findLatesIndexFile(); err == nil {
		defer func() {
			if err := file.Sync(); err != nil {
				return
			}
			if err := file.Close(); err != nil {
				return
			}
		}()

		buf := make([]byte, 36)

		for {
			_, err := file.Read(buf)

			if err != nil && err != io.EOF {
				return err
			}
			if err == io.EOF {
				break
			}
			if err = encoder.ReadIndex(buf); err != nil {
				return err
			}
		}
		return nil
	}

	return errors.New("index reading failed")
}

func dataTotalSize() int64 {
	files, _ := ioutil.ReadDir(dataDirectory)
	var datafiles []fs.FileInfo
	for _, file := range files {
		if path.Ext(file.Name()) == dataFileSuffix {
			datafiles = append(datafiles, file)
		}
	}
	var totalSize int64
	for _, datafile := range datafiles {
		totalSize += datafile.Size()
	}
	return totalSize
}
