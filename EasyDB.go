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
	Root               string = ""
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
	Secret             []byte = []byte("ME::QQ:644728872")
	itemPadding        uint32 = 20
	index              map[uint64]*record
	totalDataSize      int64 = 2 << 8 << 20 << 1
)

var (
	openDataFile = func(flag int, dataFileIdentifier int64) (*os.File, error) {
		return os.OpenFile(dataSuffixFunc(dataFileIdentifier), flag, Perm)
	}
	dataSuffixFunc = func(dataFileIdentifier int64) string {
		return fmt.Sprintf("%s%d%s", dataDirectory, dataFileIdentifier, dataFileSuffix)
	}
	openIndexFile = func(flag int, dataFileIdentifier int64) (*os.File, error) {
		return os.OpenFile(indexSuffixFunc(dataFileIdentifier), flag, Perm)
	}
	indexSuffixFunc = func(dataFileIdentifier int64) string {
		return fmt.Sprintf("%s%d%s", indexDirectory, dataFileIdentifier, indexFileSuffix)
	}
)

type Action struct {
	TTL time.Time
}

// 初始化参数
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

// @description 打开数据查询接口
func Open(opt *Option) error {
	opt.Validation()
	initialize()

	if ok, err := pathExists(Root); ok {
		// 存在数据目录时 恢复之前的数据
		return recoverData()
	} else if err != nil {
		// 如果数据文件夹不存在 则自动创建对应路径
		panic("The current path is invalid!!!")
	}
	if err := os.MkdirAll(dataDirectory, Perm); err != nil {
		// 创建数据文件夹
		panic("Failed to create a working directory!!!")
	}

	if err := os.MkdirAll(indexDirectory, Perm); err != nil {
		// 创建索引文件夹
		panic("Failed to create a working directory!!!")
	}

	return createActiveFile()
}

// 创建active文件
func createActiveFile() error {
	mutex.Lock()
	defer mutex.Unlock()

	writeOffset = 0
	dataFileVersion++

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

// @description   恢复数据和索引
func recoverData() error {
	if dataTotalSize() >= totalDataSize {
		return errors.New("data size to bigger")
	}
	// 找到最新的数据文件，设置游标【writeOffset】
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

// @description 读取最新的数据文件版本
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
	if len(ids) < 1 {
		panic("没有读取到数据文件的版本")
	}
	dataFileVersion = int64(ids[len(ids)-1])
}

// @description 重建索引
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

// @description 读取索引对象
func readIndexItem() error {
	file, err := findLatesIndexFile()
	defer func() {
		// if err := file.Sync(); err != nil {
		// 	return
		// }
		if file.Close() != nil {
			return
		}
	}()
	if err == nil {
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

// @description 计算数据文件的总大小 多个文件的大小累加
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

func Get(key []byte) *Data {
	data := &Data{}
	mutex.RLock()
	defer mutex.RUnlock()

	sum64 := HashedFunc.Sum64(key)

	rec, ok := index[sum64]
	if !ok {
		data.Err = errors.New("the current key does not find")
		return data
	}

	if rec.ExpireTime <= uint32(time.Now().Unix()) {
		data.Err = errors.New("the current key has expired")
		return data
	}

	item, err := encoder.Read(rec)

	if err != nil {
		data.Err = err
		return data
	}
	data.Item = item
	return data
}

func Del(key []byte) error {
	mutex.RLock()
	defer mutex.RUnlock()

	return nil
}

func Close() error {
	mutex.Lock()
	defer mutex.Unlock()

	if active.Sync() != nil {
		return errors.New("current close active file error")
	}

	for _, file := range fileList {
		if err := file.Close(); err != nil {
			return errors.New("close data file error :" + file.Name())
		}
	}
	return saveIndexToFile()
}

func saveIndexToFile() error {
	var file *os.File
	defer func() {
		if err := file.Sync(); err != nil {
			return
		}
		if err := file.Close(); err != nil {
			return
		}
	}()
	var channel = make(chan indexItem, 1024)
	go func() {
		for sum64, record := range index {
			channel <- indexItem{
				idx:    sum64,
				record: record,
			}
		}
		close(channel)
	}()

	oldIndexFiles, err := ioutil.ReadDir(indexDirectory)
	if err != nil {
		return err
	}
	indexId := time.Now().Unix()
	file, err = openIndexFile(FRW, indexId)
	if err != nil {
		return err
	}
	for {
		v, ok := <-channel
		if !ok {
			break
		}
		if _, err = encoder.WriteIndex(v, file); err != nil {
			return err
		}
	}

	func(oldIndexFiles []fs.FileInfo) {
		for _, oif := range oldIndexFiles {
			fileName := fmt.Sprintf("%s%s", indexDirectory, oif.Name())
			os.Remove(fileName)
		}
	}(oldIndexFiles)

	return err
}

func Deleted(key []byte) {
	mutex.Lock()
	defer mutex.Unlock()
	delete(index, HashedFunc.Sum64(key))
}

func migrate() error {
	if err := buildIndex(); err != nil {
		return err
	}

	version()

	var (
		offset       uint32
		file         *os.File
		fileInfo     os.FileInfo
		excludeFiles []uint64
		activeItem   = make(map[uint64]*Item, len(index))
	)
	dataFileVersion++

	file, _ = openDataFile(FRW, dataFileVersion)
	excludeFiles = append(excludeFiles, uint64(dataFileVersion))
	fileInfo, _ = file.Stat()

	for idx, rec := range index {
		item, err := encoder.Read(rec)

		if err != nil {
			return err
		}

		activeItem[idx] = item
	}

	for idx, item := range activeItem {
		if fileInfo.Size() >= defaultMaxFileSize {
			if err := file.Sync(); err != nil {
				return err
			}
			if err := file.Close(); err != nil {
				return err
			}

			dataFileVersion++
			excludeFiles = append(excludeFiles, uint64(dataFileVersion))

			file, _ = openDataFile(FRW, dataFileVersion)
			fileInfo, _ = file.Stat()
			offset = 0
		}

		size, err := encoder.Write(item, file)

		if err != nil {
			return err
		}

		index[idx].FID = dataFileVersion
		index[idx].Size = uint32(size)
		index[idx].Offset = offset

		offset += uint32(size)
	}

	fileInfos, err := ioutil.ReadDir(dataDirectory)

	if err != nil {
		return err
	}

	for _, info := range fileInfos {
		fileName := fmt.Sprintf("%s%s", dataDirectory, info.Name())
		for _, excludeFile := range excludeFiles {
			if fileName != dataSuffixFunc(int64(excludeFile)) {
				if err := os.Remove(fileName); err != nil {
					return err
				}
			}
		}
	}
	return saveIndexToFile()
}
