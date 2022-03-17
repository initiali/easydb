package easydb

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"time"
)

type Encoder struct {
	Encryptor      // encryptor concrete implementation
	enable    bool // whether to enable data encryption and decryption
}

func DefaultEncoder() *Encoder {
	return &Encoder{
		enable:    false,
		Encryptor: nil,
	}
}
func (e *Encoder) Write(item *Item, file *os.File) (int, error) {
	// whether encryption is enabled
	if e.enable && e.Encryptor != nil {
		// building source data
		sd := &SourceData{
			Secret: Secret,
			Data:   item.Value,
		}
		if err := e.Encode(sd); err != nil {
			return 0, errors.New("an error occurred in the encryption encoder")
		}
		item.Value = sd.Data
		return bufToFile(binaryEncode(item), file)
	}

	return bufToFile(binaryEncode(item), file)
}

func bufToFile(data []byte, file *os.File) (int, error) {
	if n, err := file.Write(data); err == nil {
		return n, nil
	}
	return 0, errors.New("error writing encode buffer data")
}

func binaryEncode(item *Item) []byte {
	item.KeySize = uint32(len(item.Key))
	item.ValueSize = uint32(len(item.Value))

	buf := make([]byte, item.KeySize+item.ValueSize+itemPadding)
	binary.LittleEndian.PutUint64(buf[4:12], item.TimeStamp)
	binary.LittleEndian.PutUint32(buf[4:12], item.KeySize)
	binary.LittleEndian.PutUint32(buf[4:12], item.ValueSize)

	copy(buf[itemPadding:itemPadding+item.KeySize], item.Key)
	copy(buf[itemPadding+item.KeySize:itemPadding+item.KeySize+item.ValueSize], item.Value)

	binary.LittleEndian.PutUint32(buf[:4], crc32.ChecksumIEEE(buf[:4]))
	return buf
}

func (Encoder) ReadIndex(buf []byte) error {
	var (
		item indexItem
	)
	if binary.LittleEndian.Uint32(buf[:4]) != crc32.ChecksumIEEE(buf[:4]) {
		return errors.New("index recoed verification failed")
	}

	item.record = new(record)
	item.idx = binary.LittleEndian.Uint64(buf[4:12])
	item.FID = int64(binary.LittleEndian.Uint64(buf[12:20]))
	item.Timestamp = binary.LittleEndian.Uint32(buf[20:24])
	item.ExpireTime = binary.LittleEndian.Uint32(buf[24:28])
	item.Size = binary.LittleEndian.Uint32(buf[28:32])
	item.Offset = binary.LittleEndian.Uint32(buf[32:36])

	if uint32(time.Now().Unix()) <= item.ExpireTime {
		index[item.idx] = &record{
			FID:        item.FID,
			Size:       item.Size,
			Offset:     item.Offset,
			Timestamp:  item.Timestamp,
			ExpireTime: item.ExpireTime,
		}
	}
	return nil
}
