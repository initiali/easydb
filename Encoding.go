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

func AES() *Encoder {
	return &Encoder{
		enable:    true,
		Encryptor: new(AESEncryptor),
	}
}

func DefaultEncoder() *Encoder {
	return &Encoder{
		enable:    false,
		Encryptor: nil,
	}
}

func (e *Encoder) Write(item *Item, file *os.File) (int, error) {
	if e.enable && e.Encryptor != nil {
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
func (Encoder) WriteIndex(item indexItem, file *os.File) (int, error) {
	buf := make([]byte, 36)

	binary.LittleEndian.PutUint64(buf[4:12], item.idx)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(item.FID))
	binary.LittleEndian.PutUint32(buf[20:24], item.Timestamp)
	binary.LittleEndian.PutUint32(buf[24:28], item.ExpireTime)
	binary.LittleEndian.PutUint32(buf[28:32], item.Size)
	binary.LittleEndian.PutUint32(buf[32:36], item.Offset)

	binary.LittleEndian.PutUint32(buf[:4], crc32.ChecksumIEEE(buf[4:]))

	return file.Write(buf)
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
	binary.LittleEndian.PutUint32(buf[12:16], item.KeySize)
	binary.LittleEndian.PutUint32(buf[16:20], item.ValueSize)

	copy(buf[itemPadding:itemPadding+item.KeySize], item.Key)
	copy(buf[itemPadding+item.KeySize:itemPadding+item.KeySize+item.ValueSize], item.Value)

	binary.LittleEndian.PutUint32(buf[:4], crc32.ChecksumIEEE(buf[4:]))
	return buf
}

func (e *Encoder) ReadIndex(buf []byte) error {
	var (
		item indexItem
	)
	if binary.LittleEndian.Uint32(buf[:4]) != crc32.ChecksumIEEE(buf[4:]) {
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

func (e *Encoder) Read(rec *record) (*Item, error) {
	item, err := parseLog(rec)
	if err != nil {
		return nil, err
	}

	if e.enable && e.Encryptor != nil && item != nil {
		sd := &SourceData{
			Secret: Secret,
			Data:   item.Value,
		}
		if err := e.Decode(sd); err != nil {
			return nil, errors.New("data decryption error")
		}
		item.Value = sd.Data
	}
	return item, nil
}

func parseLog(rec *record) (*Item, error) {
	if file, ok := fileList[rec.FID]; ok {
		data := make([]byte, rec.Size)
		_, err := file.ReadAt(data, int64(rec.Offset))
		if err != nil {
			return nil, err
		}
		return binaryDecode(data), nil
	}
	return nil, errors.New("no readable data file found")
}

func binaryDecode(data []byte) *Item {
	if binary.LittleEndian.Uint32(data[:4]) != crc32.ChecksumIEEE(data[4:]) {
		return nil
	}
	var item Item

	item.TimeStamp = binary.LittleEndian.Uint64(data[4:12])
	item.KeySize = binary.LittleEndian.Uint32(data[12:16])
	item.ValueSize = binary.LittleEndian.Uint32(data[16:20])
	item.CRC32 = binary.LittleEndian.Uint32(data[:4])

	item.Key, item.Value = make([]byte, item.KeySize), make([]byte, item.ValueSize)
	copy(item.Key, data[itemPadding:itemPadding+item.KeySize])
	copy(item.Value, data[itemPadding+item.KeySize:itemPadding+item.KeySize+item.ValueSize])
	return &item
}
