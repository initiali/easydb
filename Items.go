package easydb

type Log struct {
	Key, Value []byte
}

type Item struct {
	TimeStamp uint64 // Create timestamp
	CRC32     uint32 // Cyclic check code
	KeySize   uint32 // The size of the key
	ValueSize uint32 // The size of the value
	Log              // Key string, value serialization
}

func NewItem(key, value []byte, timestamp uint64) *Item {
	return &Item{
		TimeStamp: timestamp,
		Log: Log{
			Key:   key,
			Value: value,
		},
	}
}

type Data struct {
	Err error
	*Item
}

func (d *Data) Value() []byte {
	return d.Item.Log.Value
}
