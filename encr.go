package easydb

type SourceData struct {
	Data   []byte
	Secret []byte
}

type Encryptor interface {
	Encode(sd *SourceData) error
	Decode(sd *SourceData) error
}
