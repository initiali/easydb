package easydb

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
)

type AESEncryptor struct{}

func (AESEncryptor) Encode(sd *SourceData) error {
	sd.Data = aesEncrypt(sd.Data, sd.Secret)
	return nil
}

func (AESEncryptor) Decode(sd *SourceData) error {
	sd.Data = aesDecrypt(sd.Data, sd.Secret)
	return nil
}

func aesEncrypt(origData, key []byte) []byte {
	block, _ := aes.NewCipher(key)
	blockSize := block.BlockSize()
	origData = PKCS7Padding(origData, blockSize)
	blockMode := cipher.NewCBCEncrypter(block, key[:blockSize])
	result := make([]byte, len(origData))
	blockMode.CryptBlocks(result, origData)
	return result
}

func aesDecrypt(bytes, key []byte) []byte {
	block, _ := aes.NewCipher(key)
	blockSize := block.BlockSize()
	blockMode := cipher.NewCBCDecrypter(block, key[:blockSize])
	orig := make([]byte, len(bytes))
	blockMode.CryptBlocks(orig, bytes)
	orig = PKCS7UnPadding(orig)
	return orig
}

func PKCS7Padding(ciphertext []byte, blksize int) []byte {
	padding := blksize - len(ciphertext)%blksize
	plaintext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, plaintext...)
}

func PKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	padding := int(origData[length-1])
	return origData[:(length - padding)]
}
