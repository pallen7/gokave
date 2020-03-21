package gklogfile

import (
	"fmt"
	"log"
	"os"
	"sync"
)

// KvFile - an append only KvFile
type KvFile struct {
	file    *os.File
	fileMap map[string]int64
	mutex   sync.Mutex
}

// Open - open the specified file
func Open(fileName string) (*KvFile, error) {
	fmt.Printf("Opening: %s\n", fileName)

	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	fileMap, err := initialiseFileMap(file)
	if err != nil {
		return nil, err
	}

	kvFile := &KvFile{
		file:    file,
		fileMap: fileMap,
	}
	return kvFile, nil
}

// Close - tbc
func Close() {

}

// Delete - delete a value from the store
func Delete() {

}

// Read - the value for a given key
func Read() {

}

// Write - writes a Key Value pair to the file
func Write() {

}

// --- Internal functions

func initialiseFileMap(file *os.File) (map[string]int64, error) {

	fileStat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	for offset := int64(0); offset < fileStat.Size(); {
		md, err := readMetadata(file, offset)
		if err != nil {
			return nil, err
		}

		key := make([]byte, md.keyLength())
		if _, err := file.ReadAt(key, offset+int64(len(md))); err != nil {
			log.Fatal(err)
		}

	}

	return make(map[string]int64), nil

}

type metadata []byte

func newMetadata(version int) metadata {
	// Assuming version 1
	return make([]byte, 6)
}

func readMetadata(file *os.File, offset int64) (metadata, error) {
	// note: Code below assumes version 1 - version added as a hook for future changes
	// byte 0		version
	// byte 1		keyLength
	// byte 2-4		valueLength

	md := make([]byte, 1)
	_, err := file.ReadAt(md[0:1], offset)
	if err != nil {
		return nil, err
	}

	if _, err := file.ReadAt(md[1:6], offset+1); err != nil {
		log.Fatal(err)
	}

	return md, nil
}

// All below are assumed to be version 1
func (md metadata) keyLength() int {
	return int(md[1])
}

func (md metadata) valueLength() int {
	return int(md[2]) +
		int(md[3])<<8 +
		int(md[4])<<16 +
		int(md[5])<<24
}

func keyLength(md *metadata, len int) {
	(*md)[0] = byte(len)
}

// func createMe
