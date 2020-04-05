package gklogfile

import (
	"fmt"
	"log"
	"os"
	"sync"
)

// KvStore is a kv store backed by an append only log file
type KvStore struct {
	file    *os.File
	fileMap map[string]int64
	// Replace the below with a goroutine representing a writer
	mutex sync.Mutex
}

// Open - open the specified file
func Open(fileName string) (*KvStore, error) {

	fmt.Printf("Opening: %s\n", fileName)

	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	fileMap, err := initialiseFileMap(file)
	if err != nil {
		return nil, err
	}

	kvFile := &KvStore{
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
// Going to make the file & map write a goroutine so we can queue up our writes
// Simple example: https://play.golang.org/p/iOiH3ME7eVg
// Should return number of bytes written (possibly?) and error
func (store *KvStore) Write(value []byte, key string) {

	if len(value) > 2147483647 {
		log.Fatal("Value too long")
	}

	if len(key) > 255 {
		log.Fatal("Key too long")
	}

	// Create a v1 metadata
	md := newMetadata(1)
	writeKeyMetadata(&md, len(key))
	writeValueMetadata(&md, len(value))

	// ** Critical section
	// todo - this will be replaced with a gorouting and a channel to form a writer queue
	store.mutex.Lock()
	fmt.Println("Entering critical section. Writing:", key)

	// we shouldn't be getting the file size on every write but for the moment find out the length of the file
	// so we can create our kv map
	fileStat, err := store.file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	// Write the bytes written and 'value' to the data file.
	// append is a variadic function
	// the elipses (...) effectively take every value independently from the valueToWrite slice (is my understanding)
	// This bit needs rewriting. Would it be more efficient to write to a fixed size array
	if _, err = store.file.Write(append(append(md, []byte(key)...), value...)); err != nil {
		log.Fatal(err)
	}

	store.fileMap[key] = fileStat.Size()

	store.mutex.Unlock()
	fmt.Println("Exiting critical section")
}

// --- Internal functions
func initialiseFileMap(file *os.File) (map[string]int64, error) {

	fileStat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	fileMap := make(map[string]int64)

	for offset := int64(0); offset < fileStat.Size(); {
		meta, err := readMetadata(file, offset)
		if err != nil {
			return nil, err
		}

		key := make([]byte, meta.keyLength())
		if _, err := file.ReadAt(key, offset+int64(len(meta))); err != nil {
			log.Fatal(err)
		}

		fmt.Printf("\tKey: %s read at: %d\n", string(key), offset)

		fileMap[string(key)] = offset

		offset += int64(len(meta) + meta.keyLength() + meta.valueLength())
	}

	return make(map[string]int64), nil

}

/*
Metadata functions
All of the below assume version 1
*/
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

	// The below only works for vsn 1. When/if we change the version we need to read the first byte
	// and then read the metadata based on the version that was read in the first byte
	md := make([]byte, 6)
	if _, err := file.ReadAt(md[0:6], offset); err != nil {
		log.Fatal(err)
	}

	return md, nil
}

func (md metadata) keyLength() int {
	return int(md[1])
}

func (md metadata) valueLength() int {
	// https://play.golang.org/p/xXzANmB6PJU bitwise operators
	return int(md[2]) +
		int(md[3])<<8 +
		int(md[4])<<16 +
		int(md[5])<<24
}

func writeKeyMetadata(md *metadata, length int) {
	(*md)[1] = byte(length)
}

func writeValueMetadata(md *metadata, length int) {
	// https://play.golang.org/p/xXzANmB6PJU bitwise operators
	(*md)[2] = byte(length)
	(*md)[3] = byte(length >> 8)
	(*md)[4] = byte(length >> 16)
	(*md)[5] = byte(length >> 24)
}
