package gklogfile

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"
)

// KvFile is an individual Key Value file allowing append only operations
// It contains a map pointing to the given position in a file for any given keys
// todo: Need to remove all of the debug statements
type KvFile struct {
	file           *os.File
	fileWriteMutex sync.Mutex
	fileMap        map[string]int64
	fileMapMutex   sync.RWMutex
}

const (
	v1 = iota + 1
)

// Open - open the specified file
// Currently also creates the file if it doesn't pre-exist. Possibly pass the creation
// up a level when we get to multiple files per store
func Open(fileName string) (*KvFile, error) {

	fmt.Printf("Opening: %s\n", fileName)

	// We want an append only file but still allow concurrent reads. Pass the respobnsibility for this the OS as per:
	// https://stackoverflow.com/questions/37628873/golang-simultaneous-read-write-to-the-file-without-explicit-file-lock
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
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
// todo:
// - remove debug statement(s)
// - sort error handling
// - handle delete
func (kvFile *KvFile) Read(key string) ([]byte, error) {
	kvFile.fileMapMutex.RLock()
	startLocation, keyFound := kvFile.fileMap[key]
	kvFile.fileMapMutex.RUnlock()

	if !keyFound {
		fmt.Printf("Key not found: %s\n", key)
		return make([]byte, 0), nil // todo: When reviewing errors we should create return a not_found error
	}

	md := newMetadata(v1)
	if _, err := kvFile.file.ReadAt(md, startLocation); err != nil {
		log.Fatal(err)
	}

	valuePosition := startLocation + int64(len(md)) + int64(md.keyLength())

	value := make([]byte, md.valueLength())
	if _, err := kvFile.file.ReadAt(value, valuePosition); err != nil {
		log.Fatal(err)
	}

	// todo: remove debug statement
	fmt.Println("Key", key, "Location", startLocation, "dataLength", md.valueLength(), "keyLength", md.keyLength())
	return value, nil
}

// Write - writes a Key Value pair to the file
// Should return an error/nil
func (kvFile *KvFile) Write(value []byte, key string) {

	// Move these into the writeMetadata once we understand how to create custom errors
	if len(value) > 2147483647 {
		log.Fatal("Value too long")
	}

	if len(key) > 255 {
		log.Fatal("Key too long")
	}

	md := newMetadata(v1)
	writeKeyMetadata(&md, len(key))
	writeValueMetadata(&md, len(value))

	// Write to the buffer
	writer := bufio.NewWriterSize(kvFile.file, len(md)+md.keyLength()+md.valueLength())
	writer.Write(md)
	writer.WriteString(key)
	writer.Write(value)

	location, err := writeToFile(kvFile.file, writer, &kvFile.fileWriteMutex)
	if err != nil {
		log.Fatal()
	}

	// Note that it is dangerous to update a map that could also be being read
	// https://stackoverflow.com/questions/36167200/how-safe-are-golang-maps-for-concurrent-read-write-operations
	kvFile.fileMapMutex.Lock()
	kvFile.fileMap[key] = location
	kvFile.fileMapMutex.Unlock()

	fmt.Printf("%s written to %s\n", key, kvFile.file.Name())
}

//
// *** Internal functions
//
func writeToFile(file *os.File, writer *bufio.Writer, mutex *sync.Mutex) (location int64, err error) {
	mutex.Lock()
	defer mutex.Unlock()

	fileStat, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	if err = writer.Flush(); err != nil {
		log.Fatal(err)
	}

	return fileStat.Size(), nil
}

func initialiseFileMap(file *os.File) (map[string]int64, error) {

	fileStat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	fileMap := make(map[string]int64)
	fileSize := fileStat.Size()

	for position := int64(0); position < fileSize; {
		meta, err := readMetadata(file, position)
		if err != nil {
			return nil, err
		}

		key := make([]byte, meta.keyLength())
		if _, err := file.ReadAt(key, position+int64(len(meta))); err != nil {
			return nil, err
		}

		fileMap[string(key)] = position
		position += int64(len(meta) + meta.keyLength() + meta.valueLength())

		fmt.Printf("\tKey: %s read at: %d\n", string(key), position)
	}

	return fileMap, nil

}

/*
Metadata functions
All of the below assume version 1
	// byte 0		version
	// byte 1		keyLength
	// byte 2-5		valueLength
*/
type metadata []byte

func newMetadata(version int) metadata {
	md := make([]byte, 6) // Assuming version 1 - 6 bytes.
	md[0] = byte(version)
	return md
}

func readMetadata(file *os.File, offset int64) (metadata, error) {

	// The below only works for vsn 1. When/if we change the version we need to read the first byte
	// and then read the metadata based on the version that was read in the first byte
	md := make([]byte, 6)
	if _, err := file.ReadAt(md, offset); err != nil {
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
