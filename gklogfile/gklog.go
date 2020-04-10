package gklogfile

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"
)

// KvFile is an individual Key Value file backed by an append only log file
type KvFile struct {
	file           *os.File
	fileWriteMutex sync.Mutex
	fileMap        map[string]int64
	fileMapMutex   sync.RWMutex
}

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
func Read() {

}

// Write - writes a Key Value pair to the file
// Going to make the file & map write a goroutine so we can queue up our writes

// Should return number of bytes written (possibly?) and error
func (store *KvFile) Write(value []byte, key string) {

	// Move these into the writeMetadata once we understand how to create custom errors
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

	// Write to the buffer
	writer := bufio.NewWriterSize(store.file, len(md)+md.keyLength()+md.valueLength())
	writer.Write(md)
	writer.WriteString(key)
	writer.Write(value)

	location, err := writeToFile(store.file, writer, &store.fileWriteMutex)
	if err != nil {
		log.Fatal()
	}

	// Note that it is dangerous to update a map that could also be being read
	// https://stackoverflow.com/questions/36167200/how-safe-are-golang-maps-for-concurrent-read-write-operations
	store.fileMapMutex.Lock()
	store.fileMap[key] = location
	store.fileMapMutex.Unlock()

	fmt.Printf("%s written to %s\n", key, store.file.Name())
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
	// byte 2-5		valueLength

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
