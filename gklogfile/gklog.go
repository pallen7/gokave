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
func (kvFile *KvFile) Delete(key string) {
	md := newMetadata(currentVsn)
	writeKeyMetadata(&md, len(key))
	writeValueMetadata(&md, 0)
	writeEntryType(&md, keyRemoved)

	// Write to the buffer
	writer := bufio.NewWriterSize(kvFile.file, len(md)+md.keyLength()+md.valueLength())
	writer.Write(md)
	writer.WriteString(key)

	_, err := writeToFile(kvFile.file, writer, &kvFile.fileWriteMutex)
	if err != nil {
		log.Fatal()
	}

	kvFile.fileMapMutex.Lock()
	delete(kvFile.fileMap, key)
	kvFile.fileMapMutex.Unlock()

	fmt.Printf("%s removed from %s\n", key, kvFile.file.Name())
}

// Read - the value for a given key
// todo:
// - remove debug statement(s)
// - sort error handling
// - shares a lot of overlap with initialiseFileMap. Can we create some common functions?
func (kvFile *KvFile) Read(key string) ([]byte, error) {
	kvFile.fileMapMutex.RLock()
	startLocation, keyFound := kvFile.fileMap[key]
	kvFile.fileMapMutex.RUnlock()

	if !keyFound {
		fmt.Printf("Key not found: %s\n", key)
		return make([]byte, 0), nil // todo: When reviewing errors we should create return a not_found error
	}

	md, err := readMetadata(kvFile.file, startLocation)
	if err != nil {
		return nil, err
	}

	// keyRemoved
	if md.entryType() == keyRemoved {
		return make([]byte, 0), nil
	}

	// keyAdded - return value (this is assumed, should we check the entry type is keyAdded?)
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

	md := newMetadata(currentVsn)
	writeKeyMetadata(&md, len(key))
	writeValueMetadata(&md, len(value))
	writeEntryType(&md, keyAdded)

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

		switch meta.entryType() {
		case keyAdded:
			fileMap[string(key)] = position
		case keyRemoved:
			delete(fileMap, string(key))
		default:
			log.Fatal("Unrecognised entryType")
		}

		position += int64(len(meta) + meta.keyLength() + meta.valueLength())

		fmt.Printf("\tKey: %s read at: %d\n", string(key), position)
	}

	return fileMap, nil
}

/*
Metadata functions: Possibly remove versioned metadata once the layout is stable

Layouts:
	version 1
		// byte 0		version
		// byte 1		keyLength
		// byte 2-5		valueLength
	version 2
		// byte 0		version
		// byte 1		keyLength
		// byte 2-5		valueLength
		// byte 6		recordType
*/
type metadata []byte

// todo: read and apply some of the ideas here: https://splice.com/blog/iota-elegant-constants-golang/
// type version int
// type entryType

const (
	v1 = iota + 1
	v2
)
const currentVsn = v2

const (
	keyAdded = iota
	keyRemoved
)

func readMetadata(file *os.File, offset int64) (metadata, error) {

	vsn := make([]byte, 1)
	if _, err := file.ReadAt(vsn, offset); err != nil {
		log.Fatal(err)
	}

	md := newMetadata(int(vsn[0]))
	if _, err := file.ReadAt(md, offset); err != nil {
		log.Fatal(err)
	}

	return md, nil
}

func newMetadata(version int) metadata {
	var md []byte
	switch version {
	case v1:
		md = make([]byte, 6)
	case v2:
		md = make([]byte, 7)
	default:
		log.Fatal("Unrecognised metadata version")
	}
	md[0] = byte(version)
	return md
}

func (md metadata) keyLength() int {
	var keyLength int
	switch int(md[0]) {
	case v1, v2:
		keyLength = int(md[1])
	default:
		log.Fatal("Unrecognised metadata version")
	}
	return keyLength
}

func (md metadata) valueLength() int {
	// https://play.golang.org/p/xXzANmB6PJU bitwise operators
	var valueLength int
	switch int(md[0]) {
	case v1, v2:
		valueLength = int(md[2]) +
			int(md[3])<<8 +
			int(md[4])<<16 +
			int(md[5])<<24
	default:
		log.Fatal("Unrecognised metadata version")
	}
	return valueLength
}

func (md metadata) entryType() int {
	var entryType int
	switch int(md[0]) {
	case v1:
		entryType = keyAdded
	case v2:
		entryType = int(md[6])
	default:
		log.Fatal("Unrecognised metadata version")
	}
	return entryType
}

func writeKeyMetadata(md *metadata, length int) {
	switch int((*md)[0]) {
	case v1, v2:
		(*md)[1] = byte(length)
	default:
		log.Fatal("Unrecognised metadata version")
	}
}

func writeValueMetadata(md *metadata, length int) {
	// https://play.golang.org/p/xXzANmB6PJU bitwise operators
	switch int((*md)[0]) {
	case v1, v2:
		(*md)[2] = byte(length)
		(*md)[3] = byte(length >> 8)
		(*md)[4] = byte(length >> 16)
		(*md)[5] = byte(length >> 24)
	default:
		log.Fatal("Unrecognised metadata version")
	}
}

func writeEntryType(md *metadata, entryType int) {
	switch int((*md)[0]) {
	case v2:
		(*md)[6] = byte(entryType)
	default:
		log.Fatal("Unrecognised metadata version")
	}
}
