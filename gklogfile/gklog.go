package gklogfile

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
)

// todo(?):
/*
-- if we do the below then always use byte slices not strings?
type KvReader interface {
	Read(k string) (v []byte, err error)
}
type KvWriter interface {
	Write(k string, v []byte) (err error)
}
type KvDeleter interface {
	Read(k string) (err error)
}
*/

// https://blog.golang.org/error-handling-and-go
// https://golang.org/pkg/errors
// todo: we probably want to make the error handling more sophisticated so read and initialise errors
// return enough information to allow corrupt files to be fixed

// ErrUnrecognisedMetadataVsn means the metadata processing wasn't possible due to
// an unrecognised version
// This would be indicative of data corruption on this value or the value before
// we need to augment this with metadata to allow a fix of a corrupted file
var ErrUnrecognisedMetadataVsn = errors.New("Unrecognised metadata version")

// ErrUnrecognisedLogType means an invalid log type was encountered
var ErrUnrecognisedLogType = errors.New("Unrecognised log entry type")

const (
	// KeyWritten means the key has been written to the file / is present
	KeyWritten = iota
	// KeyDeleted means the key has been flagged as deleted
	KeyDeleted
	// KeyNotPresent means the key cannot be found is not present in the file
	KeyNotPresent
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
// up a level when we get to multiple files per store?
func Open(fileName string) (kvFile *KvFile, err error) {
	// We want an append only file but still allow concurrent reads. Pass the respobnsibility for this the OS as per:
	// https://stackoverflow.com/questions/37628873/golang-simultaneous-read-write-to-the-file-without-explicit-file-lock
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	fileMap, err := initialiseFileMap(file)
	if err != nil {
		return
	}
	kvFile = &KvFile{
		file:    file,
		fileMap: fileMap,
	}
	return
}

// Close - tbc
func Close() {

}

// Delete - delete a value from the store
func (kvFile *KvFile) Delete(key string) (err error) {
	md, _ := newMetadata(currentVsn)
	writeEntryType(md, KeyDeleted)
	writeValueMetadata(md, 0)
	if err = writeKeyMetadata(md, len(key)); err != nil {
		return
	}

	// Write to the buffer
	writer := bufio.NewWriterSize(kvFile.file, len(md)+len(key))
	if _, err = writer.Write(md); err != nil {
		return
	}
	if _, err = writer.WriteString(key); err != nil {
		return
	}

	// Flush buffer to file
	location, err := writeToFile(kvFile.file, writer, &kvFile.fileWriteMutex)
	if err != nil {
		return
	}

	kvFile.fileMapMutex.Lock()
	kvFile.fileMap[key] = location
	kvFile.fileMapMutex.Unlock()
	return
}

// Read - the value for a given key
// If we pass in a readerat (file) we remove our file dependency and that can sit with the kvFileManager
func (kvFile *KvFile) Read(key string) (value []byte, flag int, err error) {
	kvFile.fileMapMutex.RLock()
	offset, ok := kvFile.fileMap[key]
	kvFile.fileMapMutex.RUnlock()
	if !ok {
		return nil, KeyNotPresent, err
	}

	md, err := readMetadata(kvFile.file, offset)
	if err != nil {
		return
	}

	if flag = metadataEntryType(md); flag == KeyDeleted {
		return
	}

	valueOffset := offset + int64(len(md)+metdataKeyLength(md))
	value = make([]byte, metadataValueLength(md))
	_, err = kvFile.file.ReadAt(value, valueOffset)
	return
}

// Write - writes a Key Value pair to the file
// If we pass in an io.writer then we remove our reliance on a file at this level?
func (kvFile *KvFile) Write(key string, value []byte) (err error) {
	md, _ := newMetadata(currentVsn)
	writeEntryType(md, KeyWritten)
	if err = writeKeyMetadata(md, len(key)); err != nil {
		return
	}
	if err = writeValueMetadata(md, len(value)); err != nil {
		return
	}

	// Write to the buffer
	writer := bufio.NewWriterSize(kvFile.file, len(md)+len(key)+len(value))
	if _, err = writer.Write(md); err != nil {
		return
	}
	if _, err = writer.WriteString(key); err != nil {
		return
	}
	if _, err = writer.Write(value); err != nil {
		return
	}
	// Flush buffer to file
	location, err := writeToFile(kvFile.file, writer, &kvFile.fileWriteMutex)
	if err != nil {
		return
	}

	// Note that it is dangerous to update a map that could also be being read
	// https://stackoverflow.com/questions/36167200/how-safe-are-golang-maps-for-concurrent-read-write-operations
	kvFile.fileMapMutex.Lock()
	kvFile.fileMap[key] = location
	kvFile.fileMapMutex.Unlock()
	return
}

//
// *** Internal functions
//

func writeToFile(file *os.File, writer *bufio.Writer, mutex *sync.Mutex) (location int64, err error) {
	mutex.Lock()
	defer mutex.Unlock()

	fileStat, err := file.Stat()
	if err != nil {
		return 0, err
	}

	if err = writer.Flush(); err != nil {
		return 0, err
	}

	return fileStat.Size(), err
}

func initialiseFileMap(file *os.File) (fileMap map[string]int64, err error) {
	fileStat, err := file.Stat()
	if err != nil {
		return fileMap, err
	}
	fileMap = make(map[string]int64)
	fileSize := fileStat.Size()

	for position := int64(0); position < fileSize; {
		md, err := readMetadata(file, position)
		if err != nil {
			return fileMap, err
		}
		keyLength := metdataKeyLength(md)
		valueLength := metadataValueLength(md)

		key := make([]byte, int64(keyLength))
		keyOffset := position + int64(len(md))
		if _, err := file.ReadAt(key, keyOffset); err != nil {
			return fileMap, err
		}

		switch metadataEntryType(md) {
		case KeyWritten:
			fileMap[string(key)] = position
		case KeyDeleted:
			delete(fileMap, string(key))
		default:
			return fileMap, ErrUnrecognisedLogType
		}
		position += int64(len(md) + keyLength + valueLength)
		fmt.Printf("\tKey: %s read at: %d\n", string(key), position)
	}
	return
}

/*
Metadata layouts:
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

const (
	v1 = iota + 1
	v2
)
const currentVsn = v2
const maxKeyLength = 255
const maxValueLength = 2147483647

func readMetadata(file *os.File, offset int64) (md []byte, err error) {
	// Read the version - always the first byte
	vsn := make([]byte, 1)
	if _, err = file.ReadAt(vsn, offset); err != nil {
		return md, err
	}
	// Make a byte slice of the correct size based on the version
	md, err = newMetadata(vsn[0])
	if err != nil {
		return md, err
	}
	_, err = file.ReadAt(md, offset)
	return
}

func newMetadata(version byte) (md []byte, err error) {
	switch version {
	case v1:
		md = make([]byte, 6)
	case v2:
		md = make([]byte, 7)
	default:
		return md, ErrUnrecognisedMetadataVsn
	}
	md[0] = version
	return
}

func metdataKeyLength(md []byte) (keyLength int) {
	switch int(md[0]) {
	case v1, v2:
		keyLength = int(md[1])
	default:
		log.Fatal(ErrUnrecognisedMetadataVsn.Error())
	}
	return
}

func metadataValueLength(md []byte) (valueLength int) {
	// https://play.golang.org/p/xXzANmB6PJU bitwise operators
	switch int(md[0]) {
	case v1, v2:
		valueLength = int(md[2]) +
			int(md[3])<<8 +
			int(md[4])<<16 +
			int(md[5])<<24
	default:
		log.Fatal(ErrUnrecognisedMetadataVsn.Error())
	}
	return
}

func metadataEntryType(md []byte) (entryType int) {
	switch int(md[0]) {
	case v1:
		// Default v1 entries to added as there was no delete
		entryType = KeyWritten
	case v2:
		entryType = int(md[6])
	default:
		log.Fatal(ErrUnrecognisedMetadataVsn.Error())
	}
	return
}

func writeKeyMetadata(md []byte, length int) (err error) {
	if length > maxKeyLength {
		return fmt.Errorf("Key too long. Max length: %d %d", maxKeyLength, length)
	}

	switch int(md[0]) {
	case v1, v2:
		md[1] = byte(length)
	default:
		log.Fatal(ErrUnrecognisedMetadataVsn.Error())
	}
	return
}

func writeValueMetadata(md []byte, length int) (err error) {
	if length > maxValueLength {
		return fmt.Errorf("Value too long. Max length: %d %d", maxValueLength, length)
	}
	// https://play.golang.org/p/xXzANmB6PJU bitwise operators
	switch int(md[0]) {
	case v1, v2:
		md[2] = byte(length)
		md[3] = byte(length >> 8)
		md[4] = byte(length >> 16)
		md[5] = byte(length >> 24)
	default:
		log.Fatal(ErrUnrecognisedMetadataVsn.Error())
	}
	return
}

func writeEntryType(md []byte, entryType int) {
	switch int(md[0]) {
	case v2:
		md[6] = byte(entryType)
	default:
		log.Fatal(ErrUnrecognisedMetadataVsn.Error())
	}
}
