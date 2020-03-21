package gkstore

import (
	"fmt"
	"log"
	"os"
	"sync"
)

// Sitting on top of Kvstore we want a KvstoreManager
// Reads a stores.txt file (we need to block adding a store called stores)
// Initialise: Opens each of the stores and saves in a map[string]*store ?
// AddStore will update stores.txt and add an initialised store to the KvstoreManager
// RemoveStore will update stores.txt

// Kvstore I think this should only be a package level variable and not accessible to the
// outside world? - shit name too
// Should we add a store interface? - read up
type Kvstore struct {
	dataFile    *os.File
	dataFileMap map[string]int64
	mutex       sync.Mutex
}

// Open does the open
// todo: should return a pointer to a Kvstore
func Open(dataFileName string) Kvstore {
	fmt.Println("Initialising Store")

	// Review if this is the correct way to open the file..
	dataFile, err := os.OpenFile(dataFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}

	return Kvstore{
		dataFile:    dataFile,
		dataFileMap: initialiseDataFileMap(dataFile),
	}
}

// WriteData write some data
func WriteData(store *Kvstore, value []byte, key string) {

	// https://play.golang.org/p/xXzANmB6PJU bitwise operators 1 bytes keylength 3 bytes datalength

	// The below should be in a createMetadata function
	dataLength := len(value)
	keyLength := len(key)

	if dataLength > 16777215 {
		log.Fatal("Data length too long")
	}

	if keyLength > 255 {
		log.Fatal("Key length too long")
	}

	metadata := make([]byte, 4)

	metadata[0] = byte(keyLength)
	metadata[1] = byte(dataLength)
	metadata[2] = byte(dataLength >> 8)
	metadata[3] = byte(dataLength >> 16)

	// ** Critical section
	store.mutex.Lock()
	fmt.Println("Entering critical section. Writing:", key)

	// we shouldn't be getting the file size on every write but for the moment find out the length of the file
	// so we can create our kv map
	fileStat, err := store.dataFile.Stat()
	if err != nil {
		log.Fatal(err)
	}

	// Write the bytes written and 'value' to the data file.
	// append is a variadic function
	// the elipses (...) effectively take every value independently from the valueToWrite slice (is my understanding)
	// This bit needs rewriting. Would it be more efficient to write to a fixed size array
	if _, err = store.dataFile.Write(append(append(metadata, []byte(key)...), value...)); err != nil {
		log.Fatal(err)
	}

	store.dataFileMap[key] = fileStat.Size()

	store.mutex.Unlock()
	fmt.Println("Exiting critical section")
}

// DeleteData - remove from map and tombstone in the data file
func DeleteData(kvStore *Kvstore, key string) {

	// We should check that the key exists before we delete
	delete(kvStore.dataFileMap, key)

	keyLength := len(key)
	dataLength := -1

	// In the data file let's use dataLength of -1 to indicate deletion

	metadata := make([]byte, 4)

	metadata[0] = byte(keyLength)
	metadata[1] = byte(dataLength)
	metadata[2] = byte(dataLength >> 8)
	metadata[3] = byte(dataLength >> 16)

}

// ReadData read data from the store
func ReadData(kvStore *Kvstore, key string) []byte {

	location := kvStore.dataFileMap[key]

	metadata := make([]byte, 4)
	if _, err := kvStore.dataFile.ReadAt(metadata, location); err != nil {
		log.Fatal(err)
	}

	// First byte is keyLength. Next 3 bytes are dataLength
	keyLength := int(metadata[0])
	dataLength := int(metadata[3]) << 16
	dataLength += int(metadata[2]) << 8
	dataLength += int(metadata[1])

	data := make([]byte, dataLength)
	if _, err := kvStore.dataFile.ReadAt(data, location+4+int64(keyLength)); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Key", key, "Location", location, "dataLength", dataLength, "keyLength", keyLength)
	return data
}

func initialiseDataFileMap(dataFile *os.File) map[string]int64 {

	fileStat, err := dataFile.Stat()
	if err != nil {
		log.Fatal(err)
	}

	fileSize := fileStat.Size()
	var location int64 = 0

	fmt.Println("fileSize", fileSize)

	dataFileMap := make(map[string]int64)

	for location < fileSize {
		// Get metadata
		metadata := make([]byte, 4)
		if _, err := dataFile.ReadAt(metadata, location); err != nil {
			log.Fatal(err)
		}
		keyLength := int(metadata[0])
		dataLength := int(metadata[3]) << 16
		dataLength += int(metadata[2]) << 8
		dataLength += int(metadata[1])

		// Read the key value
		key := make([]byte, keyLength)
		if _, err := dataFile.ReadAt(key, location+4); err != nil {
			log.Fatal(err)
		}

		fmt.Println("Key:", string(key), "Read at:", location)

		dataFileMap[string(key)] = location

		location += int64(keyLength) + int64(dataLength) + 4
	}

	return dataFileMap
}

// Irrelevant until we handle the http shutdown more gracefully
// // Close does a close
// func Close() {
// 	fmt.Println("Closing Store")
// }
