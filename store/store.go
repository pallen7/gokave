package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Kvstore I think this should only be a package level variable and not accessible to the
// outside world? - shit name too
// Should we add a store interface? - read up
type Kvstore struct {
	dataFile *os.File
	mapFile  *os.File
	mutex    sync.Mutex
}

// Open does the open
func Open(dataFileName string, mapFileName string) Kvstore {
	fmt.Println("Initialising Store")

	// todo: on startup read the keys from the map file into a map

	dataFile, err := os.OpenFile(dataFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}

	mapFile, err := os.OpenFile(mapFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}

	return Kvstore{
		dataFile: dataFile,
		mapFile:  mapFile,
	}
}

// WriteData write some data
func WriteData(store *Kvstore, value []byte, key string) {

	// https://play.golang.org/p/xXzANmB6PJU bitwise operators 1 bytes keylength 3 bytes datalength

	// The below should be in a createMetadata function
	dataLength := len(value)
	keyLength := len(key)

	if dataLength > 16777215 {
		log.Fatal("dataLength too long")
	}

	if keyLength > 16777215 {
		log.Fatal("dataLength too long")
	}

	metadata := make([]byte, 4)

	metadata[0] = byte(keyLength)
	metadata[1] = byte(dataLength)
	metadata[2] = byte(dataLength >> 8)
	metadata[3] = byte(dataLength >> 16)

	// ** Critical section
	store.mutex.Lock()
	fmt.Println("Entering critical section")

	time.Sleep(1 * time.Second)

	// we shouldn't be getting the file size on every write but for the moment find out the length of the file
	// so we can create our kv map
	fileStat, err := store.dataFile.Stat()
	if err != nil {
		log.Fatal(err)
	}

	// Write the bytes written and 'value' to the data file.
	// append is a variadic function
	// the elipses (...) effectively take every value independently from the valueToWrite slice (is my understanding)
	if _, err = store.dataFile.Write(append(metadata, value...)); err != nil {
		log.Fatal(err)
	}

	keyMap := key + "!" + strconv.FormatInt(fileStat.Size(), 10) + "\n"
	fmt.Println(keyMap)

	// for each key we want to hold start position and length
	if _, err := store.mapFile.Write([]byte(keyMap)); err != nil {
		log.Fatal(err)
	}

	// ** End critical section
	// not idiomatic. We should defer this call
	store.mutex.Unlock()
}

// ReadData read data from the store
func ReadData(kvStore *Kvstore, key string) []byte {
	// Pretty much a temporary function as we want this to be an in memory hash-map
	// But this can make up the bulk of our hash map rehydration
	fmt.Println("Handling get")
	kvStore.mapFile.Seek(0, 0)
	scanner := bufio.NewScanner(kvStore.mapFile)

	var location int64

	for scanner.Scan() {
		line := scanner.Text()
		fileKey := strings.Split(line, "!")
		if fileKey[0] == key {

			var err error
			location, err = strconv.ParseInt(fileKey[1], 10, 64)

			if err != nil {
				log.Fatal(err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	lengthToReadBytes := make([]byte, 4)
	if _, err := kvStore.dataFile.ReadAt(lengthToReadBytes, location); err != nil {
		log.Fatal(err)
	}

	bytesRead := make([]byte, binary.LittleEndian.Uint32(lengthToReadBytes))
	if _, err := kvStore.dataFile.ReadAt(bytesRead, location+4); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Key", key, "Location", location, "Length", lengthToReadBytes)
	return bytesRead
}

// Irrelevant until we handle the http shutdown more gracefully
// // Close does a close
// func Close() {
// 	fmt.Println("Closing Store")
// }
