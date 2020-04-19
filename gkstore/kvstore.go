package gkstore

import (
	"fmt"
	"gokave/gklogfile"
	"io/ioutil"
	"strings"
)

// Store todos:
// Should we hold the data at a folder level?
// Do we base the files (read vs write) based on config or filenames? - could use: time.Now().UTC().UnixNano()

// KvStore manages a set of KV files comprising a Store
type KvStore struct {
	file *gklogfile.KvFile
}

// Open - temporary pass through
func Open(storeName string) (store *KvStore, err error) {
	//gkstore.Open(fmt.Sprintf("c:\\devwork\\go\\gokave_data\\%s.gkv", storeName))

	// Move the data dir into config
	// ReadDir returns files sorted by filename
	files, err := ioutil.ReadDir(fmt.Sprintf("c:\\devwork\\go\\gokave_data\\%s", storeName))
	if err != nil {
		return
	}

	// Count the amount of files and create a slice of files to live in the store
	// The last one that we read is going to be the latest file as we automatically sort by filename

	for _, file := range files {
		fileParts := strings.Split(file.Name(), ".")

		// Validate
		if len(fileParts) != 2 {
			// Need to add some kind of logging mechanism to log a warning/info
			fmt.Printf("Bad filename: %s", file.Name())
			continue
		}

		if fileParts[1] != "gkv" {
			fmt.Printf("Bad filename: %s", file.Name())
			continue
		}

		if fileParts[1] != "gkv" {
			fmt.Printf("Bad filename: %s", file.Name())
			continue
		}

		fmt.Printf("KvStore.Open(%s)", file.Name())
		f, err := gklogfile.Open(file.Name())
		return &KvStore{file: f}, err
	}

	return
}

// Delete - temporary pass through
func (kvStore *KvStore) Delete(key string) (err error) {
	return kvStore.file.Delete(key)
}

// Read - temporary pass through
func (kvStore *KvStore) Read(key string) (value []byte, flag int, err error) {
	return kvStore.file.Read(key)
}

// Write - temporary pass through
func (kvStore *KvStore) Write(key string, value []byte) (err error) {
	return kvStore.file.Write(key, value)
}
