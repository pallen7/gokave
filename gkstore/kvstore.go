package gkstore

import (
	"fmt"
	"gokave/gklogfile"
	"io/ioutil"
	"log"
	"strings"
	"sync"
	"time"
)

// KvStore manages a set of KV files comprising a Store
type KvStore struct {
	storeName    string
	files        []*gklogfile.KvFile
	newFileMutex sync.RWMutex // used as an exclusive lock as we only want to add a new file when the current file isn't being written to
}

// Open - temporary pass through
func Open(storeName string) (store *KvStore, err error) {
	// Todo list:
	// If we don't find the directory (i.e. this is likely a new store() should we fail and require an initialisation or initialise here?
	// If we don't find any files.. Same question as above
	// check if store.files != nil -> should be when we call open or it indicates that we have already opened the store
	// When we open a data store can we take a lock on the directory (or all of the files?)

	// ReadDir returns files sorted by filename
	fileInfos, err := ioutil.ReadDir(fmt.Sprintf("c:\\devwork\\go\\gokave_data\\%s", storeName))
	if err != nil {
		return
	}

	store = &KvStore{storeName: storeName}

	for _, fileInfo := range fileInfos {
		fileParts := strings.Split(fileInfo.Name(), ".")

		// Validate
		if len(fileParts) != 2 {
			// Need to add some kind of logging mechanism to log a warning/info
			fmt.Printf("Bad filename: %s", fileInfo.Name())
			continue
		}

		if fileParts[1] != "gkv" {
			fmt.Printf("Bad filename: %s", fileInfo.Name())
			continue
		}

		// Need to validate the filename is a numeric in a decent unix nanosecond time range
		fmt.Printf("KvStore.Open(%s)\n", fmt.Sprintf("c:\\devwork\\go\\gokave_data\\%s\\%s\n", storeName, fileInfo.Name()))

		f, err := gklogfile.Open(fmt.Sprintf("c:\\devwork\\go\\gokave_data\\%s\\%s", storeName, fileInfo.Name()))
		// Not sure that we should be bailing out here.. Maybe report a corruption error or try to fix? - work out later
		if err != nil {
			return store, err
		}

		// Note: append works on nil slices (which store should be when first passed in to open)
		store.files = append(store.files, f)
	}
	return
}

// Delete - temporary pass through
func (kvStore *KvStore) Delete(key string) (err error) {
	if len(kvStore.files) <= 0 {
		log.Fatal("No files")
	}
	count := len(kvStore.files)
	return kvStore.files[count-1].Delete(key)
}

// Read - temporary pass through
// todo: we need to take notice of the flag that is returned to differentiate between not found and deleted
func (kvStore *KvStore) Read(key string) (value []byte, flag int, err error) {
	// We need something more elegant than this
	if len(kvStore.files) <= 0 {
		log.Fatal("No files")
	}
	// Need some locking around here when we introduce file purging
	for i := len(kvStore.files) - 1; i >= 0; i-- {
		value, flag, err = kvStore.files[i].Read(key)
		if flag == gklogfile.KeyDeleted || flag == gklogfile.KeyWritten {
			return
		}
	}
	return
}

// Write - temporary pass through
func (kvStore *KvStore) Write(key string, value []byte) (err error) {
	// So here we want to check the size of the file and if it's > max size we should create
	// a new one
	// The consideration that we have to think about is that the latest file could already be in the process
	// of being written to. Could a RWMutex help us here..? As long as we haven't hit a crucial file size we
	// we can allow as many processes as are needed
	//
	count := len(kvStore.files)
	err = kvStore.files[count-1].Write(key, value)
	size, _ := kvStore.files[count-1].Size()
	// Just use 100 for the moment
	if size > 100 {
		newFile, err2 := gklogfile.Open(fmt.Sprintf("c:\\devwork\\go\\gokave_data\\%s\\%d.gkv", kvStore.storeName, time.Now().UTC().UnixNano()))
		if err != nil {
			return err2
		}
		// This needs to be in a write mutex when we update the current file. All operations apart from this are 'read'
		kvStore.files = append(kvStore.files, newFile)
	}
	fmt.Printf("File size: %d\n", size)
	return

}
