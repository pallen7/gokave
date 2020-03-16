package gkstore

import (
	"fmt"
	"log"
)

// StoreManager - a manager of Kvstores
type StoreManager struct {
	stores map[string]*Kvstore // Does this need to be a pointer? Are maps always pointers?
}

// InitialiseStoreManager - inialise the store manager
func InitialiseStoreManager() *StoreManager {
	// This should read a stores file and open all of the stores in the config
	// Move away from the .txt extension to gkv
	storeMap := make(map[string]*Kvstore)

	fmt.Println("Initialising store: people")
	s := Open("/var/log/gokave/people.gkv")

	storeMap["people"] = &s
	return &StoreManager{
		stores: storeMap,
	}
}

// AddStore - add a new store
func (storeManager *StoreManager) AddStore(storeName string) {
	// 1) Validate that the store doesn't exist (just check map doesn't exist or look for file(s)?)
	// 2) Create an in-memory version of the store
	// 3) Update the stores file

	if storeManager.stores[storeName] != nil {
		log.Fatal("Store already exists")
	}

	fmt.Printf("Creating store: %s\n", storeName)

	s := Open("/var/log/gokave/%s.gkv")
	storeManager.stores[storeName] = &s
}

// func AddStore()
// func RemoveStore()

// ReadFromStore - reads from a store
func (storeManager *StoreManager) ReadFromStore(storeName string, key string) []byte {
	s := storeManager.stores[storeName]
	return ReadData(s, key)
}

// WriteToStore - writes to a store
func (storeManager *StoreManager) WriteToStore(storeName string, value []byte, key string) {
	s := storeManager.stores[storeName]
	WriteData(s, value, key)
}
