package main

import (
	"encoding/json"
	"fmt"
	"gokave/gkstore"
	"io/ioutil"
	"log"
	"os"
)

// StoreManager - a manager of Kvstores
type StoreManager struct {
	stores map[string]*gkstore.KvStore
	config *Config
}

// StoreConfig - the Config per store
type StoreConfig struct {
	Name string
}

// Config - the store config
// todo: rename this it's more about the current running stance
type Config struct {
	Stores []StoreConfig
}

// InitialiseStoreManager - inialise the store manager
func InitialiseStoreManager() (*StoreManager, error) {

	// We need to encapsulate this
	// Every time we update the config we want to write to the file
	configFile, err := os.OpenFile("c:\\devwork\\go\\gokave_config\\store_data.json", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer configFile.Close()

	byteValue, err := ioutil.ReadAll(configFile)
	if err != nil {
		log.Fatal(err)
	}

	config := new(Config)
	json.Unmarshal(byteValue, config)

	storeMap := make(map[string]*gkstore.KvStore)

	for _, store := range config.Stores {
		// Each store will now live in a directory
		fmt.Println("Initialising:", store.Name)
		// todo: read the 'core' data directory from somewhere. Not sure is the dir config should be read a level down
		s, err := gkstore.Open(store.Name)
		// todo: decide how we want to handle a single store failure
		if err != nil {
			return nil, err
		}
		storeMap[store.Name] = s
		// for _, file := range store.Files {
		// 	fmt.Println("Initialising:", store.Name, "with file:", file)
		// 	s, err := gkstore.Open(file)
		// 	// todo: decide how we want to handle a single store failure
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// 	storeMap[store.Name] = s
		// }
	}

	return &StoreManager{
		stores: storeMap,
		config: config,
	}, nil
}

// AddStore - add a new store
func (storeManager *StoreManager) AddStore(storeName string) {
	// TODO: Fix this since we've moved to directory based stores
	// 1) Validate that the store doesn't exist (just check map doesn't exist or look for file(s)?)
	// 2) Create an in-memory version of the store
	// 3) Update the stores file

	if storeManager.stores[storeName] != nil {
		fmt.Println("Store already exists")
		return
	}

	fmt.Printf("Creating store: %s\n", storeName)

	// todo: Should the store manager handle the creation of the new file if it doesn't exist?
	s, err := gkstore.Open(fmt.Sprintf("c:\\devwork\\go\\gokave_data\\%s.gkv", storeName))
	if err != nil {
		log.Fatal(err)
	}
	storeManager.stores[storeName] = s

	newStoreConfig := StoreConfig{
		Name: storeName,
		//Files: []string{fmt.Sprintf("c:\\devwork\\go\\gokave_data\\%s.gkv", storeName)},
	}
	storeManager.config.Stores = append(storeManager.config.Stores, newStoreConfig)

	fmt.Println("Updated config:", storeManager.config)

	configString, err := json.Marshal(storeManager.config)
	if err != nil {
		log.Fatal(err)
	}

	// Now update the config with the updated config
	configFile, err := os.Create("c:\\devwork\\go\\gokave_config\\store_data.json")
	if err != nil {
		log.Fatal(err)
	}
	defer configFile.Close()

	configFile.WriteString(string(configString))
}

// // GetStore - get the details of the store
// func (storeManager *StoreManager) GetStore(storeName string) []byte {

// 	configFile, err := os.Open("c:\\devwork\\go\\gokave_config\\store_data.json")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer configFile.Close()

// 	byteValue, err := ioutil.ReadAll(configFile)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	config := new(Config)
// 	storeConfig := new(StoreConfig)
// 	json.Unmarshal(byteValue, config)

// 	for _, store := range config.Stores {
// 		if store.Name == storeName {
// 			storeConfig.Name = store.Name
// 			storeConfig.Files = store.Files
// 		}
// 	}

// 	// We need to return a notfound if we don't have a store (or empty bytes?)
// 	s, err := json.Marshal(storeConfig)
// 	if err != nil {
// 		log.Fatal()
// 	}

// 	return s
// }

// // RemoveStore - remove a store (change to Delete store to match the Restful API)
// // Currently this works by being pretty destructive and deleting the data files
// // probably want a slightly more nuanced option
// // This also needs encapsulating so that we do things the right way round or have a process
// // to clean up if it falls over mid way through
// func (storeManager *StoreManager) RemoveStore(storeName string) {

// 	if storeManager.stores[storeName] == nil {
// 		fmt.Println("Store does not exist")
// 		return
// 	}

// 	fmt.Printf("Removing store: %s\n", storeName)

// 	// We need to encapsulate this - dupe of initialise.. And will be needed in other reading of files
// 	// Every time we update the config we want to write to the file
// 	configFile, err := os.OpenFile("c:\\devwork\\go\\gokave_config\\store_data.json", os.O_RDWR, 0644)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer configFile.Close()

// 	byteValue, err := ioutil.ReadAll(configFile)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	config := new(Config)
// 	updatedConfig := new(Config)
// 	json.Unmarshal(byteValue, config)

// 	// Loop around the existing
// 	// If this is the store we want to delete remove all files
// 	// Otherwise add the Store to an updatedConfig
// 	for _, store := range config.Stores {
// 		if store.Name == storeName {
// 			for _, file := range store.Files {
// 				os.Remove(file)
// 				fmt.Printf("Removed file: %s\n", file)
// 			}
// 		} else {
// 			updatedConfig.Stores = append(updatedConfig.Stores, store)
// 		}
// 	}

// 	// Remove from the store map
// 	delete(storeManager.stores, storeName)

// 	// Update the config file
// 	configString, err := json.Marshal(updatedConfig)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	configFile.Truncate(0)
// 	configFile.Write(configString)

// 	fmt.Printf("Updated config: %v\n", updatedConfig.Stores)
// }

// DeleteFromStore - deletes from a store
func (storeManager *StoreManager) DeleteFromStore(storeName string, key string) {
	s := storeManager.stores[storeName]
	s.Delete(key)
}

// ReadFromStore - reads from a store
func (storeManager *StoreManager) ReadFromStore(storeName string, key string) []byte {
	s := storeManager.stores[storeName]
	// Todo - when processing multiple files we need read the flag (2nd param) to decide if blank value is deleted or not exists
	// Not sure if returning 3 values is bad form...?
	value, _, _ := s.Read(key)
	return value
}

// WriteToStore - writes to a store
func (storeManager *StoreManager) WriteToStore(storeName string, value []byte, key string) error {
	s := storeManager.stores[storeName]
	return s.Write(key, value)
}
