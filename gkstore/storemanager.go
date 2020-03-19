package gkstore

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

// StoreManager - a manager of Kvstores
type StoreManager struct {
	stores map[string]*Kvstore // Does this need to be a pointer? Are maps always pointers?
	config *Config
}

// StoreConfig - the Config per store
type StoreConfig struct {
	Name  string
	Files []string
}

// Config - the store config
// todo: rename this it's more about the current running stance
type Config struct {
	Stores []StoreConfig
}

// InitialiseStoreManager - inialise the store manager
func InitialiseStoreManager() *StoreManager {

	// We need to encapsulate this
	// Every time we update the config we want to write to the file
	configFile, err := os.OpenFile("/var/lib/gokave/store_data.json", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
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

	storeMap := make(map[string]*Kvstore)

	for _, store := range config.Stores {
		for _, file := range store.Files {
			fmt.Println("Initialising:", store.Name, "with file:", file)
			s := Open(file)
			storeMap[store.Name] = &s
		}
	}

	return &StoreManager{
		stores: storeMap,
		config: config,
	}
}

// AddStore - add a new store
func (storeManager *StoreManager) AddStore(storeName string) {
	// 1) Validate that the store doesn't exist (just check map doesn't exist or look for file(s)?)
	// 2) Create an in-memory version of the store
	// 3) Update the stores file

	if storeManager.stores[storeName] != nil {
		fmt.Println("Store already exists")
		return
	}

	fmt.Printf("Creating store: %s\n", storeName)

	s := Open(fmt.Sprintf("/var/lib/gokave/%s.gkv", storeName))
	storeManager.stores[storeName] = &s

	newStoreConfig := StoreConfig{
		Name:  storeName,
		Files: []string{fmt.Sprintf("/var/lib/gokave/%s.gkv", storeName)},
	}
	storeManager.config.Stores = append(storeManager.config.Stores, newStoreConfig)

	fmt.Println("Updated config:", storeManager.config)

	configString, err := json.Marshal(storeManager.config)
	if err != nil {
		log.Fatal(err)
	}

	// Now update the config with the updated config
	configFile, err := os.Create("/var/lib/gokave/store_data.json")
	if err != nil {
		log.Fatal(err)
	}
	defer configFile.Close()

	configFile.WriteString(string(configString))
}

// RemoveStore - remove a store
// Currently this works by being pretty destructive and deleting the data files
// probably want a slightly more nuanced option
// This also needs encapsulating so that we do things the right way round or have a process
// to clean up if it falls over mid way through
func (storeManager *StoreManager) RemoveStore(storeName string) {

	if storeManager.stores[storeName] == nil {
		fmt.Println("Store does not exist")
		return
	}

	fmt.Printf("Removing store: %s\n", storeName)

	// We need to encapsulate this - dupe of initialise.. And will be needed in other reading of files
	// Every time we update the config we want to write to the file
	configFile, err := os.OpenFile("/var/lib/gokave/store_data.json", os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer configFile.Close()

	byteValue, err := ioutil.ReadAll(configFile)
	if err != nil {
		log.Fatal(err)
	}

	config := new(Config)
	updatedConfig := new(Config)
	json.Unmarshal(byteValue, config)

	// Loop around the existing
	// If this is the store we want to delete remove all files
	// Otherwise add the Store to an updatedConfig
	for _, store := range config.Stores {
		if store.Name == storeName {
			for _, file := range store.Files {
				os.Remove(file)
				fmt.Printf("Removed file: %s\n", file)
			}
		} else {
			updatedConfig.Stores = append(updatedConfig.Stores, store)
		}
	}

	// Remove from the store map
	delete(storeManager.stores, storeName)

	// Update the config file
	configString, err := json.Marshal(updatedConfig)
	if err != nil {
		log.Fatal(err)
	}
	configFile.Truncate(0)
	configFile.Write(configString)

	fmt.Printf("Updated config: %v\n", updatedConfig.Stores)
}

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
