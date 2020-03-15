package store

// Manager - a manager of Kvstores
type Manager struct {
	stores map[string]*Kvstore
}

// InitialiseManager - inialise the store manager
func InitialiseManager() Manager {
	// This should read a config and open all of the stores in the config
	// Move away from the .txt extension
	s := Open("/var/log/gokave/people.txt")
	storeMap := make(map[string]*Kvstore)
	storeMap["people"] = &s
	return Manager{
		stores: storeMap,
	}
}

// func AddStore()
// func RemoveStore()

// ReadFromStore - reads from a store
func ReadFromStore(manager *Manager, storeName string, key string) []byte {
	s := manager.stores[storeName]
	return ReadData(s, key)
}

// WriteToStore - writes to a store
func WriteToStore(manager *Manager, storeName string, value []byte, key string) {
	s := manager.stores[storeName]
	WriteData(s, value, key)
}
