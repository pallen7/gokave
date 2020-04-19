package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"strings"
)

// We are sharing a single store manager over multiple requests
// a) is this correct?
// b) can/should we package the below up as controllers?
type requestHandler struct {
	storeManager *StoreManager
}

type adminHandler struct {
	storeManager *StoreManager
}

func main() {

	// MVP:
	// 1) We want a gkstore package that contains the following:
	// - CreateStore()
	// - OpenStore()
	// - DeleteStore() -> ? If each store lives at the folder level can the user just delete the folder
	// - Write()
	// - Read()
	// - Delete()

	// 2) Shift everything else out of the package (reclassify API & storemanager as test harness?)
	// 4) Sit a gkstore on top of multiple gk files (create multiple when limit reached etc - purging blah)
	// 5) Add  validation around what can be used as store names, keys, validate JSON values etc
	// 7) Add readme and sort out the comments for all of the public values
	// 9) Add tests

	// Bugs:
	// - After deleting a store for the 2nd time got a load of random bytes turn up at the beginning of data.json

	// Future:
	// 1) Replication to multiple nodes

	fmt.Println("Server started")
	sm, _ := InitialiseStoreManager()
	r := &requestHandler{storeManager: sm}
	a := &adminHandler{storeManager: sm}

	http.Handle("/store/", r)
	http.Handle("/store/admin/", a)
	// How do we add in "/store/admin" ? - and how do we add these safely if we only have a pointer to 1 storemanager?
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// https://golang.org/pkg/net/http/#Handler
func (rHandler requestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	// I'm sure there must be a better way to handle this but:
	switch r.Method {
	case "POST":
		handleRequestPost(rHandler.storeManager, w, r)
	case "GET":
		handleRequestGet(rHandler.storeManager, w, r)
	case "DELETE":
		handleRequestDelete(rHandler.storeManager, w, r)
	default:
		fmt.Println("Unrecognised HTTP request type")
	}
}

func (aHandler adminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	switch r.Method {
	case "POST":
		handleAdminPost(aHandler.storeManager, w, r)
	// case "GET":
	// 	// Fill this in later.. Get Store config?
	// 	handleAdminGet(aHandler.storeManager, w, r)
	// case "DELETE":
	// 	handleAdminDelete(aHandler.storeManager, w, r)
	default:
		fmt.Println("Unrecognised HTTP admin request type")
	}
}

func handleRequestPost(storeManager *StoreManager, responseWriter http.ResponseWriter, httpRequest *http.Request) {
	// Here we want a URL in the format /store/type/id - (case insensitive)
	// We should wrap this up in a function
	dir, id := path.Split(strings.ToLower(httpRequest.URL.Path))
	cleanDir := strings.TrimPrefix(strings.TrimSuffix(dir, "/"), "/")
	dirs := strings.Split(cleanDir, "/")

	if len(dirs) != 2 {
		http.NotFound(responseWriter, httpRequest)
		return
	}
	// Why do we need the below? Can't remember the reason since the http.handle sets this up
	if dirs[0] != "store" {
		http.NotFound(responseWriter, httpRequest)
		return
	}
	value, err := ioutil.ReadAll(httpRequest.Body)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Post: %s Store: %s Id: %s\n", value, dirs[1], id)
	if err := storeManager.WriteToStore(dirs[1], value, id); err != nil {
		http.Error(responseWriter, err.Error(), 500)
		fmt.Println(err)
	}
}

func handleRequestGet(storeManager *StoreManager, responseWriter http.ResponseWriter, httpRequest *http.Request) {

	// Here we want a URL in the format /store/type/id - (case insensitive)
	// We should wrap this up in a function
	// rename id to resource?
	dir, id := path.Split(strings.ToLower(httpRequest.URL.Path))
	cleanDir := strings.TrimPrefix(strings.TrimSuffix(dir, "/"), "/")
	dirs := strings.Split(cleanDir, "/")

	if len(dirs) != 2 {
		http.NotFound(responseWriter, httpRequest)
		return
	}
	// Why do we need the below? Can't remember the reason since the http.handle sets this up
	if dirs[0] != "store" {
		http.NotFound(responseWriter, httpRequest)
		return
	}
	fmt.Printf("Get %s from store: %s\n", id, dirs[1])
	bytes := storeManager.ReadFromStore(dirs[1], id)
	responseWriter.WriteHeader(200)
	responseWriter.Write(bytes)
}

func handleRequestDelete(storeManager *StoreManager, responseWriter http.ResponseWriter, httpRequest *http.Request) {

	dir, id := path.Split(strings.ToLower(httpRequest.URL.Path))
	cleanDir := strings.TrimPrefix(strings.TrimSuffix(dir, "/"), "/")
	dirs := strings.Split(cleanDir, "/")

	if len(dirs) != 2 {
		http.NotFound(responseWriter, httpRequest)
		return
	}
	// Why do we need the below? Can't remember the reason since the http.handle sets this up
	if dirs[0] != "store" {
		http.NotFound(responseWriter, httpRequest)
		return
	}
	fmt.Printf("Delete %s from store: %s\n", id, dirs[1])
	storeManager.DeleteFromStore(dirs[1], id)
}

func handleAdminPost(storeManager *StoreManager, responseWriter http.ResponseWriter, httpRequest *http.Request) {
	// Here we want a URL in the format /store/admin/resource - (case insensitive)
	// We should wrap this up in a function
	// This is a bit turd - need to clean up all of the routing
	dir, id := path.Split(httpRequest.URL.Path)
	cleanDir := strings.TrimPrefix(strings.TrimSuffix(dir, "/"), "/")
	dirs := strings.Split(cleanDir, "/")

	if len(dirs) != 2 {
		http.NotFound(responseWriter, httpRequest)
		return
	}

	fmt.Printf("Create store: %s:\n", id)
	storeManager.AddStore(id)
}

// func handleAdminDelete(storeManager StoreManager, responseWriter http.ResponseWriter, httpRequest *http.Request) {
// 	dir, id := path.Split(httpRequest.URL.Path)
// 	cleanDir := strings.TrimPrefix(strings.TrimSuffix(dir, "/"), "/")
// 	dirs := strings.Split(cleanDir, "/")

// 	if len(dirs) != 2 {
// 		http.NotFound(responseWriter, httpRequest)
// 		return
// 	}

// 	fmt.Printf("Remove store: %s:\n", id)
// 	storeManager.RemoveStore(id)
// }

// func handleAdminGet(storeManager StoreManager, responseWriter http.ResponseWriter, httpRequest *http.Request) {
// 	dir, id := path.Split(httpRequest.URL.Path)
// 	cleanDir := strings.TrimPrefix(strings.TrimSuffix(dir, "/"), "/")
// 	dirs := strings.Split(cleanDir, "/")

// 	if len(dirs) != 2 {
// 		http.NotFound(responseWriter, httpRequest)
// 		return
// 	}

// 	fmt.Printf("Get store: %s:\n", id)
// 	bytes := storeManager.GetStore(id)
// 	responseWriter.WriteHeader(http.StatusOK)
// 	responseWriter.Write(bytes)
// }
