package main

import (
	"fmt"
	"go_play/gkstore"
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
	storeManager *gkstore.StoreManager
}

type adminHandler struct {
	storeManager *gkstore.StoreManager
}

func main() {

	// TODO:
	// 1) Add a file to save store info and add ADD/DELETE store functions
	//    format: http://localhost:8080/store/admin/<store_name>
	// 2) Bug: reads the first value in the data file if you 'get' a non-existent key
	//         (or crashes if first read is non-existent)
	// 3) Sort out the critical sections. Look at RWMutex.
	// 4) Look at the best way to handle errors
	// 5) Review the program layout, naming conventions etc
	// 6) Add readme and sort out the comments for all of the public values
	// 7) Add in multiple files per store
	// 8) Add in the purging of old files

	fmt.Println("Server started")
	sm := gkstore.InitialiseStoreManager()
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
	default:
		fmt.Println("Unrecognised HTTP request type")
	}
}

func (aHandler adminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	switch r.Method {
	case "POST":
		handleAdminPost(aHandler.storeManager, w, r)
	case "GET":
		fmt.Println("Admin - GET")
	case "DELETE":
		fmt.Println("Admin - DELETE")
	default:
		fmt.Println("Unrecognised HTTP admin request type")
	}
}

func handleRequestPost(storeManager *gkstore.StoreManager, responseWriter http.ResponseWriter, httpRequest *http.Request) {
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
	storeManager.WriteToStore(dirs[1], value, id)
}

func handleRequestGet(storeManager *gkstore.StoreManager, responseWriter http.ResponseWriter, httpRequest *http.Request) {

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

func handleAdminPost(storeManager *gkstore.StoreManager, responseWriter http.ResponseWriter, httpRequest *http.Request) {
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
