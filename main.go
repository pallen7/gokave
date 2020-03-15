package main

import (
	"fmt"
	"go_play/store"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"strings"
)

type requestHandler struct {
	storeManager *store.Manager
}

func main() {

	// TODO:
	// 1) Add a configuration file to the store manager and add ADD/DELETE store functions
	//    format: http://localhost:8080/store/admin/<store_name>
	// 2) Bug: reads the first value in the data file if you 'get' a non-existent key
	//         (or crashes if first read is non-existent)
	// 3) Sort out the critical sections. Look at RWMutex.
	// 4) Look at the best way to handle errors
	// 5) Review the program layout, naming conventions etc

	fmt.Println("Server started")
	m := store.InitialiseManager()
	r := requestHandler{
		storeManager: &m,
	}

	http.Handle("/store/", &r) // New creates a reference vs creating a var and passing the address
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// https://golang.org/pkg/net/http/#Handler
func (rHandler requestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	switch r.Method {
	case "POST":
		handlePost(rHandler.storeManager, w, r)
	case "GET":
		handleGet(rHandler.storeManager, w, r)
	default:
		fmt.Println("Unrecognised HTTP request type")
	}
}

func handlePost(storeManager *store.Manager, responseWriter http.ResponseWriter, httpRequest *http.Request) {
	// Here we want a URL in the format /store/type/id - (case insensitive)
	// We should wrap this up in a function
	dir, id := path.Split(strings.ToLower(httpRequest.URL.Path))
	cleanDir := strings.TrimPrefix(strings.TrimSuffix(dir, "/"), "/")
	dirs := strings.Split(cleanDir, "/")

	for i, d := range dirs {
		fmt.Println(i, d)
	}

	if len(dirs) != 2 {
		http.NotFound(responseWriter, httpRequest)
		return
	}
	if dirs[0] != "store" {
		http.NotFound(responseWriter, httpRequest)
		return
	}
	value, err := ioutil.ReadAll(httpRequest.Body)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Post: %s Store: %s Id: %s", value, dirs[1], id)
	store.WriteToStore(storeManager, dirs[1], value, id)
}

func handleGet(storeManager *store.Manager, responseWriter http.ResponseWriter, httpRequest *http.Request) {

	// Here we want a URL in the format /store/type/id - (case insensitive)
	// We should wrap this up in a function
	dir, id := path.Split(strings.ToLower(httpRequest.URL.Path))
	cleanDir := strings.TrimPrefix(strings.TrimSuffix(dir, "/"), "/")
	dirs := strings.Split(cleanDir, "/")

	for i, d := range dirs {
		fmt.Println(i, d)
	}

	if len(dirs) != 2 {
		http.NotFound(responseWriter, httpRequest)
		return
	}
	if dirs[0] != "store" {
		http.NotFound(responseWriter, httpRequest)
		return
	}
	fmt.Printf("Get %s from store: %s", id, dirs[1])
	bytes := store.ReadFromStore(storeManager, dirs[1], id)
	responseWriter.WriteHeader(200)
	responseWriter.Write(bytes)
}
