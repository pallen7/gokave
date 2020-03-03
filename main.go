package main

import (
	"fmt"
	"go_play/store"
	"io/ioutil"
	"log"
	"net/http"
)

type requestHandler struct {
	kvStore *store.Kvstore
}

func main() {

	// TODO:
	// 1) Split into separate packages
	//      Store
	//		 - dataFile *File
	//		 - mapFile *File
	//		 - hashMap
	// 2) Store keys in a hash map
	// 3) Rebuild the hash map when the application start up
	// 4) Allow configuration of file locations (currently everything lives in tmp)
	// Add so that we can handle requests on a 'per store' level. i.e. /animals/cat /animals/dog etc..
	// Add ability to add a store, delete a store etc - this means we will need to dynamically handle routes

	fmt.Println("Server started")
	s := store.Open("/tmp/data.txt", "/tmp/map.txt")
	r := requestHandler{kvStore: &s}

	// Currently we can't do this due to the way we use ListenAndServe. We just have to murder the application
	// see below
	//defer store.Close()
	// http.HandleFunc("/", handler)

	// More elegant solution for start / stop of http server
	// https://stackoverflow.com/questions/39320025/how-to-stop-http-listenandserve
	http.Handle("/", &r) // New creates a reference vs creating a var and passing the address
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// https://golang.org/pkg/net/http/#Handler
func (rHandler requestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	fmt.Println("ServeHTTP")

	// If we just hold the keys as a map type then what happens if our system goes down?
	// If we write an append only map file with the last value for a given key being the current data location
	// then we can rebuild if needed
	// When we start the server up we should read our map file into our map data type
	// mapFile, err := os.OpenFile("/tmp/map.txt", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	// if err != nil {
	// 	// read up on the methods for error handling
	// 	log.Fatal(err)
	// }

	// defer mapFile.Close()

	// // Pick up the file name from configuration
	// // We also want to open the file at the beginning of the application and have it 'hittable'
	// // throughout
	// dataFile, err := os.OpenFile("/tmp/data.txt", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// // Deffered until the surrounding function is completed. Probably don't want this as want file constantly open
	// defer dataFile.Close()

	// the path is /add/ so get everything after the 5th char.
	// see if there is a better way to deal with routes

	switch method := r.Method; method {
	case "POST":
		handlePost(rHandler.kvStore, r)
	case "GET":
		handleGet(rHandler.kvStore, w, r)
	default:
		fmt.Println("Unrecognised HTTP request type")
	}
}

func handlePost(kvStore *store.Kvstore, httpRequest *http.Request) {
	// this is the value we want to save
	// need to add validation
	value, err := ioutil.ReadAll(httpRequest.Body)
	if err != nil {
		log.Fatal(err)
	}

	store.WriteData(kvStore, value, httpRequest.URL.Path[1:])
}

func handleGet(kvStore *store.Kvstore, responseWriter http.ResponseWriter, httpRequest *http.Request) {
	bytes := store.ReadData(kvStore, httpRequest.URL.Path[1:])
	responseWriter.WriteHeader(200)
	responseWriter.Write(bytes)
}
