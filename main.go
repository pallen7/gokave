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
	// 4) Allow configuration of file locations (currently everything lives in tmp)
	// 5) Add so that we can handle requests on a 'per store/bucket' level. i.e. /animals/cat /animals/dog etc..
	// 6) Add ability to add a store, delete a store etc - this means we will need to dynamically handle routes
	// 7) Bug: doesn't create file on first run?
	// 8) Bug: reads the first value in the data file if you 'get' a non-existent key
	// 9) Sort out the critical sections. Look at RWMutex.
	// 10) Change so we don't have to murder the application to stop the server - // https://stackoverflow.com/questions/39320025/how-to-stop-http-listenandserve
	// 10b) Then we can add in a call to store.close() to elegantly close the files

	fmt.Println("Server started")
	s := store.Open("/tmp/data.txt")
	r := requestHandler{kvStore: &s}

	http.Handle("/", &r) // New creates a reference vs creating a var and passing the address
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// https://golang.org/pkg/net/http/#Handler
func (rHandler requestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	fmt.Println("ServeHTTP")

	switch r.Method {
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
