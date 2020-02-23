package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
)

func main() {

	fmt.Println("Server started")
	http.HandleFunc("/add/", handler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handler(w http.ResponseWriter, r *http.Request) {

	// the route shouldn't be add. we want 1 base url that can serve
	// GET, POST, DELETE (PUT?) for a specified key

	// the path is /add/ so get everything after the 5th char.
	// see if there is a better way to deal with routes
	key := r.URL.Path[5:]
	// this is the value we want to save
	// need to add validation
	value, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}

	// Pick up the file name from configuration
	// We also want to open the file at the beginning of the application and have it 'hittable'
	// throughout
	file, err := os.OpenFile("/tmp/data.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	// Deffered until the surrounding function is completed. Probably don't want this as want file constantly open
	defer file.Close()

	// we shouldn't be getting the file size on every write but for the moment find out the length of the file
	// so we can create our kv map
	fileStat, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	// Pick up the value to write from an incoming post request
	if _, err := file.Write([]byte(value)); err != nil {
		log.Fatal(err)
	}

	// If we just hold the keys as a map type then what happens if our system goes down?
	// If we write an append only map file with the last value for a given key being the current data location
	// then we can rebuild if needed
	// When we start the server up we should read our map file into our map data type
	mapFile, err := os.OpenFile("/tmp/map.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	keyMap := key + "!" + strconv.FormatInt(fileStat.Size(), 10) + "\n"
	fmt.Println(keyMap)

	// for each key we want to hold start position and length
	if _, err := mapFile.Write([]byte(keyMap)); err != nil {
		log.Fatal(err)
	}

}
