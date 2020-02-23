package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

func main() {

	fmt.Println("Server started")
	// Shouldn't we use Handle over HandleFunc? At least the way it's currently written
	http.HandleFunc("/", handler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handler(w http.ResponseWriter, r *http.Request) {

	// If we just hold the keys as a map type then what happens if our system goes down?
	// If we write an append only map file with the last value for a given key being the current data location
	// then we can rebuild if needed
	// When we start the server up we should read our map file into our map data type
	mapFile, err := os.OpenFile("/tmp/map.txt", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		// read up on the methods for error handling
		log.Fatal(err)
	}

	defer mapFile.Close()

	// Pick up the file name from configuration
	// We also want to open the file at the beginning of the application and have it 'hittable'
	// throughout
	dataFile, err := os.OpenFile("/tmp/data.txt", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}
	// Deffered until the surrounding function is completed. Probably don't want this as want file constantly open
	defer dataFile.Close()

	// the path is /add/ so get everything after the 5th char.
	// see if there is a better way to deal with routes
	key := r.URL.Path[1:]

	switch method := r.Method; method {
	case "POST":
		handlePost(mapFile, dataFile, key, r)
	case "GET":
		handleGet(mapFile, dataFile, key, w)
	default:
		fmt.Println("Unrecognised HTTP request type")
	}
}

func handlePost(mapFile *os.File, dataFile *os.File, key string, httpRequest *http.Request) {
	// this is the value we want to save
	// need to add validation
	value, err := ioutil.ReadAll(httpRequest.Body)
	if err != nil {
		log.Fatal(err)
	}

	// we shouldn't be getting the file size on every write but for the moment find out the length of the file
	// so we can create our kv map
	fileStat, err := dataFile.Stat()
	if err != nil {
		log.Fatal(err)
	}

	// Write the 'value' to the data file
	bytesWritten, err := dataFile.Write([]byte(value))

	if err != nil {
		log.Fatal(err)
	}

	keyMap := key + "!" + strconv.FormatInt(fileStat.Size(), 10) + "!" + strconv.Itoa(bytesWritten) + "\n"
	fmt.Println(keyMap)

	// for each key we want to hold start position and length
	if _, err := mapFile.Write([]byte(keyMap)); err != nil {
		log.Fatal(err)
	}
}

func handleGet(mapFile *os.File, dataFile *os.File, key string, responseWriter http.ResponseWriter) {
	// Pretty much a temporary function as we want this to be an in memory hash-map
	fmt.Println("Handling get")
	scanner := bufio.NewScanner(mapFile)

	var length int
	var location int64

	for scanner.Scan() {
		line := scanner.Text()
		fileKey := strings.Split(line, "!")
		if fileKey[0] == key {

			var err error
			location, err = strconv.ParseInt(fileKey[1], 10, 64)

			if err != nil {
				log.Fatal(err)
			}

			length, err = strconv.Atoi(fileKey[2])

			if err != nil {
				log.Fatal(err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	bytes := make([]byte, length)
	_, err := dataFile.ReadAt(bytes, location)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Key", key, "Location", location, "Length", length)
	responseWriter.WriteHeader(200)
	responseWriter.Write(bytes)
}
