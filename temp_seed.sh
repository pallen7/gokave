#!/bin/bash


# Use & after a command to make them backgroung processes (so they run asynch)
curl -X POST -d "{\"Name\": \"Frodo Baggins\"}" -H "Content-Type: application/json" http://localhost:8080/1 &
curl -X POST -d "{\"Name\": \"Paul Allen\"}" -H "Content-Type: application/json" http://localhost:8080/2 &
curl -X POST -d "{\"Name\": \"Anna Madden\"}" -H "Content-Type: application/json" http://localhost:8080/3 &
curl -X POST -d "{\"Name\": \"Bilbo Baggins\"}" -H "Content-Type: application/json" http://localhost:8080/1 &
