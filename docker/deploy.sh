#!/bin/bash

# non-destructively remove and re-create the nssk-data container

docker stop nssk-data
docker rm nssk-data

#sudo rm -rf ./data/*

docker build -t nssk-mysql .

./start.sh db-setup.json
