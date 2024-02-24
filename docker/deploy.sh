#!/bin/bash

docker stop nssk-data;
docker rm nssk-data;

#sudo rm -rf ./data/*;

docker build -t nssk-mysql .;

./start.sh;
