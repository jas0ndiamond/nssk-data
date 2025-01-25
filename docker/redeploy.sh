#!/bin/bash

# destructively remove and re-create the nssk-data container

# dont mind if the container doesn't exist
docker stop nssk-data
docker container rm nssk-data

# purge database state
sudo rm -rf ./data/*

# run regular deploy
./deploy.sh
