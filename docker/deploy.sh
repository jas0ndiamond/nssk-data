#!/bin/bash

# non-destructively remove and re-create the nssk-data container

# dont mind if the container doesn't exist
docker stop nssk-data
docker rm nssk-data

# leave /data/ directory alone

./build.sh db-setup.json &&
./start.sh db-setup.json
