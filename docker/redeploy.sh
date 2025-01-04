#!/bin/bash

docker stop nssk-data;
docker container rm nssk-data;

sudo rm -rf ./data/*;

docker build -t nssk-mysql .;

./start.sh

echo "Waiting startup to complete and deleting the setup script..."
sleep 300

docker exec -it nssk-data rm docker-entrypoint-initdb.d/0_setup.sql
