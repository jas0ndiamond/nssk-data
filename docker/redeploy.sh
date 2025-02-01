#!/bin/bash

# destructively remove and re-create the nssk-data container
# dont mind if the container doesn't exist

CONFIG_FILE="./db-setup.json"

if [ -z "$CONFIG_FILE" ]; then
  echo "Usage: ./redeploy.sh config.json"
  exit 1
fi

CONTAINER_NAME="$(jq -r '.container_name' < "$CONFIG_FILE")"
if [ "$CONTAINER_NAME" == "null" ] || [ -z "$CONTAINER_NAME" ]; then
  echo "container_name not readable from config"
  exit 1
fi

# Check if the container is running
if docker ps --filter "name=$CONTAINER_NAME" --filter "status=running" --quiet; then
    echo "Container $CONTAINER_NAME is running. Stopping it."
    docker stop "$CONTAINER_NAME"
    docker container rm "$CONTAINER_NAME"
    docker wait "$CONTAINER_NAME"
else
    echo "Container $CONTAINER_NAME is not running. Continuing."
fi

# purge database state
sudo rm -rf ./data/*

# run regular deploy
./deploy.sh
