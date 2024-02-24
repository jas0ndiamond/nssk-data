#!/bin/bash

# run from same directory

# --restart=unless-stopped\

CONFIG_FILE="config.json"
DB_SETUP_SCRIPT="docker-entrypoint-initdb.d/0_nssk_setup.sql"

if [ ! -f $CONFIG_FILE ]; then
  echo "Missing config file $CONFIG_FILE. Create this file from the template."
  exit 1
fi

# build network if it's not already built
echo "Building network"
if [[ -z $(docker network ls | grep nssk-network) ]]; then
	echo "Creating nssk-network network"
	docker network create --driver=bridge --subnet=9.9.1.0/24 --gateway=9.9.1.1 nssk-network
else
	echo "nssk-network exists. Skipping creation"
fi

# docker run will create the mounts if they don't already exist

# MySQL needs a decent about of resources.

# Always mount a volume for logs, even if logging is disabled.

# start container
echo "Starting container"
docker run\
 --name=nssk-data\
 --network=nssk-network\
 -p 53306:3306\
 --cpus=2\
 --memory=2.5g\
 -e MYSQL_ROOT_PASSWORD="$(jq '."setup-pass"' < $CONFIG_FILE)"\
 -v "$(pwd)"/data:/var/lib/mysql\
 -v "$(pwd)"/conf.d:/etc/mysql/conf.d\
 -v "$(pwd)"/mysql:/var/log/mysql\
 -d\
 nssk-mysql &&

# TODO: may not always have logging enabled. add a switch for this or detect it from files in 'nssk-data/mysql'
# start fail2ban in container. requires mysql logs to be in place so wait for the database to fully start up.
echo "Waiting before fail2ban start" &&
sleep 40 &&

echo "Starting fail2ban" &&
docker exec -it nssk-data /etc/init.d/fail2ban start &&
sleep 10 &&
docker exec -it nssk-data /etc/init.d/fail2ban status &&
echo "Removing setup script from container filesystem" &&
docker exec -it nssk-data rm -v /docker-entrypoint-initdb.d/0_nssk_setup.sql &&
echo "Container startup completed. Move the config file to a secure location."

# TODO: check that /docker-entrypoint-initdb.d/ is empty