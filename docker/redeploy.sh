docker stop nssk-data;
docker container rm nssk-data;

sudo rm -rf ./data/*;

docker build -t nssk-mysql .;

./start.sh
sleep 300

docker exec -it nssk-data rm docker-entrypoint-initdb.d/0_setup.sql
