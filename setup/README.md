# NSSK Database Setup

Stand up and configure a MySQL database in a docker container to store and serve NSSK data.

---

### Prerequisites
* Container host running docker
* Package `jq` installed on the container host
  * `apt-get install jq`

### Users and Access

The setup script generates an SQL script to create and configure the following users.

* `setup-user` - The MySQL root user. Access permitted from localhost only.
* `nssk` - User for most workflows accessing the data. Access permitted from WAN. Read-only.
* `nssk-admin` - User for admin-type tasks like restoring from backups. Access permitted from LAN and container networks.
* `nssk-import` - User for adding/modifying data. Access permitted from LAN and container networks. 
* `nssk-backup` - User for creating database backups. Access permitted from LAN and container networks. 

---
### Create config file from template

1. Create file `nssk-data/setup/db-setup.json` from the template `nssk-data/setup/db-setup.json.template`
2. Choose secure passwords, especially if database is web-facing. 

---

### Run database setup script

Set config values in `nssk-data/setup/generate_db_setup.py`
* Specifically the LAN_NETWORK and CONTAINER_NETWORK

Run the database setup generation script. Scripts will land in `nssk-data/docker/setup/`

```
cd nssk-data/setup
../venv/bin/python3 ./generate_db_setup.py conf/config.json
```

---

### Deploy container to container host

`rsync -rvh --mkpath nssk-data/docker user@my.container.host:/path/to/nssk-data`

`scp nssk-data/setup/conf/config.json user@my.container.host:/path/to/nssk-data/docker`

Switch over to the container host, and build the docker image with the build script:

```
cd nssk-data/docker
./build.sh
``` 

Next, run the start script:

```
./start.sh db-setup.json
```

Check the output for confirmation that the database user setup script was successfully deleted.

### Test Database Connection

Test access by logging in as user `nssk` with an SQL client.

`mysql -u nssk -P X3306 -h my.container.host -p NSSK_COSMO`

Database contents are persisted in the `data` directory created by running the container.

MySQL logging is enabled by default. Logs are persisted in the `mysql` directory created by running the container. 

Once access is confirmed, move or delete the config.json file from the container host.

### Notes

