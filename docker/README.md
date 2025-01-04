# NSSK Database

Stand up a containerized database to house NSSK data imported from various sources.

---
# Setup

---

### Build Container

```
cd nssk-data/docker
docker build -t nssk-mysql .
```

### Start Container

Create root pw file

Creates network 9.9.1.0



```
cd nssk-data/docker
./start
```

The MySQL database port is forwarded from 3306 to 23306 by default.
If the database is web-facing, configure router port-forwarding if necessary.

---

## Backups

* Create a backup
  * cronjob
* Restore from backup
  * With `nssk-admin` user

---

## Purge
