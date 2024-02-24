#!/bin/bash

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"

BACKUP_DIR="$SCRIPT_DIR"../../backups
CRED_FILE="$SCRIPT_DIR"/../config.json

if [ ! -f "$CRED_FILE" ]; then
  echo "Credential file not found"
  exit 1
fi

HOST="localhost"
USER="nssk_backup"
PORT="$(jq '.port' < "$CRED_FILE")"
CRED="$(jq '.users.nssk_backup' < "$CRED_FILE")"

if [[ -z $CRED ]]; then
  echo "Could not read credentials"
  exit 1
fi

# create directory if it doesn't exist
if [ ! -d "$BACKUP_DIR" ]; then
  mkdir -p "$BACKUP_DIR"

  if [ ! -d "$BACKUP_DIR" ]; then
    echo "Failed to create backup directory. Exiting"
    exit 1
  fi
fi

TIMESTAMP=$(date +"%Y-%m-%d_%H%m%S")
DUMP_FILE=$BACKUP_DIR/nssk_dump_"$TIMESTAMP".sql
DUMP_SYSTEM_FILE=$BACKUP_DIR/nssk_dump_system_"$TIMESTAMP".sql

echo "Dumping database tables to $DUMP_FILE"
mysqldump\
 -u $USER\
 -P "$PORT"\
 -h $HOST\
 --password="$CRED"\
 --all-databases > "$DUMP_FILE"

echo "Dumping database system state to $DUMP_SYSTEM_FILE"
mysqldump\
 -u $USER\
 -P "$PORT"\
 -h $HOST\
 --password="$CRED"\
 --system=all > "$DUMP_SYSTEM_FILE"