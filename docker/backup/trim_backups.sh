#!/bin/bash

MAX_DEL=20
MIN_KEEP=20

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
BACKUP_DIR="$SCRIPT_DIR"/../../backups

#nssk_dump_2024-03-17_000301.sql
#nssk_dump_2024-05-04_000501.sql
#...

#nssk_dump_system_2024-04-10_000401.sql
#nssk_dump_system_2024-04-11_000401.sql
#...



# trim backups if we're over our maximum
BACKUP_COUNT=$(/bin/ls -l "$BACKUP_DIR" | grep -c "^total")
if [ "$BACKUP_COUNT" -gt $MIN_KEEP ]; then
        /bin/ls -l "$BACKUP_DIR" | grep "backup_.*\.xml$" | head -$MAX_DEL | awk -v mydir="$BACKUP_DIR" '{print mydir"/"$NF}' | xargs rm -v
else
        echo "Skipping backup trim for backup count $BACKUP_COUNT in directory $BACKUP_DIR"
fi

exit 0;

