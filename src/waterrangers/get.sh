#!/bin/bash

DEFAULT_OFFSET=8
TARGET_DIR="."

make_request () {

  FILE=$1
  URL=$2
  OFFSET=$3

  if [ -z "$FILE" ]; then
    printf "\nMissing File"
    return
  fi

  if [ -z "$URL" ]; then
    printf "\nMissing URL"
    return
  fi

  if [ -z "$OFFSET" ]; then
    OFFSET=$DEFAULT_OFFSET
  fi

  # need to trim metadata from top of csv file. schema appears around line 8
  # schemas may not always be the same, or start on the same line
  #curl --retry -s https://app.waterrangers.ca/locations/14796.csv | tail +8 > WAG-E-01.csv

  echo -n "Retrieving $URL..."

  curl -s "$URL" | tail +"$OFFSET" > "$FILE"

  echo "Done"
}

make_request "$TARGET_DIR/MIS-M-01.csv" "https://app.waterrangers.ca/locations/14900.csv" 8
make_request "$TARGET_DIR/MIS-E-01.csv" "https://app.waterrangers.ca/locations/14898.csv" 8
make_request "$TARGET_DIR/MIS-W-01.csv" "https://app.waterrangers.ca/locations/14897.csv" 8

make_request "$TARGET_DIR/MOS-M-01.csv" "https://app.waterrangers.ca/locations/4912.csv" 8

make_request "$TARGET_DIR/WAG-E-01.csv" "https://app.waterrangers.ca/locations/14796.csv" 8
make_request "$TARGET_DIR/WAG-E-02.csv" "https://app.waterrangers.ca/locations/14797.csv" 8
make_request "$TARGET_DIR/WAG-E-03.csv" "https://app.waterrangers.ca/locations/14598.csv" 8
make_request "$TARGET_DIR/WAG-E-05.csv" "https://app.waterrangers.ca/locations/14829.csv" 8
make_request "$TARGET_DIR/WAG-E-06a.csv" "https://app.waterrangers.ca/locations/14863.csv" 10
make_request "$TARGET_DIR/WAG-E-06b.csv" "https://app.waterrangers.ca/locations/14865.csv" 8
make_request "$TARGET_DIR/WAG-E-07.csv" "https://app.waterrangers.ca/locations/14896.csv" 8

make_request "$TARGET_DIR/WAG-M-01.csv" "https://app.waterrangers.ca/locations/14899.csv" 8
make_request "$TARGET_DIR/WAG-M-02.csv" "https://app.waterrangers.ca/locations/15555.csv" 8
make_request "$TARGET_DIR/WAG-M-03.csv" "https://app.waterrangers.ca/locations/14901.csv" 8

make_request "$TARGET_DIR/WAG-W-02a.csv" "https://app.waterrangers.ca/locations/14862.csv" 8
make_request "$TARGET_DIR/WAG-W-02b.csv" "https://app.waterrangers.ca/locations/14864.csv" 8
make_request "$TARGET_DIR/WAG-W-03.csv" "https://app.waterrangers.ca/locations/14895.csv" 8
