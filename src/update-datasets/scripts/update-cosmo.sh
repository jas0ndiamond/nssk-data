#!/bin/bash

# retrieve CoSMo data from DFO PSEC Monitoring
# https://datastream.org/en-ca/dataset/4c8d3691-99e5-4fa9-ad09-da077baa37c5

OUTPUT_FILE=$1
CURL_BANDWIDTH_LIMIT="10m"

if [ -z "$OUTPUT_FILE" ] || [ "$OUTPUT_FILE" == "-h" ] || [ "$OUTPUT_FILE" == "-help" ] || [ "$OUTPUT_FILE" == "--help" ]; then
  echo "Usage: ./update-cosmo.sh OUTPUT_FILE"
  echo -e "\tOUTPUT_FILE\t The path to the file to write the data dump."
  exit 1
fi

touch "$OUTPUT_FILE"
if [ ! -f "$OUTPUT_FILE" ]; then
  echo "Could not create OUTPUT_FILE"
  exit 1
fi


OUTPUT_FILE_PATH=$(realpath "$OUTPUT_FILE")

if [ ! -f "$OUTPUT_FILE_PATH" ]; then
  echo "Could not resolve realpath for OUTPUT_FILE"
  exit 1
fi

if [ ! -d $(dirname "$OUTPUT_FILE_PATH") ]; then
  echo "Could not resolve dirname for OUTPUT_FILE"
  exit 1
fi

DATASET_ID="4c8d3691-99e5-4fa9-ad09-da077baa37c5"
DATASET_NAME="doi.org_10.25976_0gvo-9d12"

URL="https://datastream.org/api/dataset/$DATASET_ID/latest/download?name=$DATASET_NAME"

# handle return code of pipes below. return code of script should be return code of first failed command or
# the success code of the last command where all commands succeeded
set -o pipefail

# retrieve the dataset
CMD="curl --limit-rate \"$CURL_BANDWIDTH_LIMIT\" -s \"$URL\" | jq '.data.url' | xargs curl  --limit-rate \"$CURL_BANDWIDTH_LIMIT\" -s -o \"$OUTPUT_FILE\""

#echo "$CMD"

eval "$CMD"

# explicitly return exit code of our retrieval command. 0 is success.
exit $?
