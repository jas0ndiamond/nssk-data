#!/bin/bash

# pull a single site/channel for the api key
# rename the "value" field to the supplied key

# api trims token it receives to an expected length
TOKEN=$1
SITE=$2
CHANNEL=$3
UPDATE_FLAG=$4

RUN_UPDATE=0

if [ -z "$TOKEN" ]; then
  echo "Token required"
  exit 1;
fi

if [ -z "$SITE" ]; then
  echo "Site required"
  exit 1;
fi

if [ -z "$CHANNEL" ]; then
  echo "Channel required"
  exit 1;
fi

if [ -n "$UPDATE_FLAG" ] && [ "$UPDATE_FLAG" == "-u" ]; then
  RUN_UPDATE=1
fi

# cnv flowworks account uses v2 of api with 'sites' and 'channels' uri parameters
API_VERSION="v2"
CURL_BANDWIDTH_LIMIT="1m"

######################################

# retrieve dump from api
# v2 sites and channels
# v1 site and channel
URL="https://developers.flowworks.com/fwapi/$API_VERSION/$API_KEY/sites/$SITE/channels/$CHANNEL/data"

if [ $RUN_UPDATE -eq 1 ]; then
#  echo "Updating"

  # 1 year
#  TIME_BUCKET="Y"
#  DURATION=1
#
#  # 6 months
#  TIME_BUCKET="M"
#  DURATION=6

  # 3 months
  TIME_BUCKET="M"
  DURATION=3

  UPDATE_SEGMENT="intervalTypeFilter=$TIME_BUCKET&intervalNumberFilter=$DURATION"
  URL="$URL?$UPDATE_SEGMENT"
fi

CMD="curl -s\
 --limit-rate \"$CURL_BANDWIDTH_LIMIT\"\
 -X GET\
 -H \"Content-Type: application/json\"\
 -H \"Authorization: Bearer $TOKEN\"\
 \"$URL\" "

#echo "$CMD"

#{
#  "Resources": [
#    {
#      "DataValue": "",
#      "DataTime": "2017-05-25T12:00:00"
#    },
#    {
#      "DataValue": "",
#      "DataTime": "2017-05-25T12:05:00"
#    },
#    ...
#   ],
#  "ResultCode": 0,
#  "ResultMessage": "Request OK – Request is valid and was accepted."
#}

eval "$CMD"

# maybe leave the response inspection to the invoker
#read -r REQ_REQ_STATUS <<< $(jq -r '.requeststatus' "$TEMP_FILE")
#if [ "$REQ_REQ_STATUS" -ne "1" ]; then
#  echo "Request status failed check: $REQ_REQ_STATUS"
#  exit 1
#fi
#
#read -r REQ_MSG <<< $(jq -r '.msg' "$TEMP_FILE")
#if [ "$REQ_MSG" != "Request OK." ]; then
#  echo "Request message failed check: $REQ_MSG"
#  exit 1
#fi
#
#read -r REQ_USER <<< $(jq -r '.username' "$TEMP_FILE")
#if [ "$REQ_USER" != "cnvrain" ]; then
#  echo "Request username failed check: $REQ_USER"
#  exit 1
#fi