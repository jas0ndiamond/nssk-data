#!/bin/bash

# should work for both cnv-rainfall and dnv-whitewater, but will be api v1 or v2 dependent

# FIELD_NAME should be a database field

# pull a single site/channel for the api key
# rename the "value" field to the supplied key

# site 40 District of North Vancouver Rain Gauge
# channels
# 151 Published Rainfall
# 721 Rainfall
# 686 Raw Rainfall
# 228 Rolling 1 hr Rain
# 554 Rolling 24 hr Rainfall
# 481 Temperature
# 687 Temperature_

# site 183 Lynn Creek
# channels
# 141 Data Notes
# 293 Published Discharge
# 294 Published Stage
# 674 Raw Discharge
# 666 Raw Stage
# 166 Upper Discharge Confidence Limit

# site 309 Seymour River
# channels
# 144 Data Notes
# 204 Published Discharge
# 203 Published Stage
# 669 Raw Stage
# 179 Upper Discharge Confidence Limit

API_KEY=$1
SITE=$2
CHANNEL=$3
UPDATE_FLAG=$4

RUN_UPDATE=0

if [ -z "$API_KEY" ]; then
  echo "API Key required"
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

# dnv flowworks account uses v1 of api with 'sites' and 'channels' api resources
# v1 api works with or without an interval
API_VERSION="v1"
CURL_BANDWIDTH_LIMIT="1m"

######################################
# retrieve dump from api
URL="https://developers.flowworks.com/fwapi/$API_VERSION/$API_KEY/sites/$SITE/channel/$CHANNEL/data"


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

  UPDATE_SEGMENT="intervaltype/$TIME_BUCKET/intervalnum/$DURATION"
  URL="$URL/$UPDATE_SEGMENT"
fi

CMD="curl -s --limit-rate \"$CURL_BANDWIDTH_LIMIT\" $URL"

# debug
# echo "$CMD"

eval "$CMD"

#{
#  "requeststatus": 1,
#  "msg": "Request OK.",
#  "username": "DNV_van_whitewater",
#  "datapoints": [
#    {
#      "date": "2024-11-03T21:55:00",
#      "value": 7.684527
#    },
#    ...
#    ]
#  }

###############
# maybe leave the response inspection to the invoker
# inspect the json of the curl request to make sure we got something coherent. return accordingly
#  "requeststatus": 1,
#  "msg": "Request OK.",
#  "username": "DNV_van_whitewater",
