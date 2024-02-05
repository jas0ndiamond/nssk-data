
import sys
import os
import logging
import json
import requests
from datastream_py import set_api_key, records
import pprint

#Rate limit yourself (2/sec)
#don't make requests in parallel.
#This will ensure you don't get 403 Unauthorized responses.

#API metadata
#https://github.com/datastreamapp/api-docs/tree/main/docs

# DatasetName,
# MonitoringLocationID,
# MonitoringLocationName,
# MonitoringLocationLatitude,
# MonitoringLocationLongitude,
# MonitoringLocationHorizontalCoordinateReferenceSystem,
# MonitoringLocationHorizontalAccuracyMeasure,
# MonitoringLocationHorizontalAccuracyUnit,
# MonitoringLocationVerticalMeasure,
# MonitoringLocationVerticalUnit,
# MonitoringLocationType,
# ActivityType,
# ActivityMediaName,
# ActivityStartDate,
# ActivityStartTime,
# ActivityEndDate,
# ActivityEndTime,
# ActivityDepthHeightMeasure,
# ActivityDepthHeightUnit,
# SampleCollectionEquipmentName,
# CharacteristicName,
# MethodSpeciation,
# ResultSampleFraction,
# ResultValue,
# ResultUnit,
# ResultValueType,
# ResultDetectionCondition,
# ResultDetectionQuantitationLimitMeasure,
# ResultDetectionQuantitationLimitUnit,
# ResultDetectionQuantitationLimitType,
# ResultStatusID,ResultComment,
# ResultAnalyticalMethodID,
# ResultAnalyticalMethodContext,
# ResultAnalyticalMethodName,
# AnalysisStartDate,
# AnalysisStartTime,
# AnalysisStartTimeZone,
# LaboratoryName,
# LaboratorySampleID

#and ActivityStartDate gt '2024-01-01' and ActivityStartDate lt '2024-01-31'
#MonitoringLocationID

#monitoringlocationids
# Wagg Creek
# WAGG01
# WAGG03

# Mosquito Creek
# MOSQ02
# MOSQ03
# MOSQ04
# MOSQ05

# Mission Creek
# MISS01

# Mackay Creek
# MACK02
# MACK03
# MACK04

# Hastings Creek
# HAST01
# HAST02
# HAST03

conf_file = open("./config.json")

key = json.load(conf_file)["api_key"]

conf_file.close()

doi = '10.25976/0gvo-9d12'

set_api_key(key)

pp = pprint.PrettyPrinter(indent=4)

maxResults = 20
resultWindow = 20



select_full = 'Id,DOI,ActivityType,ActivityMediaName,MonitoringLocationID,ActivityStartDate,ActivityStartTime,SampleCollectionEquipmentName,CharacteristicName,MethodSpeciation,ResultSampleFraction,ResultValue,ResultUnit,ResultValueType'
select_test = 'Id,DOI,MonitoringLocationID,ActivityStartDate,ActivityStartTime,ResultValue,ResultUnit,ResultValueType'

locationId = 'MOSQ02'

#works, adds "Id" field
# results = records({
#     '$select': select_test,
#     '$filter': "DOI eq '%s'" % doi,
#     '$top': resultWindow
# })

# 400 bad request
# results = records({
#     '$select': select_test,
#     '$filter': "DOI eq '%s' and MonitoringLocationID eq '%s'" % (doi, locationId),
#     '$top': resultWindow
# })


# 400 bad request
# results = records({
#     '$select': select_test,
#     '$filter': "ActivityStartDate gt '%s'" % "2024-01-01",
#     '$top': resultWindow
# })

# 400 bad request
results = records({
    '$select': select_test,
    '$filter': "MonitoringLocationID eq '%s'" % locationId,
    '$top': resultWindow
})

iResult = 0
for record in results:

    if(iResult >= maxResults):
        print("Target result count retrieved. Bailing")
        break

    record_str = pp.pformat(record)
    print("===================\n%s\n" % record_str)

    iResult += 1