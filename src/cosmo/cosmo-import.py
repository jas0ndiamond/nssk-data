import csv
import sys
import timeit

from datetime import datetime

from CosmoDataEntry import CosmoDataEntry
from src.exception.DataValidationException import DataValidationException
from src.importer.DBImporter import DBImporter

import logging
import random

################
# logging

logFile = "cosmo.log"

# init logging outside of constructor so constructed objects can access
logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)

# custom log levels
logging.getLogger("CosmoDataEntry").setLevel(logging.INFO)
logging.getLogger("DBImporter").setLevel(logging.INFO)

###############


# TODO: move to config file or shell param
dump_filename = "/home/jason/Pub/nssk-data-dumps/doi.org_10.25976_0gvo-9d12.csv"

# TODO: shell param for main config file
# TODO: not just db config
DB_CONFIG_FILE = "../../conf/cosmo.json"

# Wagg Creek
# WAGG01
# WAGG02
# WAGG03

# Mosquito Creek
# MOSQ01
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
# MACK05

# Hastings Creek
# HAST01
# HAST02
# HAST03

# Move to config file
dataset_name_field = "DatasetName"
cosmo_dataset_name = 'DFO PSEC Community Stream Monitoring (CoSMo)'
monitoring_location_id_field = "MonitoringLocationID"
sensors = {
    "WAGG01",
    "WAGG02",
    "WAGG03",
    "MOSQ01",
    "MOSQ02",
    "MOSQ03",
    "MOSQ04",
    "MOSQ05",
    "MOSQ06",
    "MOSQ07",
    "MISS01",
    "MACK02",
    "MACK03",
    "MACK04",
    "MACK05",
    "HAST01",
    "HAST02",
    "HAST03"
}

cosmo_schema = [
    "DatasetName",
    "MonitoringLocationID",
    "MonitoringLocationName",
    "MonitoringLocationLatitude",
    "MonitoringLocationLongitude",
    "MonitoringLocationHorizontalCoordinateReferenceSystem",
    "MonitoringLocationHorizontalAccuracyMeasure",
    "MonitoringLocationHorizontalAccuracyUnit",
    "MonitoringLocationVerticalMeasure",
    "MonitoringLocationVerticalUnit",
    "MonitoringLocationType",
    "ActivityType",
    "ActivityMediaName",
    "ActivityStartDate",
    "ActivityStartTime",
    "ActivityEndDate",
    "ActivityEndTime",
    "ActivityDepthHeightMeasure",
    "ActivityDepthHeightUnit",
    "SampleCollectionEquipmentName",
    "CharacteristicName",
    "MethodSpeciation",
    "ResultSampleFraction",
    "ResultValue",
    "ResultUnit",
    "ResultValueType",
    "ResultDetectionCondition",
    "ResultDetectionQuantitationLimitMeasure",
    "ResultDetectionQuantitationLimitUnit",
    "ResultDetectionQuantitationLimitType",
    "ResultStatusID",
    "ResultComment",
    "ResultAnalyticalMethodID",
    "ResultAnalyticalMethodContext",
    "ResultAnalyticalMethodName",
    "AnalysisStartDate",
    "AnalysisStartTime",
    "AnalysisStartTimeZone",
    "LaboratoryName",
    "LaboratorySampleID",
]


def want_row(in_row):
    # if a row in the data dump is on our shortlist of sensors, we want it
    return (
            in_row[monitoring_location_id_field] in sensors and
            in_row[dataset_name_field] == cosmo_dataset_name
    )


###############################

# open csv file
# get a handle on schema
# parse each line, checking for errors
# if not on guest list of sensor names, skip
# create new data object
# add data object to processing queue
# close csv file

# for each data object in processing queue
# check if any uniqueness is observed (ids, timestamps, etc)
# determine destination mysql table
# create mysql insert statement from object
# add to insert list

# open db connection
# for each entry in insert list, execute
# close db connection

# print report

##############################################

def main(args):
    dry_run = False

    ############################
    # shell args
    if len(args) == 2:
        if args[1] == "-h" or args[1] == "-help" or args[1] == "--help":
            print(
                "Usage: python3 cosmo-import.py [--dry-run]\n"
                "\t--dry-run: output database insert statements. Does not write to database.")
            exit(1)
        elif args[1] == "--dry-run":
            logger.info("Executing dry run")
            dry_run = True
    else:
        logger.info("Executing import")

    ############################

    logger.info("Beginning import of CoSMo data")
    print("Beginning import of CoSMo data")

    #  If csvfile is a file object, it should be opened with newline=''
    # no quote char
    rows_processed = 0

    invalid_rows = []
    invalid_row_count = 0

    db_importer = DBImporter(DB_CONFIG_FILE)
    db_importer.set_importer_name("cosmo")
    db_importer.set_schema(cosmo_schema)

    csvread_start_time = timeit.default_timer()
    with open(dump_filename, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', strict=True)

        field_names = reader.fieldnames

        # schema
        logger.info("CSV file schema: %s" % field_names)
        logger.info("------------")

        print("Extracting data from CSV file...")

        # row is a CSV object
        for row in reader:

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("MonitoringLocationID: %s" % row[monitoring_location_id_field])

            # narrow our incoming data here
            # want only some rows
            if want_row(row):
                try:

                    # test random validation failures (1/1000 => .1% failure rate)
                    # if random.randint(0, 1000) == 20:
                    #     raise DataValidationException("Random validation failure")

                    db_importer.add(CosmoDataEntry(row))
                    rows_processed += 1
                except Exception as e:

                    # push object into collection
                    # log collection at end to file

                    logger.error("Error constructing CosmoDataEntry")
                    logger.error(e)

                    invalid_rows.append(row)
                    invalid_row_count += 1

                print("\r\tRows processed: %d. Validation failures: %d" %
                      (rows_processed, invalid_row_count), end='', flush=True)
            else:
                # use sparingly
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("Rejecting row:\n%s\n---------", row)

    csvread_elapsed = (timeit.default_timer() - csvread_start_time)

    log_msg = ("\nProcessed %d rows from dump file %s in %.3f sec. Found %d validation failures" %
               (rows_processed, dump_filename, csvread_elapsed, invalid_row_count))
    print(log_msg, flush=True)
    logger.info(log_msg)

    ################
    # log read/parse failures here. not needed for database write
    if invalid_row_count > 0:
        date_time = datetime.now()
        invalid_row_file = "./cosmo_invalid_rows_%s.log" % (date_time.strftime("%Y%m%d-%H%M%S"))

        log_msg = "Found %d invalid rows. Logging to file '%s'" % (invalid_row_count, invalid_row_file)

        print(log_msg)
        logger.info(log_msg)

        with open(invalid_row_file, 'w', encoding='utf-8') as filehandle:
            for invalid_row in invalid_rows:
                filehandle.write("%s\n" % invalid_row)
    else:
        log_msg = "Found no rejected rows."

        print(log_msg)
        logger.info(log_msg)

    ################
    # write to database

    if dry_run:
        db_importer.dump()
    else:
        dbimport_start_time = timeit.default_timer()
        db_importer.execute()
        dbimport_elapsed = timeit.default_timer() - dbimport_start_time

        log_msg = "Completed database import in %.3f sec" % dbimport_elapsed
        print(log_msg, flush=True)
        logger.info(log_msg)

    logger.info("Exiting...")


##############################
if __name__ == "__main__":
    main(sys.argv)
