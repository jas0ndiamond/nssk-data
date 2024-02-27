import csv
import sys
import timeit

from CNVRainfallDataEntry import CNVRainfallDataEntry
from src.importer.DBImporter import DBImporter

import logging

################
# logging

logFile = "cnv-rainfall.log"

# init logging outside of constructor so constructed objects can access
logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)

# custom log levels
logging.getLogger("CNVRainfallDataEntry").setLevel(logging.INFO)
logging.getLogger("DBImporter").setLevel(logging.INFO)

###############


# TODO: move to config file or shell param
filename = "/home/jason/Pub/nssk-data-dumps/NorthVancouverCityHall_export_20240215142617.csv"

# TODO: shell param for main config file
# TODO: not just db config
DB_CONFIG_FILE = "../../conf/cnv-rainfall.json"

# schema in the data dump file
# yyyy/MM/dd HH:mm:ss
# Air Temperature - 5 min Intervals (°C)
# Barometer 5 min Intervals (mbar)
# Hourly Rainfall (mm)
# Rainfall (mm)
# --- order matters
cnv_rainfall_dump_schema = [
    "yyyy/MM/dd HH:mm:ss",
    "Air Temperature - 5 min Intervals (°C)",
    "Barometer 5 min Intervals (mbar)",
    "Hourly Rainfall (mm)",
    "Rainfall (mm)"
]

# schema expected by the database
# csv schema has odd characters that mysql probably won't like
# --- order matters
cnv_rainfall_db_schema = [
    "MeasurementTimestamp",
    "AirTemperature",
    "BarometricPressure",
    "HourlyRainfall",
    "Rainfall"
]

# map the schemas
# where to put this?
# relevant only to database?
schema_field_mapping = {
    "yyyy/MM/dd HH:mm:ss": "MeasurementTimestamp",
    "Air Temperature - 5 min Intervals (°C)": "AirTemperature",
    "Barometer 5 min Intervals (mbar)": "BarometricPressure",
    "Hourly Rainfall (mm)": "HourlyRainfall",
    "Rainfall (mm)": "Rainfall"
}

def want_row(in_row):
    # only one site for now: accept it all
    return True


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
                "Usage: python3 cnv-rainfall-import.py [--dry-run]\n"
                "\t--dry-run: output database insert statements. Does not write to database.")
            exit(1)
        elif args[1] == "--dry-run":
            logger.info("Executing dry run")
            dry_run = True
    else:
        logger.info("Executing import")

    ############################

    logger.info("Beginning import of CNV Rainfall data")
    print("Beginning import of CNV Rainfall data")

    #  If csvfile is a file object, it should be opened with newline=''
    # no quote char
    rows_processed = 0

    db_importer = DBImporter(DB_CONFIG_FILE)
    db_importer.set_importer_name("cnv-rainfall")
    db_importer.set_schema(cnv_rainfall_dump_schema)
    db_importer.set_schema_mapping(schema_field_mapping)

    csvread_start_time = timeit.default_timer()
    with open(filename, newline='', encoding='utf-8') as csvfile:

        # data dump file has two metadata lines above the schema
        next(csvfile)
        next(csvfile)

        reader = csv.DictReader(csvfile, delimiter=',', strict=True)

        field_names = reader.fieldnames

        logger.info("CSV file schema:")
        # schema
        for field in field_names:
            logger.info("\t%s" % field)

        logger.info("------------")

        print("Extracting data from CSV file...")

        for row in reader:

            # narrow our incoming data here
            # want only some rows
            if want_row(row):
                db_importer.add(CNVRainfallDataEntry(row))
                rows_processed += 1

                print("\r\tRows processed: %d" % rows_processed, end='', flush=True)
            else:
                # use sparingly
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("Rejecting row:\n%s\n---------", row)

    csvread_elapsed = (timeit.default_timer() - csvread_start_time)

    log_msg = "\nProcessed %d rows from csv file in %.3f sec" % (rows_processed, csvread_elapsed)
    print(log_msg, flush=True)
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
