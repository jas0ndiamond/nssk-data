import pprint
import sys
import timeit
import json

from FlowworksDataEntry import FlowworksDataEntry
from src.importer.DBImporter import DBImporter

import logging

################
# logging

logFile = "flowworks.log"

# init logging outside of constructor so constructed objects can access
logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)

# custom log levels
logging.getLogger("FlowworksDataEntry").setLevel(logging.INFO)
logging.getLogger("DBImporter").setLevel(logging.INFO)

###############


# TODO: move to config file or shell param
filename = "/home/jason/Pub/nssk-data-dumps/flowworks.json"

# TODO: shell param for main config file
# TODO: not just db config
DB_CONFIG_FILE = "../../conf/flowworks.json"

# schema in the data dump file
# yyyy/MM/dd HH:mm:ss
# Air Temperature - 5 min Intervals (°C)
# Barometer 5 min Intervals (mbar)
# Hourly Rainfall (mm)
# Rainfall (mm)
# --- order matters
flowworks_dump_schema = [
    "date",
    "value"
]

# schema expected by the database
# csv schema has odd characters that mysql probably won't like
# --- order matters
flowworks_db_schema = [
    "MeasurementTimestamp",
    "FlowReading"
]

# map the schemas
# where to put this?
# relevant only to database?
schema_field_mapping = {
    "date": "MeasurementTimestamp",
    "value": "FlowReading"
}


def want_entry(in_row):
    # only one site for now: accept it all
    return True


###############################

# open json file
# get a handle on schema
# parse each object, checking for errors
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
                "Usage: python3 flowworks-importer.py [--dry-run]\n"
                "\t--dry-run: output database insert statements. Does not write to database.")
            exit(1)
        elif args[1] == "--dry-run":
            logger.info("Executing dry run")
            dry_run = True
    else:
        logger.info("Executing import")

    ############################

    logger.info("Beginning import of Flowworks data")
    print("Beginning import of Flowworks data")

    #  If csvfile is a file object, it should be opened with newline=''
    # no quote char
    entries_processed = 0

    db_importer = DBImporter(DB_CONFIG_FILE)
    db_importer.set_importer_name("flowworks")
    db_importer.set_schema(flowworks_dump_schema)
    db_importer.set_schema_mapping(schema_field_mapping)

    # TODO: move to ijson. json.loads will load the entire file into memory. stupid.
    print("Extracting data from JSON file...")
    json_data = open(filename).read()
    dump_data = json.loads(json_data)

    json_read_start_time = timeit.default_timer()

    for datapoint in dump_data["datapoints"]:

        if want_entry(datapoint):
            db_importer.add(FlowworksDataEntry(datapoint))
            entries_processed += 1

            print("\r\tEntries processed: %d" % entries_processed, end='', flush=True)
        else:
            # use sparingly
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Rejecting entry:\n%s\n---------", datapoint)

    json_read_elapsed = (timeit.default_timer() - json_read_start_time)

    log_msg = "\nProcessed %d rows from csv file in %.3f sec" % (entries_processed, json_read_elapsed)
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
