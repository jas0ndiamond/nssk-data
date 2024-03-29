import json
import argparse
import logging
import timeit

from datetime import datetime
from FlowworksDataEntry import FlowworksDataEntry
from src.importer.DBImporter import DBImporter

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

# data dump file
# "datapoints":[{"date":"2021-03-29T14:00:00","value":11.46656},...]
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

def main(parsed_args):

    # handle parsed arguments

    dry_run = False
    data_dump_filename = None
    db_config_filename = None

    if getattr(parsed_args, "data_dump_file") is not None:
        data_dump_filename = getattr(parsed_args, "data_dump_file")[0]

    if getattr(parsed_args, "db_cfg_file") is not None:
        db_config_filename = getattr(parsed_args, "db_cfg_file")[0]

    if getattr(parsed_args, "dryrun") is not None:
        # dry run - don't need a db config file since there's no db interaction
        log_msg = "Executing dry run"
        logger.info(log_msg)
        print(log_msg)
        dry_run = True
    else:
        # data import - make sure we have a config file to connect to the database
        if db_config_filename is None:
            print("Error- Need a DB config file for the import")
            exit(1)

        log_msg = "Executing import"
        logger.info(log_msg)
        print(log_msg)

    ############################

    # read the dump file

    log_msg = "Beginning import of Flowworks data from data dump file %s" % data_dump_filename
    logger.info(log_msg)
    print(log_msg)

    #  If csvfile is a file object, it should be opened with newline=''
    # no quote char
    entries_processed = 0

    invalid_entry = []
    invalid_entry_count = 0

    db_importer = DBImporter(db_config_filename)
    db_importer.set_importer_name("flowworks")
    db_importer.set_schema(flowworks_dump_schema)
    db_importer.set_schema_mapping(schema_field_mapping)

    # TODO: move to ijson. json.loads will load the entire file into memory. stupid.
    print("Extracting data from JSON file...")
    json_data = open(data_dump_filename).read()
    dump_data = json.loads(json_data)

    json_read_start_time = timeit.default_timer()

    for entry in dump_data["datapoints"]:

        # narrow our incoming data here
        # want only some rows
        if want_entry(entry):
            try:

                # test random validation failures (1/1000 => ~.1% failure rate)
                # if random.randint(0, 1000) == 20:
                #     raise DataValidationException("Random validation failure")

                db_importer.add(FlowworksDataEntry(entry))
                entries_processed += 1
            except Exception as e:

                # push object into collection
                # log collection at end to file

                logger.error("Error constructing FlowworksDataEntry")
                logger.error(e)

                invalid_entry.append(entry)
                invalid_entry_count += 1

            print("\r\tEntries processed: %d. Validation failures: %d" %
                  (entries_processed, invalid_entry_count), end='', flush=True)
        else:
            # use sparingly
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Rejecting row:\n%s\n---------", entry)

    json_read_elapsed = (timeit.default_timer() - json_read_start_time)

    log_msg = ("\nProcessed %d rows from dump file %s in %.3f sec. Found %d validation failures" %
               (entries_processed, data_dump_filename, json_read_elapsed, invalid_entry_count))
    print(log_msg, flush=True)
    logger.info(log_msg)

    ################
    # log read/parse failures here. not needed for database write
    if invalid_entry_count > 0:
        date_time = datetime.now()
        invalid_entry_file = "./flowworks_invalid_entries_%s.log" % (date_time.strftime("%Y%m%d-%H%M%S"))

        log_msg = "Found %d invalid entries. Logging to file '%s'" % (invalid_entry_count, invalid_entry_file)

        print(log_msg)
        logger.info(log_msg)

        with open(invalid_entry_file, 'w', encoding='utf-8') as filehandle:
            for invalid_entry in invalid_entry:
                filehandle.write("%s\n" % invalid_entry)
    else:
        log_msg = "Found no rejected entries."

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
    ############################
    # shell args
    #
    # --dry-run                                           read data dump file and output sql statements.
    # -cfg flowworks.json                                 database config     not required
    # flowworks.json                                      data dump file      required
    ############################

    # reads sys.argv
    parser = argparse.ArgumentParser(
                        description='Import data from a Flowworks data dump into a configured database.')
    parser.add_argument('--dry-run', action='store_const', const=1, dest='dryrun',
                        help='Output database insert statements. Does not write to database.')
    parser.add_argument('-cfg', nargs=1, dest='db_cfg_file',
                        help='Database config file in json format. Ex: flowworks.json')
    parser.add_argument(nargs=1, dest='data_dump_file',
                        help='Flowworks data dump file. Ex: 20240329-145659_flowworks.json')

    # call main with parsed args
    main(parser.parse_args())
