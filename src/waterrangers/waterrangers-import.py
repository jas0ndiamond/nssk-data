import csv
import timeit
import argparse
import logging

# TODO need path_root modifier like in other importers?

from datetime import datetime
from WaterrangersDataEntry import WaterrangersDataEntry
from src.importer.DBImporter import DBImporter
from src.logger.LoggerFactory import LoggerFactory

# for testing validation failures
# import random
# from src.exception.DataValidationException import DataValidationException

################
# logging

logFile = "waterrangers.log"

# init logging outside of constructor so constructed objects can access
logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)

# custom log levels
logging.getLogger("WaterrangersDataEntry").setLevel(logging.INFO)
logging.getLogger("DBImporter").setLevel(logging.INFO)

###############

# Move to config file
sensors = {
    "WAG-E-01": "WAG_E_01",
    "WAG-E-02": "WAG_E_02",
    "WAG-E-03": "WAG_E_03",
    "WAG-E-05": "WAG_E_05",
    "WAG-E-06a": "WAG_E_06a",
    "WAG-E-06b": "WAG_E_06b",
    "WAG-E-07": "WAG_E_07",
    "WAG-M-01": "WAG_M_01",
    "WAG-M-02": "WAG_M_02",
    "WAG-M-03": "WAG_M_03",
    "MIS-M-01": "MIS_M_01",
    "MIS-E-01": "MIS_E_01",
    "MIS-W-01": "MIS_W_01",
    "WAG-W-02a": "WAG_W_02a",
    "WAG-W-02b": "WAG_W_02b",
    "WAG-W-03": "WAG_W_03",
    "MOS-M-01": "MOS_M_01"
}

# file csv file mapped to db column


# db_schema = [
#     "ObservedOn",
#     "ObservedBy",
#     "SampleId",
#     "Testers",
#     "HilsenhoffBioticIndex",
#     "Microfibers",
#     "Microbeads",
#     "Fragments",
#     "Film",
#     "Ph",
#     "Clarity",
#     "Nitrites",
#     "Conductivity",
#     "Alkalinity",
#     "Turbidity",
#     "Hardness",
#     "TotalKjeldahlNitrogen",
#     "TotalPhosphorus",
#     "OtherColiform",
#     "TotalColiform",
#     "ChlorophyllA",
#     "TotalPhosphorusBottom",
#     "TurbidityNtu",
#     "Calcium",
#     "BiochemicalOxygenDemand",
#     "TotalSuspendedSolids",
#     "IncubationTime",
#     "IncubationTemperature",
#     "OxygenPercent",
#     "Enterococci",
#     "WaterFlow",
#     "Chloride",
#     "RiverFlow",
#     "RiverStage",
#     "Fluorometer",
#     "Oxygen",
#     "WaterDepth",
#     "Chlorine",
#     "Nitrates",
#     "PhosphatesThreshold",
#     "TestStrips",
#     "WaterTemperature",
#     "AirTemperature",
#     "Salinity",
#     "Civ",
#     "Taxa",
#     "TotalDissolvedSolids",
#     "DissolvedOrganicCarbon",
#     "DissolvedOrganicCarbon2",
#     "Ammonia",
#     "Ecoli"
# ]

def want_row(in_row):
    # we're taking all measurements from dump files
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

def main(parsed_args):

    # handle parsed arguments
    #print(parsed_args)

    dry_run = False
    dry_run_param = getattr(parsed_args, "dry_run")

    data_dump_filename = None
    db_config_filename = None

    if getattr(parsed_args, "data_dump_file") is not None:
        data_dump_filename = getattr(parsed_args, "data_dump_file")[0]

    if getattr(parsed_args, "db_cfg_file") is not None:
        db_config_filename = getattr(parsed_args, "db_cfg_file")

    db_site = None
    if getattr(parsed_args, "site") is not None:
        db_site = getattr(parsed_args, "site")[0]

        if db_site not in sensors:
            print("Could not resolve table from site %s. Exiting..." % db_site)
            exit(1)
        else:
            print("Resolved table %s from site %s" % (sensors[db_site], db_site))

    if dry_run_param is not None and dry_run_param is True:
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

    log_msg = "Beginning import of Waterrangers data from data dump file %s to site %s" % (data_dump_filename, db_site)
    logger.info(log_msg)
    print(log_msg)

    #  If csvfile is a file object, it should be opened with newline=''
    # no quote char
    rows_processed = 0

    invalid_rows = []
    invalid_row_count = 0

    db_importer = DBImporter(db_config_filename)
    db_importer.set_importer_name("waterrangers")
    db_importer.set_schema(WaterrangersDataEntry.schema)
    db_importer.set_schema_mapping(WaterrangersDataEntry.schema_mapping)
    db_importer.set_commit_size(10000)

    csvread_start_time = timeit.default_timer()
    with open(data_dump_filename, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', strict=True)

        field_names = reader.fieldnames

        # schema
        logger.info("CSV file schema: %s" % field_names)
        logger.info("------------")

        print("Extracting data from CSV file...")

        # row is a CSV object
        for row in reader:

            # narrow our incoming data here
            # want only some rows
            if want_row(row):
                try:

                    # test random validation failures (1/1000 => ~.1% failure rate)
                    # if random.randint(0, 1000) == 20:
                    #     raise DataValidationException("Random validation failure")

                    data_entry = WaterrangersDataEntry(row)
                    data_entry.set_db_destination(sensors[db_site])

                    db_importer.add(data_entry)
                    rows_processed += 1
                except Exception as e:

                    # push object into collection
                    # log collection at end to file

                    logger.error("Error constructing WaterrangersDataEntry", e)

                    invalid_rows.append(row)
                    invalid_row_count += 1

                print("\r\tRows processed: %d. Validation failures: %d" %
                      (rows_processed, invalid_row_count), end='', flush=True)

                # use a subset when testing
                # if rows_processed > 1000:
                #     break
            else:
                # use sparingly
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("Rejecting row:\n%s\n---------", row)

    csvread_elapsed = (timeit.default_timer() - csvread_start_time)

    log_msg = ("\nProcessed %d rows from dump file %s in %.3f sec. Found %d validation failures" %
               (rows_processed, data_dump_filename, csvread_elapsed, invalid_row_count))
    print(log_msg, flush=True)
    logger.info(log_msg)

    ################
    # log read/parse failures here. not needed for database write
    if invalid_row_count > 0:
        date_time = datetime.now()
        invalid_row_file = "./waterrangers_invalid_rows_%s.log" % (date_time.strftime("%Y%m%d-%H%M%S"))

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

        print("Exiting...")

    logger.info("Exiting...")


##############################
if __name__ == "__main__":

    ############################
    # shell args
    #
    # --dry-run                             read data dump file and output sql statements.
    # -cfg conf.json                        database config     not required
    # WAG-E-01                              waterrangers site   required
    # WAG-E-01.csv                          data dump file      required
    ############################

    # reads sys.argv
    parser = argparse.ArgumentParser(description='Import data from a Waterrangers data dump into a configured database.')
    parser.add_argument('-q', '--quiet', action='store_true', dest='quiet',
                        help='Quiet mode. Limits ncurses status output and similar.')
    parser.add_argument('--dry-run', action='store_true', dest='dry_run',
                        help='Output database insert statements. Does not write to database.')
    parser.add_argument('-cfg', '--config-file', type=str, dest='db_cfg_file',
                        help='Database config file in json format. Ex: waterrangers.json')
    parser.add_argument(nargs=1, dest='site', help='Waterrangers site. Ex: WAG-E-01')
    parser.add_argument(nargs=1, dest='data_dump_file', type=str,
                        help='CoSMo data dump file. Ex: WAG-E-01.csv')

    # call main with parsed args
    main(parser.parse_args())
