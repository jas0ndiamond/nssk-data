import csv
import argparse
import logging
import timeit

from datetime import datetime
from CNVRainfallDataEntry import CNVRainfallDataEntry
from src.importer.DBImporter import DBImporter

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

    log_msg = "Beginning import of CNV Rainfall data from data dump file %s" % data_dump_filename
    logger.info(log_msg)
    print(log_msg)

    #  If csvfile is a file object, it should be opened with newline=''
    # no quote char
    rows_processed = 0

    invalid_rows = []
    invalid_row_count = 0

    db_importer = DBImporter(db_config_filename)
    db_importer.set_importer_name("cnv-rainfall")
    db_importer.set_schema(cnv_rainfall_dump_schema)
    db_importer.set_schema_mapping(schema_field_mapping)

    csvread_start_time = timeit.default_timer()
    with open(data_dump_filename, newline='', encoding='utf-8') as csvfile:

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

        # row is a CSV object
        for row in reader:

            # narrow our incoming data here
            # want only some rows
            if want_row(row):
                try:

                    # test random validation failures (1/1000 => ~.1% failure rate)
                    # if random.randint(0, 1000) == 20:
                    #     raise DataValidationException("Random validation failure")

                    db_importer.add(CNVRainfallDataEntry(row))
                    rows_processed += 1
                except Exception as e:

                    # push object into collection
                    # log collection at end to file

                    logger.error("Error constructing CNVRainfallDataEntry")
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
               (rows_processed, data_dump_filename, csvread_elapsed, invalid_row_count))
    print(log_msg, flush=True)
    logger.info(log_msg)

    ################
    # log read/parse failures here. not needed for database write
    if invalid_row_count > 0:
        date_time = datetime.now()
        invalid_row_file = "./cnv-rainfall_invalid_rows_%s.log" % (date_time.strftime("%Y%m%d-%H%M%S"))

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
    ############################
    # shell args
    #
    # --dry-run                                              read data dump file and output sql statements.
    # -cfg cnv-rainfall.json                                 database config     not required
    # NorthVancouverCityHall_export_20240328073312.csv       data dump file      required
    ############################

    # reads sys.argv
    parser = argparse.ArgumentParser(
                        description='Import data from a CNV Rainfall data dump into a configured database.')
    parser.add_argument('--dry-run', action='store_const', const=1, dest='dryrun',
                        help='Output database insert statements. Does not write to database.')
    parser.add_argument('-cfg', nargs=1, dest='db_cfg_file',
                        help='Database config file in json format. Ex: cnv-rainfall.json')
    parser.add_argument(nargs=1, dest='data_dump_file',
                        help='CNV Rainfall data dump file. Ex: NorthVancouverCityHall_export_20240328073312.csv')

    # call main with parsed args
    main(parser.parse_args())
