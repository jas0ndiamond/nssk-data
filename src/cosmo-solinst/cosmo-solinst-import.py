import csv
import sys
import timeit
import argparse
import logging

from datetime import datetime
from pathlib import Path
from string import Template

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

from src.cosmo.CosmoDataEntry import CosmoDataEntry
from src.exception.DataValidationException import DataValidationException
from src.importer.DBImporter import DBImporter

################
# logging

TRACE_LOGGING = False

logFile = "cosmo-solinst.log"

# init logging outside of constructor so constructed objects can access
logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)

# custom log levels
logging.getLogger("CosmoDataEntry").setLevel(logging.INFO)
logging.getLogger("DBImporter").setLevel(logging.INFO)

###############

COSMO_TEMPLATES = {
    "WAGG01": "cosmo-entry.wagg01.csv.template",
    "WAGG03": "cosmo-entry.wagg03.csv.template"
}

# Move to config file
dataset_name_field = "DatasetName"
cosmo_dataset_name = 'DFO PSEC Community Stream Monitoring (CoSMo)'
monitoring_location_id_field = "MonitoringLocationID"

# date in YYYY-MM-DD
# time in 12h
# ms scalar
# LEVEL in m    => same as cosmo
# TEMPERATURE in C  => same as cosmo
# CONDUCTIVITY in uS/cm     => same as cosmo
SOLINST_SCHEMA = [
    "Date",
    "Time",
    "ms",
    "LEVEL",
    "TEMPERATURE",
    "CONDUCTIVITY"
]

# CoSMo measurement types we're interested in and respective units
# conductivity measurements use same units
COSMO_CHARACTERISTIC_NAME_CONDUCTIVITY = "Conductivity"
COSMO_RESULT_UNIT_CONDUCTIVITY = "uS/cm"

COSMO_CHARACTERISTIC_NAME_SPECIFIC_CONDUCTANCE = "Specific Conductance"

COSMO_CHARACTERISTIC_NAME_TEMPERATURE_WATER = "Temperature water"
COSMO_RESULT_UNIT_TEMPERATURE_WATER = "deg C"

COSMO_CHARACTERISTIC_NAME_WATER_LEVEL = "Water level (probe)"
COSMO_RESULT_UNIT_WATER_LEVEL = "m"

COSMO_RESULT_VALUE_TYPE = "NSSK Supplied Actual"

COSMO_RESULT_COMMENT = "Preliminary QC and Specific Conductance by NSSK"

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
    "ActivityStartTimeZone",
    "ActivityEndDate",
    "ActivityEndTime",
    "ActivityEndTimeZone",
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
    print(parsed_args)

    dry_run = False
    data_dump_filename = None
    db_config_filename = None
    site = None

    if getattr(parsed_args, "data_dump_file") is not None:
        data_dump_filename = getattr(parsed_args, "data_dump_file")[0]

    if getattr(parsed_args, "site") is not None:
        site = getattr(parsed_args, "site")[0]

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

        if site is None:
            print("Error- Need a CoSMo site for the import")
            exit(1)

        log_msg = "Executing import"
        logger.info(log_msg)
        print(log_msg)

    ############################

    # load entry template for cosmo site
    if site in COSMO_TEMPLATES:
        template_file = COSMO_TEMPLATES[site]
        logger.info("Loading template %s for site %s" % (template_file, site))
        data_entry_template = Template(open("template/%s" % template_file).read())
    else:
        print("Error- Unknown CoSMo site: %s" % site)
        exit(1)

    # read the dump file
    log_msg = "Beginning import of Solinst data from data dump file %s" % data_dump_filename
    logger.info(log_msg)
    print(log_msg)

    #  If csvfile is a file object, it should be opened with newline=''
    # no quote char
    rows_processed = 0

    invalid_rows = []
    invalid_row_count = 0

    db_importer = DBImporter(db_config_filename)
    db_importer.set_importer_name("cosmo-solinst")
    db_importer.set_schema(cosmo_schema)
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

            # a row contains multiple measurements, which are used to compute an additional measurement
            # a single row will map to multiple CosmoDataEntrys and subsequent database inserts
            # one invalid measurement does not invalidate the whole row

            try:
                # local vars declared here since they're used in multiple measurements
                has_valid_temperature = False
                has_valid_conductance = False
                entry_conductivity = None
                entry_temperature = None

                # an invalid measurement should not invalidate the whole row

                # fields used in all measurements
                # YYYY-MM-DD
                entry_date = row["Date"]

                # HH:MM:SS
                # 12h format
                # hour field isn't always zero-padded
                entry_time_12h = row["Time"]
                # time_ms = row[2] # not used

                # TODO check validity of date and time strings

                # convert time to 24h
                entry_time_12h_datetime = datetime.strptime(entry_time_12h, "%I:%M:%S %p")
                entry_time_24h = datetime.strftime(entry_time_12h_datetime, "%H:%M:%S")

                if TRACE_LOGGING:
                    logger.debug("Converting 12h datetime to 24h. %s => %s" % (entry_time_12h_datetime, entry_time_24h))

                #############
                # entry for water level
                if row["LEVEL"] is not None and row["LEVEL"]:
                    entry_water_level = float(row["LEVEL"])

                    # create a csv string with the values substituted
                    data_entry_csv_str = data_entry_template.substitute(
                        DATE=entry_date,
                        TIME=entry_time_24h,
                        CHARACTERISTIC_NAME=COSMO_CHARACTERISTIC_NAME_WATER_LEVEL,
                        RESULT_UNIT=COSMO_RESULT_UNIT_WATER_LEVEL,
                        RESULT_VALUE="%.4f" % entry_water_level,
                        RESULT_VALUE_TYPE=COSMO_RESULT_VALUE_TYPE,
                        RESULT_COMMENT=COSMO_RESULT_COMMENT
                    )

                    logger.debug("data_entry_csv_str: %s" % data_entry_csv_str)

                    # reparse the csv string into a parsed row
                    # add in the original headers so DictReader can build a dict for the CosmoDataEntry
                    data_entry_row = next(csv.DictReader(data_entry_csv_str.split("\n"), delimiter=',', strict=True))

                    logger.debug("data_entry_row: %s" % data_entry_row)

                    if data_entry_row is not None and data_entry_row:
                        try:
                            db_importer.add(CosmoDataEntry(data_entry_row))
                            rows_processed += 1
                        except DataValidationException as e:
                            logger.error("DataValidation exception processing water level in row %s" %
                                         data_entry_csv_str, e)
                            invalid_rows.append(row)
                            invalid_row_count += 1
                    else:
                        print("Failed to parse water level measurement entry %s" % data_entry_csv_str)
                        invalid_rows.append(row)
                        invalid_row_count += 1
                else:
                    logger.warning("Invalid water level measurement: %s" % row)
                    invalid_rows.append(row)
                    invalid_row_count += 1

                #############
                # temperature
                if row["TEMPERATURE"] is not None and row["TEMPERATURE"]:
                    entry_temperature = float(row["TEMPERATURE"])

                    # create a csv string with the values substituted
                    data_entry_csv_str = data_entry_template.substitute(
                        DATE=entry_date,
                        TIME=entry_time_24h,
                        CHARACTERISTIC_NAME=COSMO_CHARACTERISTIC_NAME_TEMPERATURE_WATER,
                        RESULT_UNIT=COSMO_RESULT_UNIT_TEMPERATURE_WATER,
                        RESULT_VALUE="%.4f" % entry_temperature,
                        RESULT_VALUE_TYPE=COSMO_RESULT_VALUE_TYPE,
                        RESULT_COMMENT=COSMO_RESULT_COMMENT
                    )

                    # reparse the csv string into a parsed row
                    data_entry_row = next(csv.DictReader(data_entry_csv_str.split("\n"), delimiter=',', strict=True))

                    if data_entry_row is not None and data_entry_row:
                        try:
                            db_importer.add(CosmoDataEntry(data_entry_row))
                            rows_processed += 1
                            has_valid_temperature = True
                        except DataValidationException as e:
                            logger.error("DataValidation exception processing temperature in row %s" %
                                         data_entry_csv_str, e)
                            invalid_rows.append(row)
                            invalid_row_count += 1
                    else:
                        print("Failed to parse temperature measurement entry %s" % data_entry_csv_str)
                        invalid_rows.append(row)
                        invalid_row_count += 1
                else:
                    logger.warning("Invalid temperature measurement: %s" % row)
                    invalid_rows.append(row)
                    invalid_row_count += 1
                #############
                # conductivity
                if row["CONDUCTIVITY"] is not None and row["CONDUCTIVITY"]:
                    entry_conductivity = float(row["CONDUCTIVITY"])

                    # create a csv string with the values substituted
                    data_entry_csv_str = data_entry_template.substitute(
                        DATE=entry_date,
                        TIME=entry_time_24h,
                        CHARACTERISTIC_NAME=COSMO_CHARACTERISTIC_NAME_CONDUCTIVITY,
                        RESULT_UNIT=COSMO_RESULT_UNIT_CONDUCTIVITY,
                        RESULT_VALUE="%.4f" % entry_conductivity,
                        RESULT_VALUE_TYPE=COSMO_RESULT_VALUE_TYPE,
                        RESULT_COMMENT=COSMO_RESULT_COMMENT
                    )

                    # reparse the csv string into a parsed row
                    data_entry_row = next(csv.DictReader(data_entry_csv_str.split("\n"), delimiter=',', strict=True))

                    if data_entry_row is not None and data_entry_row:
                        try:
                            db_importer.add(CosmoDataEntry(data_entry_row))
                            rows_processed += 1
                            has_valid_conductance = True
                        except DataValidationException as e:
                            logger.error("DataValidation exception processing conductivity in row %s" %
                                         data_entry_csv_str, e)
                            invalid_rows.append(row)
                            invalid_row_count += 1
                    else:
                        logger.error("Failed to parse conductivity measurement entry %s" % data_entry_csv_str)
                        invalid_rows.append(row)
                        invalid_row_count += 1
                else:
                    logger.error("Invalid conductivity measurement: %s" % row)
                    invalid_rows.append(row)
                    invalid_row_count += 1

                #############
                # specific conductance
                # requires valid conductivity and temperature
                if has_valid_conductance and has_valid_temperature and entry_temperature and entry_conductivity:

                    # calculate specific conductance from conductivity and temperature

                    # Specific Conductance = Conductivity / ((1 + 0.02 * (Temperature - 25)))
                    entry_specific_conductance = entry_conductivity / (1 + (0.02 * (entry_temperature - 25)))

                    # create a csv string with the values substituted
                    data_entry_csv_str = data_entry_template.substitute(
                        DATE=entry_date,
                        TIME=entry_time_24h,
                        CHARACTERISTIC_NAME=COSMO_CHARACTERISTIC_NAME_SPECIFIC_CONDUCTANCE,
                        RESULT_UNIT=COSMO_RESULT_UNIT_CONDUCTIVITY,
                        RESULT_VALUE="%.4f" % entry_specific_conductance,
                        RESULT_VALUE_TYPE=COSMO_RESULT_VALUE_TYPE,
                        RESULT_COMMENT=COSMO_RESULT_COMMENT
                    )

                    # reparse the csv string into a parsed row
                    data_entry_row = next(csv.DictReader(data_entry_csv_str.split("\n"), delimiter=',', strict=True))

                    if data_entry_row is not None and data_entry_row:
                        db_importer.add(CosmoDataEntry(data_entry_row))
                        rows_processed += 1
                    else:
                        logger.error("Failed to parse specific conductance measurement entry %s" % data_entry_csv_str)
                        invalid_rows.append(row)
                        invalid_row_count += 1
                else:
                    logger.error("Could not determine specific conductance due to missing measurements: %s" % row)
                    invalid_rows.append(row)
                    invalid_row_count += 1

            except Exception as e:

                # catch-all for any exception during row processing that are not caught earlier

                # push object into collection
                # log collection at end to file

                logger.error("Error constructing CosmoDataEntry", e)

                invalid_rows.append(row)
                invalid_row_count += 1

            print("\r\tRows processed: %d. Validation failures: %d" %
                  (rows_processed, invalid_row_count), end='', flush=True)

    csvread_elapsed = (timeit.default_timer() - csvread_start_time)

    log_msg = ("\nProcessed %d rows from dump file %s in %.3f sec. Found %d validation failures" %
               (rows_processed, data_dump_filename, csvread_elapsed, invalid_row_count))
    print(log_msg, flush=True)
    logger.info(log_msg)

    ################
    # log read/parse failures here. not needed for database write
    if invalid_row_count > 0:
        date_time = datetime.now()
        invalid_row_file = "./cosmo-solinst_invalid_rows_%s.log" % (date_time.strftime("%Y%m%d-%H%M%S"))

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
    # WAGG01                                site                required
    # doi.org_10.25976_0gvo-9d12.csv        data dump file      required
    ############################

    # reads sys.argv
    parser = argparse.ArgumentParser(description='Import data from a Solinst data dump into a configured database.')
    parser.add_argument('--dry-run', action='store_const', const=1, dest='dryrun',
                        help='Output database insert statements. Does not write to database.')
    parser.add_argument('-cfg', nargs=1, dest='db_cfg_file', help='Database config file in json format. Ex: cosmo.json')
    parser.add_argument(nargs=1, dest='site', help='CoSMo site. Ex: WAGG01')
    parser.add_argument(nargs=1, dest='data_dump_file', help='CoSMo data dump file. Ex: doi.org_10.25976_0gvo-9d12.csv')

    # call main with parsed args
    main(parser.parse_args())
