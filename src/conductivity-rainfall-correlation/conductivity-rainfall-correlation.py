from pprint import pprint

from mysql.connector import connect, Error, IntegrityError
import datetime
from string import Template

from pathlib import Path
import sys
import argparse
import logging
import timeit

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

from src.importer.DBImporter import DBImporter
from src.importer.DBConfig import DBConfig
from src.importer.DBConfigFactory import DBConfigFactory
from ConductivityRainfallDataEntry import ConductivityRainfallDataEntry

# correlate conductivity in cosmo data with rainfall amount in cnv rainfall data

# designed to run once after both cosmo-import and cnv-rainfall-import have run.
# each run should wipe any existing table

# query the cosmo data in segments. don't want to hold a massive result in memory.

# upfront cost of correlating data vs ongoing cost of redoing the same correlations multiple times in a stored procedure

########################
# For each entry of conductance in a cosmo sensor table
#     If there exists a rainfall amount within 10 minutes of the time of cosmo sensor read
#         Log the correlation at the time of the cosmo sensor read.
#         if there are multiple rainfall readings within the window, take the average/median/something
#     else
#         continue and consider the cosmo sensor read uncorrelated with rainfall
########################

logFile = "conductivity-rainfall-correlation.log"

logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)

logging.getLogger("DBImporter").setLevel(logging.INFO)

# time window to search for a corresponding conductivity value
# +/- 5 minutes. in seconds
CORRELATION_WINDOW = 5 * 60

# increment to step through conductivity values in COSMO table
# shouldn't be too big
# 7 days
CORRELATION_INCREMENT = 60 * 60 * 24 * 7

SENSORS = [
    "WAGG01",
    "WAGG03"
]

SCHEMA = [
    "COSMO_TIMESTAMP",
    "CONDUCTANCE_RESULT",
    "CNV_RAINFALL",
    "CNV_TIMESTAMP"
]

SOURCE_DB_NSSK_COSMO = "NSSK_COSMO"
SOURCE_DB_CNV_RAINFALL = "NSSK_CNV_RAINFALL"

# only one site in dataset
SOURCE_DB_CNV_RAINFALL_SITE = "CNV"

TARGET_DATABASE = None


def precheck(conf_file):
    # need a destination database/table for the correlated data
    # database created by setup script? => yes
    # table created by setup script? => yes

    config = DBConfigFactory.build(conf_file)

    #################
    # check that the source databases exist and the target database exists
    try:
        with connect(
                host=config[DBConfig.CONFIG_HOST],
                port=int(config[DBConfig.CONFIG_PORT]),
                user=config[DBConfig.CONFIG_USER],
                password=config[DBConfig.CONFIG_PASS],
                database=config[DBConfig.CONFIG_DBASE],
        ) as connection:
            target_db = config[DBConfig.CONFIG_DBASE]
            config = None

            try:
                with connection.cursor() as cursor:
                    # SHOW DATABASES LIKE "NSSK_CONDUCTIVITY_RAINFALL_CORRELATION";

                    # NSSK_CONDUCTIVITY_RAINFALL_CORRELATION target database
                    cursor.execute("SHOW DATABASES LIKE '%s';" % target_db)

                    cursor.fetchall()

                    if cursor.rowcount != 1:
                        # TODO: custom exception
                        raise Exception("Could not find %s target database" % target_db)
                    else:
                        logger.debug("Found target database %s" % target_db)

                    # nssk cosmo source database
                    cursor.reset()
                    cursor.execute("SHOW DATABASES LIKE '%s';" % SOURCE_DB_NSSK_COSMO)
                    cursor.fetchall()

                    if cursor.rowcount != 1:
                        # TODO: custom exception
                        raise Exception("Could not find source database %s" % SOURCE_DB_NSSK_COSMO)
                    else:
                        logger.debug("Found source database %s" % SOURCE_DB_NSSK_COSMO)

                    # cnv rainfall source database
                    cursor.reset()
                    cursor.execute("SHOW DATABASES LIKE '%s';" % SOURCE_DB_CNV_RAINFALL)
                    cursor.fetchall()

                    if cursor.rowcount != 1:
                        # TODO: custom exception
                        raise Exception("Could not find source database %s" % SOURCE_DB_CNV_RAINFALL)
                    else:
                        logger.debug("Found source database %s" % SOURCE_DB_CNV_RAINFALL)

                    # check that a table for each sensor exists

                    for sensor in SENSORS:
                        logger.debug("Precheck Sensor %s" % sensor)

                        cursor.reset()
                        cursor.execute("SHOW TABLES LIKE '%s';" % sensor)
                        cursor.fetchall()
                        if cursor.rowcount != 1:
                            # TODO: custom exception
                            raise Exception("Could not find sensor table %s in target database" % sensor)
                        else:
                            logger.debug("Found Sensor Table %s" % sensor)

                    # target database validated. cache database name, so we can reference when running the correlation
                    global TARGET_DATABASE
                    TARGET_DATABASE = target_db

            except Error as e:
                logger.error("Error checking databases", e)
    except Error as e:
        logger.error("Error connecting to database", e)


# return start and end dates for cosmo conductivity, and cnv rainfall measurements
def get_measurement_date_windows(cursor, sensor_name):
    ###################
    # determine cosmo start datetime (earliest conductivity measurement)
    # cosmo datetimes are split across date and time fields

    cosmo_date_search_template = Template(open("sql/get-cosmo-timestamp.sql.template").read())
    cosmo_date_search_sql = cosmo_date_search_template.substitute(DB=SOURCE_DB_NSSK_COSMO,
                                                                  SITE=sensor_name,
                                                                  ORDER="ASC")

    logger.debug("cosmo start date search sql:\n%s" % cosmo_date_search_sql)

    cursor.execute(cosmo_date_search_sql)
    row = cursor.fetchall()
    if cursor.rowcount == 1:
        # expect date in mysq datetime format
        print("raw start datetime: %s" % row[0][0])

        # pprint(row[0])

        # row[0] is a tuple containing a datetime, get value with row[0][0]
        cosmo_start_time = row[0][0]
    else:
        # TODO: custom exception
        raise Exception(
            "Could not determine earliest conductivity measurement timestamp for site %s in target database" %
            sensor_name)

    log_msg = "Determined earliest conductivity measurement timestamp %s" % cosmo_start_time
    logger.info(log_msg)
    print(log_msg)

    # determine end datetime (most recent conductivity measurement)
    # cosmo datetimes are split across date and time fields

    cosmo_date_search_sql = cosmo_date_search_template.substitute(DB=SOURCE_DB_NSSK_COSMO,
                                                                  SITE=sensor_name,
                                                                  ORDER="DESC")

    logger.debug("cosmo end date search sql:\n%s" % cosmo_date_search_sql)

    cursor.execute(cosmo_date_search_sql)
    row = cursor.fetchall()
    if cursor.rowcount == 1:
        # row[0] is a tuple containing a datetime, get value with row[0][0]
        cosmo_end_time = row[0][0]
    else:
        # TODO: custom exception
        raise Exception(
            "Could not determine latest conductivity measurement timestamp for site %s in target database" % sensor_name)

    log_msg = "Determined latest conductivity measurement timestamp %s" % cosmo_end_time
    logger.info(log_msg)
    print(log_msg)

    ###################
    # determine cnv rainfall start datetime (earliest measurement)

    cnv_rainfall_date_search_template = Template(open("sql/get-cnv-rainfall-timestamp.sql.template").read())
    cnv_rainfall_date_search_sql = cnv_rainfall_date_search_template.substitute(DB=SOURCE_DB_CNV_RAINFALL,
                                                                                SITE=SOURCE_DB_CNV_RAINFALL_SITE,
                                                                                ORDER="ASC")

    logger.debug("cnv rainfall start date search sql:\n%s" % cnv_rainfall_date_search_sql)

    cursor.execute(cnv_rainfall_date_search_sql)
    row = cursor.fetchall()
    if cursor.rowcount == 1:
        # expect date in mysq datetime format

        # pprint(row[0])

        # row[0] is a tuple containing a datetime, get value with row[0][0]
        cnv_rainfall_start_time = row[0][0]
    else:
        # TODO: custom exception
        raise Exception(
            "Could not determine earliest rainfall measurement timestamp for site %s in target database" % SOURCE_DB_CNV_RAINFALL_SITE)

    log_msg = "Determined earliest rainfall measurement timestamp %s" % cnv_rainfall_start_time
    logger.info(log_msg)
    print(log_msg)

    # determine end datetime (most recent rainfall measurement)

    cnv_rainfall_date_search_sql = cnv_rainfall_date_search_template.substitute(DB=SOURCE_DB_CNV_RAINFALL,
                                                                                SITE=SOURCE_DB_CNV_RAINFALL_SITE,
                                                                                ORDER="DESC")

    logger.debug("cnv rainfall end date search sql:\n%s" % cnv_rainfall_date_search_sql)

    cursor.execute(cnv_rainfall_date_search_sql)
    row = cursor.fetchall()
    if cursor.rowcount == 1:
        # expect date in mysq datetime format

        # pprint(row[0])

        # row[0] is a tuple containing a datetime, get value with row[0][0]
        cnv_rainfall_end_time = row[0][0]
    else:
        # TODO: custom exception
        raise Exception(
            "Could not determine latest rainfall measurement timestamp for site %s in target database" % SOURCE_DB_CNV_RAINFALL_SITE)

    log_msg = "Determined latest rainfall measurement timestamp %s" % cnv_rainfall_end_time
    logger.info(log_msg)
    print(log_msg)

    #############
    # done with the supplied cursor
    cursor.reset()

    return cosmo_start_time, cosmo_end_time, cnv_rainfall_start_time, cnv_rainfall_end_time


# run the correlation for a sensor site. NSSK_COSMO is the reference data source with conductivity measurements.
# search for matching rainfall measurements within a time window of a conductivity measurement.
# sensor site table should be manually dropped before running (or TODO: automatically?)
def run_correlation(sensor_name, db_config_filename, db_importer):
    config = DBConfigFactory.build(db_config_filename)

    try:
        with (connect(
                host=config[DBConfig.CONFIG_HOST],
                port=int(config[DBConfig.CONFIG_PORT]),
                user=config[DBConfig.CONFIG_USER],
                password=config[DBConfig.CONFIG_PASS],
                database=config[DBConfig.CONFIG_DBASE],
        ) as connection):
            config = None

            try:
                with connection.cursor() as cursor:

                    (cosmo_start_time,
                     cosmo_end_time,
                     cnv_rainfall_start_time,
                     cnv_rainfall_end_time) = get_measurement_date_windows(cursor, sensor_name)

                    ###################
                    # date determinations

                    print(("==========\n" +
                           "cosmo_start_time: %s\n" +
                           "cosmo_end_time: %s\n" +
                           "cnv_rainfall_start_time: %s\n" +
                           "cnv_rainfall_end_time: %s"
                           ) %
                          (cosmo_start_time,
                           cosmo_end_time,
                           cnv_rainfall_start_time,
                           cnv_rainfall_end_time)
                          )

                    # TODO dynamically determine the outer interval from these 4 dates
                    # have to start at the latest of first cosmo conductivity/cnv rainfall readings
                    # have to end at the earliest of last cosmo conductivity/cnv rainfall readings
                    # for now, consider the cosmo dates as the determiner of the outer interval and
                    # leave the extra processing on the table

                    ###################
                    # correlation

                    correlation_query_template = Template(
                        open("sql/correlate-conductivity-and-rainfall.sql.template").read())

                    # TODO how hard do we want to lean on the query for this?
                    #

                    # resolve conductivity values for times in the sensor table

                    # new or update? => always new, note duplicates and check measurement differences

                    correlated_value_count = 0

                    # cosmo_block_query = Template(open("sql/cosmo-block-query.sql.template").read())

                    cosmo_date_i = cosmo_start_time

                    correlation_processing_start_time = timeit.default_timer()

                    # for each cosmo conductivity measurement in our cosmo data, in time chunks of CORRELATION_INCREMENT
                    while cosmo_date_i <= cosmo_end_time:
                        # retrieve a block of sensor data.
                        # should be okay if end date is past cosmo_end_time- nothing will be pulled from the db
                        # start: cosmo_date_i
                        # end: cosmo_date_i + CORRELATION_INCREMENT
                        cosmo_block_start_date = cosmo_date_i
                        cosmo_block_end_date = cosmo_date_i + datetime.timedelta(seconds=CORRELATION_INCREMENT)

                        # retrieve the cnv rainfall data for the corresponding date range, +/- CORRELATION_WINDOW
                        correlation_start_time = cosmo_block_start_date - datetime.timedelta(seconds=CORRELATION_WINDOW)
                        correlation_end_time = cosmo_block_end_date + datetime.timedelta(seconds=CORRELATION_WINDOW)

                        # print(("==========\n" +
                        #        "cosmo_block_start_date: %s\n" +
                        #        "cosmo_block_end_date: %s\n" +
                        #        "correlation_start_time: %s\n" +
                        #        "correlation_end_time: %s"
                        #        ) %
                        #       (cosmo_block_start_date,
                        #        cosmo_block_end_date,
                        #        correlation_start_time,
                        #        correlation_end_time)
                        #       )

                        ####################
                        # run correlation
                        #
                        # build query from template
                        # run query
                        # iterate and process results
                        # dump into DBImporter
                        correlation_block_query_sql = correlation_query_template.substitute(
                            COSMO_START_DATETIME=cosmo_block_start_date,
                            COSMO_END_DATETIME=cosmo_block_end_date,
                            CNV_RAINFALL_START_DATETIME=correlation_start_time,
                            CNV_RAINFALL_END_DATETIME=correlation_end_time
                        )
                        cursor.execute(correlation_block_query_sql)

                        row = cursor.fetchone()
                        while row is not None:

                            # COSMO_TIMESTAMP, CONDUCTANCE_RESULT, CNV_RAINFALL, CNV_TIMESTAMP

                            # account for a single cosmo conductance measurement correlating to
                            # multiple cnv rainfall measurements. ex:
                            # 2023-01-05 20:20:00, 118.0, 0.232, 2023-01-05 20:20:00
                            # 2023-01-05 20:20:00, 118.0, 0.232, 2023-01-05 20:25:00

                            data_entry = {
                                SCHEMA[0]: row[0],
                                SCHEMA[1]: row[1],
                                SCHEMA[2]: row[2],
                                SCHEMA[3]: row[3]
                            }

                            new_entry = ConductivityRainfallDataEntry(data_entry)
                            new_entry.set_db_destination(sensor_name)

                            db_importer.add(new_entry)

                            correlated_value_count += 1

                            print("\r\tCorrelations processed: %d" % correlated_value_count, end='', flush=True)



                            # grab next row
                            row = cursor.fetchone()

                        # increment source block start time
                        # ensure overlapping time windows are managed by query, and here
                        # query should be date_field >= start_date and date_field < end_date
                        cosmo_date_i = cosmo_date_i + datetime.timedelta(seconds=CORRELATION_INCREMENT)

                correlation_processing_elapsed_time = (timeit.default_timer() - correlation_processing_start_time)
                log_msg = "Completed correlation Processing in %.3f sec" % correlation_processing_elapsed_time
                print("\n%s" % log_msg, flush=True)
                logger.info(log_msg)

            # iterate over cosmo timestamps and query resolved values
            # resolved dates +/- resolution window
            # do any math to take or compute the conductivity
            # if no more cosmo timestamps for a time interval
            # get the next interval
            # repeat as above

            except Error as e:
                logger.error("Error checking databases", e)
    except Error as e:
        logger.error("Error connecting to database", e)


##############################################

def main(parsed_args):
    # handle parsed arguments

    dry_run = False
    db_config_filename = None

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

    precheck(db_config_filename)
    print("Precheck passed. Running correlation...")

    # build the importer. target database is defined in the config
    db_importer = DBImporter(db_config_filename)
    db_importer.set_importer_name("conductivity-rainfall-correlation")
    db_importer.set_schema(SCHEMA)

    # for a sensor
    #   get the start date and end date
    for sensor in SENSORS:
        print("Running conductivity-rainfall correlation for sensor %s" % sensor)
        run_correlation(sensor, db_config_filename, db_importer)

    #########################
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
    # --dry-run                                              read data dump file and output sql statements.
    # -cfg cnv-rainfall.json                                 database config     not required
    ############################

    # reads sys.argv
    parser = argparse.ArgumentParser(
        description='Compute conductivity-rainfall correlation and store results in the database.')
    parser.add_argument('--dry-run', action='store_const', const=1, dest='dryrun',
                        help='Output database insert statements. Does not write to database.')
    parser.add_argument('-cfg', nargs=1, dest='db_cfg_file',
                        help='Database config file in json format. Ex: conductivity-rainfall-correlation.json')

    # call main with parsed args
    main(parser.parse_args())
