# debug resource
# from pprint import pprint

from mysql.connector import connect, Error
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
from RainfallEventDataEntry import RainfallEventDataEntry

# compile rainfall event data for the cnv region

# designed to run once after both cosmo-import and cnv-flowworks-import have run.

# query the data in segments. don't want to hold a massive result in memory.

# upfront cost of correlating data vs ongoing cost of redoing the same correlations multiple times in a stored procedure

########################
# rain events
#     table scan cnv rainfall
#         discard until we get a non-zero rainfall amount
#             get 1000 rows, search for rainfall >0, else get next 1000 rows...
#         set start point of rain period
#         search for end of rain period - 48 hour duration of zero rainfall in cnv rainfall
#             get 1000 rows, search for rainfall >0, else get next 1000 rows

########################

logFile = "rainfall-events.log"

logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
logging.getLogger("RainfallEventDataEntry").setLevel(logging.INFO)
logging.getLogger("DBImporter").setLevel(logging.INFO)

SCHEMA = [
    "EVENT_START_TIMESTAMP",
    "EVENT_END_TIMESTAMP",
    "EVENT_ID"
]

TARGET_TABLE = "RAINFALL_EVENTS"

SOURCE_DB_CNV_FLOWWORKS = "NSSK_CNV_FLOWWORKS"

# only one site in dataset
SOURCE_DB_CNV_FLOWWORKS_SITE = "CNVRain"

# TODO: maybe hardcode these
TARGET_DATABASE = None

# formatting for the raw id in the event id
# 2021[004]
EVENT_ID_PADDING = 3

# length of the dry period in seconds when considering if a rain event has ended
DRY_PERIOD_DURATION_THRESHOLD = 48 * 60 * 60


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

            # TODO: make sure this refers to the outer scope
            config = None

            try:
                with connection.cursor() as cursor:
                    # SHOW DATABASES LIKE "NSSK_RAINFALL_EVENT_DATA";

                    # NSSK_RAINFALL_EVENT_DATA target database
                    cursor.execute("SHOW DATABASES LIKE '%s';" % target_db)

                    cursor.fetchall()

                    if cursor.rowcount != 1:
                        # TODO: custom exception
                        raise Exception("Could not find %s target database" % target_db)
                    else:
                        logger.debug("Found target database %s" % target_db)

                    # cnv rainfall source database
                    cursor.reset()
                    cursor.execute("SHOW DATABASES LIKE '%s';" % SOURCE_DB_CNV_FLOWWORKS)
                    cursor.fetchall()

                    if cursor.rowcount != 1:
                        # TODO: custom exception
                        raise Exception("Could not find source database %s" % SOURCE_DB_CNV_FLOWWORKS)
                    else:
                        logger.debug("Found source database %s" % SOURCE_DB_CNV_FLOWWORKS)

                    # check that our destination table exists
                    logger.debug("Precheck check rainfall event destination table %s" % TARGET_TABLE)

                    cursor.reset()

                    # we checked target_db's existence a few lines up. now check that the target table exists
                    cursor.execute("use %s;" % target_db)

                    cursor.execute("SHOW TABLES LIKE '%s';" % TARGET_TABLE)
                    cursor.fetchall()
                    if cursor.rowcount != 1:
                        # TODO: custom exception
                        raise Exception(
                            "Could not find rainfall event destination table %s in target database" % TARGET_TABLE)
                    else:
                        logger.debug("Found rainfall event destination table %s" % TARGET_TABLE)

                    # target database validated. cache database name, so we can reference when running the correlation
                    global TARGET_DATABASE
                    TARGET_DATABASE = target_db

            except Error as e:
                logger.error("Error checking databases", e)
                raise e
    except Error as e:
        logger.error("Error connecting to database", e)
        raise e


# return start and end dates for cosmo conductivity, and cnv rainfall measurements
def get_measurement_date_window(cursor):
    ###################
    # determine cnv rainfall start datetime (earliest measurement)

    cnv_rainfall_date_search_template = Template(open("sql/get-cnv-flowworks-timestamp.sql.template").read())
    cnv_rainfall_date_search_sql = cnv_rainfall_date_search_template.substitute(DB=SOURCE_DB_CNV_FLOWWORKS,
                                                                                SITE=SOURCE_DB_CNV_FLOWWORKS_SITE,
                                                                                ORDER="ASC")

    logger.debug("cnv flowworks start date search sql:\n%s" % cnv_rainfall_date_search_sql)

    cursor.execute(cnv_rainfall_date_search_sql)
    row = cursor.fetchall()
    if cursor.rowcount == 1:
        # expect date in mysq datetime format

        # pprint(row[0])

        # row[0] is a tuple containing a datetime, get value with row[0][0]
        cnv_flowworks_start_time = row[0][0]
    else:
        # TODO: custom exception
        raise Exception(
            "Could not determine earliest rainfall measurement timestamp for site %s in target database"
            % SOURCE_DB_CNV_FLOWWORKS_SITE
        )

    log_msg = "Determined earliest rainfall measurement timestamp %s" % cnv_flowworks_start_time
    logger.info(log_msg)
    print(log_msg)

    # determine end datetime (most recent rainfall measurement)

    cnv_rainfall_date_search_sql = cnv_rainfall_date_search_template.substitute(DB=SOURCE_DB_CNV_FLOWWORKS,
                                                                                SITE=SOURCE_DB_CNV_FLOWWORKS_SITE,
                                                                                ORDER="DESC")

    logger.debug("cnv flowworks rainfall end date search sql:\n%s" % cnv_rainfall_date_search_sql)

    cursor.execute(cnv_rainfall_date_search_sql)
    row = cursor.fetchall()
    if cursor.rowcount == 1:
        # expect date in mysq datetime format

        # pprint(row[0])

        # row[0] is a tuple containing a datetime, get value with row[0][0]
        cnv_flowworks_end_time = row[0][0]
    else:
        # TODO: custom exception
        raise Exception(
            "Could not determine latest rainfall measurement timestamp for site %s in target database"
            % SOURCE_DB_CNV_FLOWWORKS_SITE
        )

    log_msg = "Determined latest rainfall measurement timestamp %s" % cnv_flowworks_end_time
    logger.info(log_msg)
    print(log_msg)

    #############
    # done with the supplied cursor
    cursor.reset()

    return cnv_flowworks_start_time, cnv_flowworks_end_time


# determine the end of a rainfall event, given the start date
def determine_rainfall_event_end(cursor, rainfall_event_start_datetime, end_search_datetime):
    logger.debug("Determining end of rainfall event beginning: %s", rainfall_event_start_datetime)

    rainfall_event_end_datetime = None

    if rainfall_event_start_datetime is None:
        logger.error("Cannot determine rainfall end datetime from a None start datetime")
        # TODO: exception?
        return None

    next_no_rainfall_measurement_query_template = Template(
        open("sql/get-next-no-rainfall-measurement.sql.template").read())

    next_rainfall_measurement_query_template = Template(
        open("sql/get-next-rainfall-measurement.sql.template").read())

    # find the next measurement with no rainfall
    # find the next measurement with rainfall after ^^^
    # if the timedelta of above is > 48h, done, else repeat
    #
    # there is no minimum duration for a rainfall event

    # if rainfall event has next_zero_rainfall_measurement_result of None
    #   dataset ends in the middle of the rainevent
    #   rainfall event end date is last measurement in table
    # if rainfall event has subsequent_rainfall_measurement_result of None
    #   dataset ends before a long-enough dry period is determined
    #   no rainfall event end date discernible. will likely change with next dataset update

    # TODO: query db to confirm that the measurement at rainfall_event_start_datetime is > 0. exception otherwise
    # TODO: check start datetime is before end_search_datetime

    rainfall_datetime_i = rainfall_event_start_datetime

    # search for the end of the wet period in rainfall measurements.
    # at the start of the loop, rainfall_datetime_i would be a rainfall measurement, but this may not be the case in
    # successive iterations
    while rainfall_datetime_i <= end_search_datetime:

        # find the next dry measurement
        next_no_rainfall_measurement_query_sql = next_no_rainfall_measurement_query_template.substitute(
            SEARCH_START=rainfall_datetime_i
        )

        logger.debug("Running next_no_rainfall_measurement_query_sql:\n%s",
                     next_no_rainfall_measurement_query_sql
                     )

        cursor.execute(next_no_rainfall_measurement_query_sql)

        next_zero_rainfall_measurement_result = cursor.fetchone()
        if next_zero_rainfall_measurement_result is not None:
            next_zero_rainfall_measurement_datetime = next_zero_rainfall_measurement_result[0]
        else:
            # dataset ends in the middle of a rain event
            logger.info("Could not find beginning of dry period after rainfall event start")
            # could not find the next measurementtimestamp of zero rainfall
            break

        logger.debug("Determined next zero rainfall measurement datetime: %s" % next_zero_rainfall_measurement_datetime)

        # determine length of dry event following rain event
        subsequent_rainfall_measurement_query_sql = next_rainfall_measurement_query_template.substitute(
            SEARCH_START=next_zero_rainfall_measurement_datetime
        )

        cursor.execute(subsequent_rainfall_measurement_query_sql)

        subsequent_rainfall_measurement_result = cursor.fetchone()
        if subsequent_rainfall_measurement_result is not None:
            subsequent_rainfall_measurement_datetime = subsequent_rainfall_measurement_result[0]
            logger.info("Found subsequent rainfall measurement after dry period start: %s" %
                        subsequent_rainfall_measurement_datetime)

        else:
            logger.info("Could not find subsequent rainfall event after dry period start")

            # check for a dry period lasting between the last rainfall measurement and the end of the data set
            dry_period_duration = int(abs((
                next_zero_rainfall_measurement_datetime -
                end_search_datetime
                ).total_seconds())
            )

            if dry_period_duration > DRY_PERIOD_DURATION_THRESHOLD:
                logger.debug(
                    "Dataset-Ending Dry period duration following rain period %s -> %s ==> %d above threshold." %
                    (next_zero_rainfall_measurement_datetime,
                     end_search_datetime,
                     dry_period_duration
                     )
                )

                rainfall_event_end_datetime = next_zero_rainfall_measurement_datetime
            else:
                logger.debug(
                    "Dataset-Ending Dry period duration following rain period " +
                    "%s -> %s ==> %d below threshold. Continuing search." %
                    (next_zero_rainfall_measurement_datetime,
                     end_search_datetime,
                     dry_period_duration
                     )
                )
            break

        #################################
        # dry period duration determination. if it's over the threshold, that's the end date of the rain event
        # needs total_seconds() for seconds between datetime objects
        dry_period_duration = int(abs((
            next_zero_rainfall_measurement_datetime -
            subsequent_rainfall_measurement_datetime
            ).total_seconds())
        )

        if dry_period_duration > DRY_PERIOD_DURATION_THRESHOLD:
            logger.debug(
                "Dry period duration following rain period %s -> %s ==> %d above threshold." %
                (next_zero_rainfall_measurement_datetime,
                 subsequent_rainfall_measurement_datetime,
                 dry_period_duration)
            )
            rainfall_event_end_datetime = next_zero_rainfall_measurement_datetime

            # logging start/end times of rain event done by invoker

            break
        else:
            logger.debug(
                "Dry period duration following rain period %s -> %s ==> %d below threshold. Continuing search." %
                (next_zero_rainfall_measurement_datetime,
                 subsequent_rainfall_measurement_datetime,
                 dry_period_duration
                 )
            )

            # increment our search-from datetime to the start of the next wet period
            rainfall_datetime_i = subsequent_rainfall_measurement_datetime

    return rainfall_event_end_datetime


# compile the list of rainfall events according to criteria for a sensor site
# the first non-zero rainfall measurement until the next 48-hour period of zero rainfall
def compile_rainfall_events(db_config_filename, db_importer):
    # the cnv rainfall dataset can start in the middle of a rain event
    # the cnv rainfall dataset can end in the middle of a rain event
    # each query must handle empty results
    # search for dry with no results => source dataset ends during a rain event
    # search for wet with no results => done looking for events

    # We can define a rainfall event as the first non-zero rainfall measurement
    # until the next 48 hour period of zero rainfall.
    #
    # this can mean that rain starts and stops repeatedly, and it's all in one rain event

    config = DBConfigFactory.build(db_config_filename)

    try:
        with (connect(
                host=config[DBConfig.CONFIG_HOST],
                port=int(config[DBConfig.CONFIG_PORT]),
                user=config[DBConfig.CONFIG_USER],
                password=config[DBConfig.CONFIG_PASS],
                database=config[DBConfig.CONFIG_DBASE],
        ) as connection):
            config[DBConfig.CONFIG_PASS] = None

            try:
                with connection.cursor() as cursor:

                    (cnv_flowworks_start_datetime, cnv_flowworks_end_datetime) = get_measurement_date_window(cursor)

                    print(("==========\n" +
                           "cnv_flowworks_start_datetime: %s\n" +
                           "cnv_flowworks_end_datetime: %s"
                           ) %
                          (
                              cnv_flowworks_start_datetime,
                              cnv_flowworks_end_datetime)
                          )

                    rainfall_event_count = 0

                    rainfall_date_i = cnv_flowworks_start_datetime

                    rainfall_event_processing_start_time = timeit.default_timer()

                    next_rainfall_measurement_query_template = Template(
                        open("sql/get-next-rainfall-measurement.sql.template").read())

                    event_counter = 0

                    previous_measurement_datetime_year = None

                    # until there's no more measurements to search
                    while rainfall_date_i <= cnv_flowworks_end_datetime:
                        #############
                        # find the start of the next rainfall event
                        next_rainfall_measurement_query_sql = next_rainfall_measurement_query_template.substitute(
                            SEARCH_START=rainfall_date_i
                        )

                        logger.debug("Running find-next-rainfall-measurement query:\n%s",
                                     next_rainfall_measurement_query_sql
                                     )

                        cursor.execute(next_rainfall_measurement_query_sql)
                        row = cursor.fetchone()

                        if row is None:
                            logger.info(
                                "No next rainfall measurement found. We're likely at the end of the dataset. Bailing..."
                            )
                            break

                        # MeasurementTimestamp,Rainfall
                        rainfall_event_start_datetime = row[0]

                        #############
                        # find the next 48-hour period of zero rainfall
                        #
                        # default initialization of start + 1s as a measure to ensure the loop terminates
                        # rainfall_event_end_datetime = rainfall_event_start_datetime + datetime.timedelta(seconds=1)

                        rainfall_event_end_datetime = determine_rainfall_event_end(cursor,
                                                                                   rainfall_event_start_datetime,
                                                                                   cnv_flowworks_end_datetime)

                        # bail out early if there's no next dry period. assign end datetime to the last date
                        if rainfall_event_end_datetime is None:
                            logger.info("Rainfall end date beginning at %s is at end of dataset. Discarding." %
                                        rainfall_event_start_datetime)
                            break

                        #############
                        # store rainfall event

                        # TODO: validity checks on determined start/end datetimes

                        logger.debug("Found rainfall event. Start: %s, End: %s",
                                     rainfall_event_start_datetime,
                                     rainfall_event_end_datetime
                                     )

                        # compute event id
                        # [event start year][event counter]
                        # i.e. 2022004 => 4th event in year 2022

                        measurement_datetime_year: int = rainfall_event_start_datetime.year

                        # reset to 0 if it's the first event of the year, otherwise increment
                        if measurement_datetime_year == previous_measurement_datetime_year:
                            event_counter += 1
                        else:
                            logger.debug("Resetting event id counter")

                            # 1-based indexing for events
                            event_counter = 1

                        # cache measurement year to see if it changes in the next iteration
                        previous_measurement_datetime_year = measurement_datetime_year

                        # compute the event_id
                        event_id = str(
                            int(measurement_datetime_year)
                        ) + str(event_counter).zfill(EVENT_ID_PADDING)

                        # create a DataEntry with our data and add it to the db_importer
                        rainfall_event_data_entry = {
                            SCHEMA[0]: rainfall_event_start_datetime,
                            SCHEMA[1]: rainfall_event_end_datetime,
                            SCHEMA[2]: event_id
                        }

                        new_data_entry = RainfallEventDataEntry(rainfall_event_data_entry)
                        new_data_entry.set_db_destination(TARGET_TABLE)

                        db_importer.add(new_data_entry)

                        rainfall_event_count += 1

                        print("\r\tEvents processed: %d" % rainfall_event_count, end='', flush=True)

                        #############
                        # increment our _date_i
                        #
                        rainfall_date_i = rainfall_event_end_datetime

                rainfall_event_processing_elapsed_time = (timeit.default_timer() - rainfall_event_processing_start_time)
                log_msg = "Completed rainfall event Processing in %.3f sec" % rainfall_event_processing_elapsed_time
                print("\n%s" % log_msg, flush=True)
                logger.info(log_msg)

            except Error as e:
                logger.error("Error running event compilation", e)
    except Error as e:
        logger.error("Error connecting to database", e)

    # find the start of the rainfall data

    # find the first non-zero rainfall amount
    # find the end of the rainfall event
    # store event start/end
    # repeat
    pass


# TODO
# def postcheck(db_config_filename):
# check that start is before end date
# check that each rain event does not overlap with all others


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

    try:
        precheck(db_config_filename)
    except Error as e:
        print("Precheck failed. Bailing out...", e)
        exit(1)

    print("Precheck passed. Running rainfall event compilation...")

    # build the importer. target database is defined in the config
    db_importer = DBImporter(db_config_filename)
    db_importer.set_importer_name("rainfall-events")
    db_importer.set_schema(SCHEMA)

    print("Running rainfall event determination")
    compile_rainfall_events(db_config_filename, db_importer)

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
    # -cfg conductivity-rainfall-correlation.json            database config     not required
    ############################

    # reads sys.argv
    parser = argparse.ArgumentParser(
        description='Determine rainfall events and store results in the database.')
    parser.add_argument('--dry-run', action='store_const', const=1, dest='dryrun',
                        help='Output database insert statements. Does not write to database.')
    parser.add_argument('-cfg', nargs=1, dest='db_cfg_file',
                        help='Database config file in json format. Ex: conductivity-rainfall-correlation.json')

    # call main with parsed args
    main(parser.parse_args())
