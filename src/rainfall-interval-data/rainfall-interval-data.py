from sqlite3 import Cursor

from mysql.connector import connect, Error
import datetime
from string import Template

from pathlib import Path
import sys
import argparse
import logging
import timeit

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

from RainfallIntervalDataEntry import RainfallIntervalDataEntry
from src.importer.DBImporter import DBImporter
from src.importer.DBConfig import DBConfig
from src.importer.DBConfigFactory import DBConfigFactory
from src.exception.DataValidationException import DataValidationException
from src.exception.PrecheckFailedException import PrecheckFailedException

logFile = "rainfall-interval-data.log"

logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
logging.getLogger("RainfallIntervalDataEntry").setLevel(logging.INFO)
logging.getLogger("DBImporter").setLevel(logging.INFO)

TRACE_LOGGING = True

# time window to search for a corresponding conductivity value
# +/- 2 minutes. in seconds
# cnv hydrometrics measurements are every 5 minutes
# TODO: what changes if the measurement frequency changes
CNV_HYDROMETRIC_CORRELATION_WINDOW = 2 * 60

# time window for two summed 5-min rainfall measurements, to be correlated with existing cosmo/cnv hydro measurements
# +/- 5 minutes. in seconds
CNV_FLOWWORKS_CORRELATION_WINDOW = 5 * 60

# increment to step through measurements in a COSMO table
# shouldn't be too big
# 7 days
SCAN_WINDOW_DAYS = 14
COSMO_SCAN_INCREMENT = 60 * 60 * 24 * SCAN_WINDOW_DAYS

# increment to step through measurements in a cnv flowworks table
# shouldn't be too big
# 7 days
# 1 day for test value
CNV_FLOWWORKS_SCAN_INCREMENT = 60 * 60 * 24 * SCAN_WINDOW_DAYS

# increment to step through measurements in a cnv hydrometric table
# shouldn't be too big
# 7 days
CNV_HYDROMETRIC_SCAN_INCREMENT = 60 * 60 * 24 * SCAN_WINDOW_DAYS

# ideally how far apart rainfall measurements are apart timewise for consideration in 10-minute intervals
# in seconds
RAINFALL_INTERVAL_LEN = 5 * 60

# buffer to account for measurement drift in RAINFALL_INTERVAL_LEN
# in seconds
RAINFALL_INTERVAL_BUFFER = 60

# correlating rainfall measurements to cosmo/cnv hydro measurements
# in seconds
RAINFALL_CORRELATION_THRESHOLD = 45

SOURCE_DB_NSSK_COSMO = "NSSK_COSMO"
SOURCE_DB_CNV_FLOWWORKS = "NSSK_CNV_FLOWWORKS"

# only one site in dataset
SOURCE_DB_CNV_FLOWWORKS_SITE = "CNVRain"

# only one site in dataset
SOURCE_DB_CNV_HYDROMETRICS_SITE = "WaggCreek"

COSMO_SITES = [
    "WAGG01",
    "WAGG03"
]

SCHEMA = [
    RainfallIntervalDataEntry.COSMO_TIMESTAMP_FIELD,
    RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD,
    RainfallIntervalDataEntry.COSMO_TEMPERATURE_WATER_FIELD,

    RainfallIntervalDataEntry.CNV_HYDROMETRIC_TIMESTAMP_FIELD,
    RainfallIntervalDataEntry.CNV_HYDROMETRIC_REVISED_STAGE_FIELD,

    RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_START_TIMESTAMP_FIELD,
    RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_END_TIMESTAMP_FIELD,
    RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_AMT_FIELD,
    RainfallIntervalDataEntry.CNV_FLOWWORKS_BARO_PRESSURE_FIELD,
    RainfallIntervalDataEntry.CNV_FLOWWORKS_AIR_TEMPERATURE_FIELD,
]

MYSQL_DATE_FMT = "%Y-%m-%d %H:%M:%S"

# TODO: use these and implement in other importers as warranted
ORDER_ASC = "ASC"
ORDER_DESC = "DESC"

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
            config[DBConfig.CONFIG_PASS] = None
            config[DBConfig.CONFIG_USER] = None

            try:
                with connection.cursor() as cursor:
                    # SHOW DATABASES LIKE "NSSK_CONDUCTIVITY_RAINFALL_CORRELATION";

                    # NSSK_CONDUCTIVITY_RAINFALL_CORRELATION target database
                    cursor.execute("SHOW DATABASES LIKE '%s';" % target_db)

                    cursor.fetchall()

                    if cursor.rowcount != 1:
                        raise PrecheckFailedException("Could not find %s target database" % target_db)
                    else:
                        logger.debug("Found target database %s" % target_db)

                    # nssk cosmo source database
                    cursor.reset()
                    cursor.execute("SHOW DATABASES LIKE '%s';" % SOURCE_DB_NSSK_COSMO)
                    cursor.fetchall()

                    if cursor.rowcount != 1:
                        raise PrecheckFailedException("Could not find source database %s" % SOURCE_DB_NSSK_COSMO)
                    else:
                        logger.debug("Found source database %s" % SOURCE_DB_NSSK_COSMO)

                    # cnv rainfall source database
                    cursor.reset()
                    cursor.execute("SHOW DATABASES LIKE '%s';" % SOURCE_DB_CNV_FLOWWORKS)
                    cursor.fetchall()

                    if cursor.rowcount != 1:
                        raise PrecheckFailedException("Could not find source database %s" % SOURCE_DB_CNV_FLOWWORKS)
                    else:
                        logger.debug("Found source database %s" % SOURCE_DB_CNV_FLOWWORKS)

                    # check that a table for each sensor exists
                    for sensor in COSMO_SITES:
                        logger.debug("Precheck Cosmo sensor site %s" % sensor)

                        cursor.reset()
                        cursor.execute("SHOW TABLES LIKE '%s';" % sensor)
                        cursor.fetchall()
                        if cursor.rowcount != 1:
                            raise PrecheckFailedException("Could not find sensor table %s in target database" % sensor)
                        else:
                            logger.debug("Found Sensor Table %s" % sensor)

                        # check that destination table is empty
                        cursor.reset()
                        cursor.execute(
                            "SELECT EXISTS(SELECT 1 FROM %s.%s LIMIT 1) AS is_not_empty;" % (target_db, sensor)
                        )
                        if cursor.fetchone()[0]:
                            logger.debug("Truncating destination table %s.%s" % (target_db, sensor))

                            cursor.execute("truncate table %s.%s" % (target_db, sensor))
                        else:
                            logger.debug("Destination table %s.%s is empty. Proceeding." % (target_db, sensor))
            except Error as e:
                logger.error("Error checking databases", e)
                raise
    except Error as e:
        logger.error("Error connecting to database", e)
        raise


# return start and end dates for both cosmo conductivity measurements
def get_cosmo_measurements_date_window(cursor, sensor_name):
    ###################
    # determine cosmo start datetime (earliest conductivity measurement)
    # cosmo datetimes are split across date and time fields

    # TODO move this and other template reads to precheck. file existence check should not happen this late
    cosmo_date_search_template = Template(open("sql/get-cosmo-timestamp.sql.template").read())
    cosmo_date_search_sql = cosmo_date_search_template.substitute(DB=SOURCE_DB_NSSK_COSMO, SITE=sensor_name,
                                                                  ORDER="ASC")
    if TRACE_LOGGING:
        logger.debug("cosmo start date search sql:\n%s" % cosmo_date_search_sql)

    cursor.execute(cosmo_date_search_sql)
    row = cursor.fetchall()
    if cursor.rowcount == 1:
        # expect date in mysq datetime format
        if TRACE_LOGGING:
            logger.debug("raw start datetime: %s" % row[0][0])

        # pprint(row[0])

        # row[0] is a tuple containing a datetime, get value with row[0][0]
        cosmo_start_time = row[0][0]
    else:
        # TODO: custom exception
        raise Exception(
            "Could not determine earliest cosmo measurement timestamp for site %s in target database" %
            sensor_name)

    log_msg = "Determined earliest cosmo measurement timestamp %s" % cosmo_start_time
    logger.info(log_msg)
    print(log_msg)

    # determine end datetime (most recent conductivity measurement)
    # cosmo datetimes are split across date and time fields

    cosmo_date_search_sql = cosmo_date_search_template.substitute(DB=SOURCE_DB_NSSK_COSMO, SITE=sensor_name,
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
            "Could not determine latest cosmo measurement timestamp for site %s in target database" % sensor_name
        )

    log_msg = "Determined latest cosmo measurement timestamp %s" % cosmo_end_time
    logger.info(log_msg)
    print(log_msg)

    return cosmo_start_time, cosmo_end_time


# return start and end dates for cnv rainfall measurements
def get_cnv_flowworks_measurements_date_window(cursor: Cursor, sensor_name):
    ###################
    # determine cnv_flowworks start datetime

    # TODO move this and other template reads to precheck
    cnv_flowworks_date_search_template = Template(open("sql/get-cnv-flowworks-timestamp.sql.template").read())
    cnv_flowworks_date_search_sql = cnv_flowworks_date_search_template.substitute(
        SITE=sensor_name,
        ORDER="ASC"
    )

    logger.debug("cnv flowworks start date search sql:\n%s" % cnv_flowworks_date_search_sql)

    cursor.execute(cnv_flowworks_date_search_sql)
    row = cursor.fetchall()
    if cursor.rowcount == 1:
        # expect date in mysq datetime format
        print("raw start datetime: %s" % row[0][0])

        # pprint(row[0])

        # row[0] is a tuple containing a datetime, get value with row[0][0]
        cnv_flowworks_start_time = row[0][0]
    else:
        # TODO: custom exception
        raise Exception(
            "Could not determine earliest rainfall measurement timestamp for site %s in target database" %
            sensor_name)

    log_msg = "Determined earliest rainfall measurement timestamp %s" % cnv_flowworks_start_time
    logger.info(log_msg)
    print(log_msg)

    # determine end datetime (most recent conductivity measurement)
    # cosmo datetimes are split across date and time fields

    cnv_flowworks_date_search_sql = cnv_flowworks_date_search_template.substitute(SITE=sensor_name, ORDER="DESC")

    logger.debug("cnv_flowworks end date search sql:\n%s" % cnv_flowworks_date_search_sql)

    cursor.execute(cnv_flowworks_date_search_sql)
    row = cursor.fetchall()
    if cursor.rowcount == 1:
        # row[0] is a tuple containing a datetime, get value with row[0][0]
        cnv_flowworks_end_time = row[0][0]
    else:
        # TODO: custom exception
        raise Exception(
            "Could not determine latest rainfall measurement timestamp for site %s in target database" % sensor_name
        )

    log_msg = "Determined latest rainfall measurement timestamp %s" % cnv_flowworks_end_time
    logger.info(log_msg)
    print(log_msg)

    return cnv_flowworks_start_time, cnv_flowworks_end_time


# return start and end dates for cnv hydrometric measurements
def get_cnv_hydrometric_measurements_date_window(cursor, sensor_name):
    ###################
    # determine cnv_hydrometric start datetime

    # TODO move this and other template reads to precheck
    cnv_hydrometric_date_search_template = Template(open("sql/get-cnv-hydrometric-timestamp.sql.template").read())
    cnv_hydrometric_date_search_sql = cnv_hydrometric_date_search_template.substitute(SITE=sensor_name, ORDER="ASC")

    logger.debug("cnv flowworks start date search sql:\n%s" % cnv_hydrometric_date_search_sql)

    cursor.execute(cnv_hydrometric_date_search_sql)
    row = cursor.fetchall()
    if cursor.rowcount == 1:
        # expect date in mysq datetime format
        print("raw start datetime: %s" % row[0][0])

        # pprint(row[0])

        # row[0] is a tuple containing a datetime, get value with row[0][0]
        cnv_hydrometric_start_time = row[0][0]
    else:
        # TODO: custom exception
        raise Exception(
            "Could not determine earliest hydrometric measurement timestamp for site %s in target database" %
            sensor_name)

    log_msg = "Determined earliest hydrometric measurement timestamp %s" % cnv_hydrometric_start_time
    logger.info(log_msg)
    print(log_msg)

    # determine end datetime (most recent cnv hydrometric measurement)
    # cnv hydrometric datetimes are split across date and time fields

    cnv_hydrometric_date_search_sql = cnv_hydrometric_date_search_template.substitute(SITE=sensor_name, ORDER="DESC")

    logger.debug("cnv_hydrometric end date search sql:\n%s" % cnv_hydrometric_date_search_sql)

    cursor.execute(cnv_hydrometric_date_search_sql)
    row = cursor.fetchall()
    if cursor.rowcount == 1:
        # row[0] is a tuple containing a datetime, get value with row[0][0]
        cnv_hydrometric_end_time = row[0][0]
    else:
        # TODO: custom exception
        raise Exception(
            "Could not determine latest hydrometric measurement timestamp for site %s in target database" % sensor_name
        )

    log_msg = "Determined latest rainfall hydrometric timestamp %s" % cnv_hydrometric_end_time
    logger.info(log_msg)
    print(log_msg)

    return cnv_hydrometric_start_time, cnv_hydrometric_end_time


def collect_cosmo_and_cnvhydro_measurements(cosmo_site_name, cnv_hydro_site_name, db_config_filename):
    # different from strict correlation, as we're keeping cosmo and cnv hydrometric measurements that do not correlate
    # also are not narrowing to the time window where measurements from both datasets are present
    #
    # iterate in chunks over cosmo and cnv hydrometric datasets. correlation should be done in the chunk time window
    # to save a lot of unnecessary processing. TODO: maybe a temp mongodb for staging work (local or remote)

    # TODO: what happens if there's very skewed timestamps for measurements (i.e. cnv hydro starts 2 years before cosmo)?
    # story: a new cosmo site with conductivity data comes online
    #
    # we still want these measurements because maybe we have rainfall data from that time period
    # cosmo is our primary dataset where we look to find time-correlated values in the cnv hydro dataset
    # after all regular correlation processing is done, check if the first cnv hydro measurement is before the first cosmo
    # measurement
    # yes => use cnv hydro measurement template to retrieve start_cnv_hydro to start_cosmo. still page through this
    # just add these to the final measurement set. no need to check for correlation.

    # TODO: what happens if cosmo stops well before cnv hydrometric?
    # just like above, but do the comparison with the dataset end timestamps

    # TODO: what if there's a large gap in cosmo measurements but not cnv hydro?
    # should be okay, both cosmo and cnv hydro data for the time block are retrieved before a correlation check is run
    # nothing will correlate, and the full block of cnv hydrometric measurements will be added to cnv_hydro_measurements

    # needs to be a list since these RainfallIntervalDataEntry will have both cosmo and cnv hydrometric timestamps
    correlated_measurements = []

    # dict to store uncorrelated measurements for each dataset. still want uniqueness by key-timestamp
    cosmo_measurements = {}
    cnv_hydro_measurements = {}

    # our final dataset containing correlated and non-correlated values from both datasets
    # is a list of RainfallIntervalDataEntry since who knows what timestamps will be available
    final_measurements = []

    # interval data localized to cosmo sites
    target_destination_db = cosmo_site_name

    config = DBConfigFactory.build(db_config_filename)

    # TODO: check exception block messages and flow
    try:
        with (connect(
                host=config[DBConfig.CONFIG_HOST],
                port=int(config[DBConfig.CONFIG_PORT]),
                user=config[DBConfig.CONFIG_USER],
                password=config[DBConfig.CONFIG_PASS]
        ) as connection):
            config[DBConfig.CONFIG_PASS] = None

            try:
                with connection.cursor() as cursor:
                    correlation_processing_start_time = timeit.default_timer()

                    cosmo_measurements_query_template = Template(
                        open("sql/get-cosmo-measurements.sql.template").read()
                    )

                    cnv_hydrometric_measurements_query_template = Template(
                        open("sql/get-cnv-hydrometric-measurements.sql.template").read()
                    )

                    # get cosmo date range
                    (cosmo_start_time,
                     cosmo_end_time) = get_cosmo_measurements_date_window(cursor, cosmo_site_name)

                    # get cnv hydro date range
                    # might not need this if cosmo is our primary dataset. we might not care if there are no
                    # cnv hydro measurements for a time period as there's nothing to correlate
                    (cnv_hydro_start_time,
                     cnv_hydro_end_time) = get_cnv_hydrometric_measurements_date_window(cursor, cnv_hydro_site_name)

                    # cosmo is our "primary" dataset used in this correlation
                    cosmo_date_inc = cosmo_start_time

                    correlated_measurement_count = 0

                    # cosmo and other datasets can be pretty large. step through the table in time blocks
                    while cosmo_date_inc <= cosmo_end_time:

                        # determine window and build queries
                        # need full date for cosmo
                        # need full date for cnv hydro
                        cosmo_block_start_datetime = cosmo_date_inc
                        cosmo_block_end_datetime = (
                                cosmo_date_inc + datetime.timedelta(seconds=COSMO_SCAN_INCREMENT))

                        # retrieve the cnv hydrometric data for the time of the cosmo block, +/- CORRELATION_WINDOW
                        cnv_hydro_block_start_datetime = cosmo_block_start_datetime - datetime.timedelta(
                            seconds=CNV_HYDROMETRIC_CORRELATION_WINDOW)
                        cnv_hydro_block_end_datetime = cosmo_block_end_datetime + datetime.timedelta(
                            seconds=CNV_HYDROMETRIC_CORRELATION_WINDOW)

                        cosmo_block_measurements_sql = cosmo_measurements_query_template.substitute(
                            DB=SOURCE_DB_NSSK_COSMO,
                            COSMO_SITE=cosmo_site_name,
                            COSMO_START_DATETIME=cosmo_block_start_datetime,
                            COSMO_END_DATETIME=cosmo_block_end_datetime
                        )

                        cnv_hydrometric_block_measurements_sql = cnv_hydrometric_measurements_query_template.substitute(
                            CNV_HYDROMETRIC_SITE=cnv_hydro_site_name,
                            CNV_HYDROMETRIC_START_DATETIME=cnv_hydro_block_start_datetime,
                            CNV_HYDROMETRIC_END_DATETIME=cnv_hydro_block_end_datetime
                        )

                        ###########
                        # get cosmo measurements for block
                        # use a dict of measurement timestamps => DataEntry. we want quick determination of uniqueness
                        if TRACE_LOGGING:
                            logger.debug("Starting scan of cosmo block %s -> %s" % (cosmo_block_start_datetime,
                                                                              cosmo_block_end_datetime))

                        cosmo_block_measurements = {}
                        logger.debug("Retrieving cosmo measurements with query:\n%s", cosmo_block_measurements_sql)

                        cursor.execute(cosmo_block_measurements_sql)

                        # TODO: conductivity coming back None sometimes.
                        # throw out measurement entirely?
                        # throw out only if temperature water not defined?
                        # ==> handle Nones robustly with both.
                        row = cursor.fetchone()
                        while row is not None:
                            # query should return schema CosmoTimestamp, Conductivity, TemperatureWater
                            measurement_timestamp = row[0]
                            data_entry = {
                                RainfallIntervalDataEntry.COSMO_TIMESTAMP_FIELD: measurement_timestamp,
                                RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD: row[1],
                                RainfallIntervalDataEntry.COSMO_TEMPERATURE_WATER_FIELD: row[2]
                            }
                            try:
                                new_entry = RainfallIntervalDataEntry(data_entry)

                                new_entry.set_db_destination(target_destination_db)

                                cosmo_block_measurements[measurement_timestamp] = new_entry
                            except DataValidationException as e:
                                logger.error(
                                    "Validation error building DataEntry from cosmo measurement (%s) checking databases. Discarding" % data_entry,
                                    exc_info=True)

                            # grab next row
                            row = cursor.fetchone()

                        logger.debug("Retrieving cosmo measurements completed.")

                        ###########
                        # get cnv hydro measurement for block +/- correlation window
                        cnv_hydrometric_block_measurements = {}

                        if TRACE_LOGGING:
                            logger.debug("Ending scan of cnv hydrometric block %s -> %s" % (cnv_hydro_block_start_datetime,
                                                                              cnv_hydro_block_end_datetime))

                        logger.debug("Retrieving cnv hydrometric measurements with query:\n%s",
                                     cnv_hydrometric_block_measurements_sql)

                        # reset for the next query
                        cursor.reset()

                        cursor.execute(cnv_hydrometric_block_measurements_sql)

                        row = cursor.fetchone()
                        while row is not None:
                            # query should return schema CNVHydrometricTimestamp, RevisedFinalStage
                            measurement_timestamp = row[0]
                            data_entry = {
                                RainfallIntervalDataEntry.CNV_HYDROMETRIC_TIMESTAMP_FIELD: measurement_timestamp,
                                RainfallIntervalDataEntry.CNV_HYDROMETRIC_REVISED_STAGE_FIELD: row[1]
                            }
                            try:

                                new_entry = RainfallIntervalDataEntry(data_entry)

                                # use the cnv hydro site name associated with the cosmo site name
                                # only one cnv hydro site now so hardcode it
                                new_entry.set_db_destination(target_destination_db)

                                cnv_hydrometric_block_measurements[measurement_timestamp] = new_entry

                                # TODO: log something here?

                            except DataValidationException as e:
                                logger.error(
                                    "Validation error building DataEntry from cnv hydrometric measurement (%s) checking databases. Discarding" % data_entry,
                                    exc_info=True)

                            # grab next row
                            row = cursor.fetchone()

                        if TRACE_LOGGING:
                            logger.debug("Starting scan of cnv hydrometric block %s -> %s" % (cnv_hydro_block_start_datetime,
                                                                              cnv_hydro_block_end_datetime))

                        logger.debug("Retrieving cnv hydrometric measurements completed.")

                        ##############
                        # correlate measurements from cosmo_block_measurements and cnv_hydrometric_block_measurements
                        #
                        # needs to work if either dataset is empty

                        # for each cosmo measurement
                        # check if there's a corresponding cnv hydro measurement (within +/- 2 minutes, closest)
                        # scan cnv_hydrometric_block_measurements for first timestamp within correlation window
                        #
                        # yes => add composite measurement to measurements[]
                        # and remove both cosmo and cnv hydrometric measurements from respective datasets

                        # list.del(i) is a o(n) operation so it may not be wise to prematurely optimize

                        # iterate over new list of keys since we have to modify the underlying dict
                        # specifically we are removing correlated measurements from their source datasets

                        if TRACE_LOGGING:
                            logger.debug("cosmo block rows: %d" % len(cosmo_block_measurements))
                            logger.debug("cnv hydrometric block rows: %d" % len(cnv_hydrometric_block_measurements))

                        for cosmo_measurement_timestamp in list(cosmo_block_measurements.keys()):

                            for cnv_hydrometric_timestamp in list(cnv_hydrometric_block_measurements.keys()):
                                if abs((cosmo_measurement_timestamp - cnv_hydrometric_timestamp).total_seconds()) < CNV_HYDROMETRIC_CORRELATION_WINDOW:
                                    # correlated timestamp
                                    # set cnv hydro values in cosmo measurement
                                    logger.debug(
                                        "Correlating cosmo measurement at %s with cnv hydrometric measurement at %s" %
                                        (
                                            cosmo_measurement_timestamp,
                                            cnv_hydrometric_timestamp
                                        ))

                                    cosmo_measurement = cosmo_block_measurements[cosmo_measurement_timestamp]
                                    logger.debug("Correlated cosmo measurement:\n%s" % cosmo_measurement.to_s())

                                    cnv_hydrometric_measurement = cnv_hydrometric_block_measurements[
                                        cnv_hydrometric_timestamp
                                    ]
                                    logger.debug(
                                        "Correlated cnv hydrometric measurement:\n%s" % cnv_hydrometric_measurement.to_s())

                                    # create new composite measurement
                                    # any cosmo measurement could be missing, or any set that has multiple measurements
                                    composite_measurement = RainfallIntervalDataEntry({
                                        RainfallIntervalDataEntry.COSMO_TIMESTAMP_FIELD: cosmo_measurement_timestamp,
                                        RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD: cosmo_measurement.get_cosmo_conductivity(),
                                        RainfallIntervalDataEntry.COSMO_TEMPERATURE_WATER_FIELD: cosmo_measurement.get_cosmo_temperature_water(),
                                        RainfallIntervalDataEntry.CNV_HYDROMETRIC_TIMESTAMP_FIELD: cnv_hydrometric_timestamp,
                                        RainfallIntervalDataEntry.CNV_HYDROMETRIC_REVISED_STAGE_FIELD: cnv_hydrometric_measurement.get_cnv_hydrometric_revised_stage(),
                                    })

                                    composite_measurement.set_db_destination(target_destination_db)

                                    correlated_measurements.append(composite_measurement)

                                    # delete source cosmo measurement
                                    del cosmo_block_measurements[cosmo_measurement_timestamp]

                                    # delete source cnv hydrometric measurement
                                    del cnv_hydrometric_block_measurements[cnv_hydrometric_timestamp]

                                    correlated_measurement_count += 1
                                    print("\r\tCorrelations processed: %d" % correlated_measurement_count, end='',
                                          flush=True)

                                    # we found a correlated measurement. we only want the first, and don't care about
                                    # the remaining cnv hydro measurements
                                    break
                                # else keep searching for a correlating cnv hydrometric measurement

                            # here there is no correlation of a cosmo measurement to a cnv hydrometric measurement
                            pass

                        # here the measurement block has finished processing

                        # add remaining cosmo measurements to cosmo_measurements{}
                        cosmo_measurements.update(cosmo_block_measurements)

                        # add remaining cnv hydro measurements to cnv_hydro_measurements{}
                        cnv_hydro_measurements.update(cnv_hydrometric_block_measurements)

                        if TRACE_LOGGING:
                            logger.debug("Ending scan of cosmo block %s -> %s" % (cosmo_block_start_datetime,
                                                                              cosmo_block_end_datetime))

                        # increment source block start time
                        # ensure overlapping time windows are managed by query, and here
                        # query should be date_field >= start_date and date_field < end_date
                        cosmo_date_inc = cosmo_date_inc + datetime.timedelta(seconds=COSMO_SCAN_INCREMENT)

                    print("\nFinished processing cosmo and cnv hydrometric measurements. Consolidating...")
                    # all blocks are finished processing

                    # print out of how many measurements we found
                    log_msg = "Total cosmo measurement count: %d" % len(cosmo_measurements.values())
                    print("%s" % log_msg, flush=True)
                    logger.info(log_msg)

                    log_msg = "Total cnv hydrometric measurement count: %d" % len(cnv_hydro_measurements.values())
                    print("%s" % log_msg, flush=True)
                    logger.info(log_msg)

                    log_msg = "Total correlated measurement count: %d" % len(correlated_measurements)
                    print("%s" % log_msg, flush=True)
                    logger.info(log_msg)

                    ##################################
                    # consolidate measurements from our component measurements
                    # cosmo_measurements, cnv_hydro_measurements, correlated_measurements
                    final_measurements.extend(cosmo_measurements.values())
                    final_measurements.extend(cnv_hydro_measurements.values())
                    final_measurements.extend(correlated_measurements)

                    correlation_processing_elapsed_time = (timeit.default_timer() - correlation_processing_start_time)
                    log_msg = "Completed correlation Processing in %.3f sec" % correlation_processing_elapsed_time
                    print("\n%s" % log_msg, flush=True)
                    logger.info(log_msg)

            except Error as e:
                logger.error("Error checking databases", exc_info=True)
                raise e
    except Error as e:
        logger.error("Error connecting to database", exc_info=True)
        raise e

    return final_measurements


def correlate_rainfall_data(prelim_measurements, cosmo_site, cnv_flowworks_site, db_config_filename
) -> list[RainfallIntervalDataEntry]:
    """
    Assemble 10-minute rainfall intervals from the 5-minute measurements, and attempt to correlate them with the existing
    cosmo/cnv hydrometric measurements.
    :param prelim_measurements: sorted list of preliminary measurements containing cosmo and cnv hydrometric measurements
    :param cosmo_site:  cosmo site that prelim_measurements applies to
    :param cnv_flowworks_site:  cnv flowworks site to consider rainfall measurements from
    :param db_config_filename:  database config file to use to connect to our database
    :return:
    """
    # the aggregate list of all processed measurements, correlated with rainfall intervals or not
    final_measurements: list[RainfallIntervalDataEntry] = []

    # measurements correlated with rainfall intervals
    correlated_rainfall_measurements: list[RainfallIntervalDataEntry] = []

    # measurements correlated with rainfall intervals
    uncorrelated_rainfall_measurements: list[RainfallIntervalDataEntry] = []

    config = DBConfigFactory.build(db_config_filename)

    # TODO: check exception block messages and flow
    try:
        with (connect(
                host=config[DBConfig.CONFIG_HOST],
                port=int(config[DBConfig.CONFIG_PORT]),
                user=config[DBConfig.CONFIG_USER],
                password=config[DBConfig.CONFIG_PASS]
        ) as connection):
            config[DBConfig.CONFIG_PASS] = None

            try:
                with connection.cursor() as cursor:
                    correlation_processing_start_time = timeit.default_timer()

                    cnv_flowworks_measurements_query_template = Template(
                        open("sql/get-cnv-flowworks-measurements.sql.template").read()
                    )

                    # get cnv flowworks date range for block
                    (cnv_flowworks_start_datetime,
                     cnv_flowworks_end_datetime) = get_cnv_flowworks_measurements_date_window(cursor, cnv_flowworks_site)

                    # window through rainfall data, find the best 2 rainfall measurements that correlate with known
                    # cosmo/cnv hydrometric measurements
                    # for each rainfall measurement, find a companion measurement

                    # generally, if rainfall measurements are not correlatable with any cosmo/cnv hydro measurement,
                    # then add a new measurement for the 10-minute rainfall data
                    # not so simple though

                    # not guaranteed that successive flowworks rainfall measurements are 5 minutes apart

                    # which is our outer dataset?
                    # !! cnv flowworks -> consider each rainfall measurement form a 10-minute rainfall value if the next element is 5 mins later
                    # if a correlated measurement is close enough, add the rainfall data to the existing correlated value
                    # otherwise add new entry
                    # if a 10-minute value can't be created, discard current value and continue
                    #
                    # correlated values -> find rainfall values that are close to known correlated measurements, then add everything that doesn't match
                    # no, ugly, hard to jump around in flowworks dataset when we query in batches

                    # iterate through flowworks measurement blocks, and make determinations on what to do with the
                    # current measurement
                    #
                    # control what to do with the current measurement
                    #
                    # check that the next measurement within 5 minutes (not exactly) in the future
                    # advance if not
                    # if so, then we have 2 rainfall measurements 5 minutes apart
                    # check if there's a measurement in the correlation set within the correlation threshold
                    # correlation check done with the median of the 2 rainfall measurement timestamps

                    # what if there are more than 2 rainfall measurements within a 10-minute period?
                    # would make 5-minute rainfall amounts invalid

                    # flowworks dataset

                    # cnv flowworks is our "primary" dataset used in this correlation
                    cnv_flowworks_inc = cnv_flowworks_start_datetime

                    # running tally of how many 10-minute intervals we've collected
                    rainfall_measurement_count = 0

                    trailing_rainfall_measurement = None

                    # datasets can be pretty large. step through the table in time blocks
                    while cnv_flowworks_inc <= cnv_flowworks_end_datetime:

                        # determine window and build queries
                        cnv_flowworks_block_start_datetime = cnv_flowworks_inc
                        cnv_flowworks_block_end_datetime = (
                                cnv_flowworks_inc + datetime.timedelta(seconds=CNV_FLOWWORKS_SCAN_INCREMENT))

                        cnv_flowworks_block_measurements_sql = cnv_flowworks_measurements_query_template.substitute(
                            DB=SOURCE_DB_CNV_FLOWWORKS,
                            SITE=cnv_flowworks_site,
                            CNV_FLOWWORKS_START_DATETIME=cnv_flowworks_block_start_datetime,
                            CNV_FLOWWORKS_END_DATETIME=cnv_flowworks_block_end_datetime
                        )

                        logger.debug("Starting scan of cnv flowworks %s block %s => %s" % (
                            cosmo_site,
                            cnv_flowworks_block_start_datetime,
                            cnv_flowworks_block_end_datetime)
                        )

                        if TRACE_LOGGING:
                            logger.debug("Retrieving CNV Flowworks measurement block with query:\n%s",
                                         cnv_flowworks_block_measurements_sql)

                        ###########
                        # retrieve and process cnv flowworks measurements for block

                        # how do we tack an orphaned last element onto the next processing block?
                        # => maintain a trailing_rainfall_measurement reference

                        cursor.execute(cnv_flowworks_block_measurements_sql)

                        # for reasonable amounts (< a few weeks) of CNV_FLOWWORKS_SCAN_INCREMENT, this won't be large.
                        # fetch_one has no peek, which we need
                        # with fetchall we can jump around more easily
                        rows = cursor.fetchall()

                        block_measurements = get_block_prelim_measurements(
                            prelim_measurements,
                            cnv_flowworks_block_start_datetime,
                            cnv_flowworks_block_end_datetime
                        )

                        # count the initial block size, we'll be removing correlated elements from it in
                        # add_rainfall_interval_measurement and want to compare sizes later
                        initial_block_measurement_count = len(block_measurements)

                        # with our block of rainfall measurements from the sql query, find sets of two measurements
                        # that are 5 minutes apart, and attempt to correlate with existing correlation of cosmo/cnvhydro
                        # measurements
                        row_count = len(rows)
                        for row_i in range(row_count):

                            row = rows[row_i]

                            first_rainfall_measurement = None
                            second_rainfall_measurement = None

                            # row[0]: measurement_timestamp
                            # row[1]: 5-minute rainfall amount

                            # the last iteration may have ended on a value we need to consider
                            if trailing_rainfall_measurement is not None:
                                if TRACE_LOGGING:
                                    logger.debug("Found trailing_rainfall_measurement: %s",
                                                 trailing_rainfall_measurement)

                                # trailing_rainfall_measurement[0]: measurement_timestamp
                                # trailing_rainfall_measurement[1]: 5-minute rainfall amount

                                # consider this the first measurement of a potential pair with row[i]
                                # rather than row[i] and row[i+1]

                                # sanity check trailing_rainfall_measurement
                                # cant check field count since a rainfall measurement is more than just the rainfall value
                                if isinstance(trailing_rainfall_measurement, (list, tuple)):
                                    # valid trailing_rainfall_measurement, check that so we haven't moved the window correctly
                                    if row[0] == trailing_rainfall_measurement[0]:
                                        raise Exception(
                                            "Current measurement and trailing measurement have the same timestamp: %s" % trailing_rainfall_measurement)
                                    else:
                                        # okay - trailing_rainfall_measurement valid, and not the same
                                        # process below
                                        pass
                                else:
                                    # trailing_rainfall_measurement defined, but something isn't right
                                    raise Exception(
                                        "Trailing measurement malformed: %s" % trailing_rainfall_measurement)

                                # valid trailing measurement

                                # check if we can make a 10-minute interval out of measurement_i and trailing_rainfall_measurement
                                # is trailing_rainfall_measurement and row[0] 5 mins apart? account for some buffer
                                # yes => place in results, continue loop (jump to next iteration, None out trailing_rainfall_measurement)
                                # no => continue execution, try same with next element in rows

                                # if trailing_rainfall_measurement is valid, but too far away from row[i], we need to consider row[i] and row[i+1]

                                first_rainfall_measurement = list(trailing_rainfall_measurement)
                                second_rainfall_measurement = list(row)

                                # TODO: type/sanity check on what a valid measurement looks like?
                                # trust database to enforce this?

                                # unset trailing_rainfall_measurement now that we're done with it
                                trailing_rainfall_measurement = None
                            else:
                                # no trailing measurement

                                if row_i == row_count - 1:
                                    # is this the last element? cant make a 10-minute interval out of a single measurement
                                    # set our trailing_rainfall_measurement and consider it with the next measurement block
                                    trailing_rainfall_measurement = list(row)

                                    if TRACE_LOGGING:
                                        logger.debug("Setting trailing_rainfall_measurement for next block: %s => %s" %
                                            (trailing_rainfall_measurement[0].strftime('%Y-%m-%d %H:%M:%S'),
                                            trailing_rainfall_measurement)
                                        )

                                    # count this row as processed
                                    row_i += 1

                                    # should prompt retrieval of next block
                                    continue
                                else:
                                    # no trailing measurement to consider
                                    # not the last measurement in the block
                                    first_rainfall_measurement = list(row)
                                    second_rainfall_measurement = list(rows[row_i + 1])

                                    # count this row as processed
                                    # expect an additional increment before the end of the loop to account for the second
                                    # measurement retrieved above
                                    row_i += 1

                            #####################

                            # we're at first_rainfall_measurement, is second_rainfall_measurement 5 mins later (plus or minus buffer)?
                            # difference in seconds
                            measurement_time_difference = abs(
                                (first_rainfall_measurement[0] - second_rainfall_measurement[0]).total_seconds()
                            )
                            bounds_max = RAINFALL_INTERVAL_LEN + RAINFALL_INTERVAL_BUFFER
                            bounds_min = RAINFALL_INTERVAL_LEN - RAINFALL_INTERVAL_BUFFER
                            if bounds_max > measurement_time_difference > bounds_min:
                                # can make an interval

                                if TRACE_LOGGING:
                                    logger.debug(
                                        "Found first/second rainfall measurements comprise a 10-minute interval.\n" +
                                        "first: %s => %s\nsecond: %s => %s" % (
                                            first_rainfall_measurement[0].strftime(MYSQL_DATE_FMT), first_rainfall_measurement,
                                            second_rainfall_measurement[0].strftime(MYSQL_DATE_FMT), second_rainfall_measurement)
                                    )

                                # see if we have a cosmo/hydro measurement within our interval
                                # use baro pressure and air temperature of the first measurement. don't expect these to
                                # meaningfully change over 10 minutes. we can always take the average if need be.
                                add_rainfall_interval_measurement(
                                    first_rainfall_measurement,
                                    second_rainfall_measurement,
                                    cosmo_site,
                                    block_measurements,
                                    correlated_rainfall_measurements
                                )

                                # yes => insert the rainfall data into the existing RainfallIntervalDataEntry
                                # no => add a new RainfallIntervalDataEntry with just the rainfall data

                                # count the measurement, regardless if it correlates
                                rainfall_measurement_count += 1
                                print("\r\t10-minute Rainfall measurements processed: %d" % rainfall_measurement_count,
                                      end='',
                                      flush=True)
                            else:
                                if TRACE_LOGGING:
                                    logger.debug(
                                        "Found first/second measurements do not comprise a 10-minute interval.\n" +
                                        "first: %s => %s\nsecond: %s => %s" % (
                                            first_rainfall_measurement[0].strftime(MYSQL_DATE_FMT), first_rainfall_measurement,
                                            second_rainfall_measurement[0].strftime(MYSQL_DATE_FMT), second_rainfall_measurement)
                                    )
                                # cannot make an interval. do nothing this iteration.

                                # consider the second_rainfall_measurement as the first element next as the trailing_rainfall_measurement
                                trailing_rainfall_measurement = list(second_rainfall_measurement)

                            # increment the row index
                            row_i += 1

                        # shouldn't need to call cursor.reset() since we're fetching everything from the cursor

                        if TRACE_LOGGING:
                            logger.debug("Ending scan of cnv flowworks %s block %s => %s" %
                                (cosmo_site, cnv_flowworks_block_start_datetime, cnv_flowworks_block_end_datetime))

                        # add uncorrelated measurements in this block to the running list
                        # comparison of original block size to the remaining measurements in the block
                        if TRACE_LOGGING:
                            logger.debug(
                                "Adding %d measurements uncorrelated with rainfall from initial block size %d" %
                                (len(block_measurements), initial_block_measurement_count)
                            )

                        uncorrelated_rainfall_measurements.extend(block_measurements)

                        # increment source block start time
                        # ensure overlapping time windows are managed by query, and here
                        # query should be date_field >= start_date and date_field < end_date
                        # existence of a trailing measurement should not affect this, so long as the mysql query is
                        # exclusive of the block end datetime
                        cnv_flowworks_inc = cnv_flowworks_inc + datetime.timedelta(seconds=CNV_FLOWWORKS_SCAN_INCREMENT)

                    correlation_processing_elapsed_time = (timeit.default_timer() - correlation_processing_start_time)
                    log_msg = "Completed rainfall correlation Processing in %.3f sec" % correlation_processing_elapsed_time
                    print("\n%s" % log_msg, flush=True)
                    logger.info(log_msg)
            except Error as e:
                logger.error("Error checking databases", exc_info=True)
                raise e
    except Error as e:
        logger.error("Error connecting to database", exc_info=True)
        raise e

    ##########################
    # form our set of correlated and uncorrelated measurements

    # add measurements that do not correlate with 10-minute rainfall intervals to the final list
    final_measurements.extend(uncorrelated_rainfall_measurements)

    # add measurements that do correlate with 10-minute rainfall intervals to the final list
    for new_measurement in correlated_rainfall_measurements:
        if TRACE_LOGGING:
            logger.debug("Adding new correlated measurement: %s", new_measurement.to_s())
        new_measurement.set_db_destination(cosmo_site)
        final_measurements.append(new_measurement)

    return final_measurements


def add_rainfall_interval_measurement(
        first_rainfall_measurement: list,
        second_rainfall_measurement: list,
        site: str,
        measurements: list[RainfallIntervalDataEntry],
        new_measurements: list[RainfallIntervalDataEntry]
) -> None:
    """
    We have a pair of rainfall measurements, compute the 10minute rainfall total, and add to the known measurements.
    Consider air temperature and baro pressure from the first measurement. These don't typically change much over
    10 minutes. Can always take the mean if need be.

    first_measurement and second_measurement must comprise a pair consecutive 5-minute rainfall measurements.

    :param site:
    :param first_rainfall_measurement: database row data comprising the first rainfall measurement
    :param second_rainfall_measurement: database row data comprising the second rainfall measurement
    :param measurements: List of correlated cosmo/cnvhydro measurements to attempt to correlate rainfall measurements
    :param new_measurements: List of newly correlated rainfall measurements
    """

    # TODO: what's the performance impact if every call of this function results in a correlation?
    # can we constrain measurements at all?
    # this function does a full scan of measurements at worse case with each call.
    # can we do a single scan of it once to limit it to the block time interval?
    # if there's 500k measurements before rainfall data, then we have to skip those each invocation.
    # same if they're at the end

    # sanity check if first measurement is before second measurement. swap otherwise.
    if first_rainfall_measurement > second_rainfall_measurement:
        temp = second_rainfall_measurement
        second_rainfall_measurement = first_rainfall_measurement
        first_rainfall_measurement = temp

    # the earlier rainfall timestamp is considered the middle of the interval
    # a rainfall measurement is the rainfall amount recorded in the previous 5 minutes
    target_timestamp = first_rainfall_measurement[0]

    # we're adding the rainfall amount to something, so record it outside the loop
    interval_rainfall_amt = float(first_rainfall_measurement[1]) + float(second_rainfall_measurement[1])

    # search the measurements list for a measurement
    measurement_i = 0
    for measurement in measurements:

        measurement_timestamp = measurement.get_timestamp()

        # TODO: robustly constrain what timestamp we're correlating

        # is the measurement timestamp close enough in time to the rainfall timestamp?
        if abs((measurement_timestamp - target_timestamp).total_seconds()) < RAINFALL_CORRELATION_THRESHOLD:
            # we can correlate this measurement with our rainfall data

            if TRACE_LOGGING:
                logger.debug(
                    "Rainfall measurement at %s correlates to cosmo/cnvhydro measurement at %s for site %s:\nfirst: %s\nsecond: %s" %
                    (target_timestamp, measurement_timestamp, site, first_rainfall_measurement, second_rainfall_measurement))

            # create a new copy to modify
            new_measurement = measurement.copy()

            # set the fields in our new correlated measurement
            # schema guarantees rainfall, airtemp, baro pressure
            # rainfall is summed, air temp and baro pressure taken from first measurement as they don't change much over
            # 5 minutes
            new_measurement.set(RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_START_TIMESTAMP_FIELD,
                                first_rainfall_measurement[0])
            new_measurement.set(RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_END_TIMESTAMP_FIELD,
                                second_rainfall_measurement[0])
            new_measurement.set(RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_AMT_FIELD, interval_rainfall_amt)
            new_measurement.set(RainfallIntervalDataEntry.CNV_FLOWWORKS_BARO_PRESSURE_FIELD,
                                first_rainfall_measurement[2])
            new_measurement.set(RainfallIntervalDataEntry.CNV_FLOWWORKS_AIR_TEMPERATURE_FIELD,
                                first_rainfall_measurement[3])


            if TRACE_LOGGING:
                logger.debug("Adding new correlated measurement for site %s:\n%s" % (site, new_measurement.to_s()))
            # add the new measurement to the list of new measurements
            new_measurements.append(new_measurement)

            # delete the original measurement
            # only expect a single correlation, and to cut down on search space
            del measurements[measurement_i]

            return
        elif measurement_timestamp > target_timestamp:
            if TRACE_LOGGING:
                logger.debug(
                    "Rainfall measurement at %s does not correlate to cosmo/cnvhydro measurement for site %s:\nfirst: %s\nsecond: %s" %
                    (target_timestamp, site, first_rainfall_measurement, second_rainfall_measurement))

            # we're past the target timestamp of a sorted list, then there's no correlation
            # use a break so we add a new measurement if measurements is empty
            break
        else:
            # continue the search
            pass

        # else keep searching for a potential correlation
        measurement_i += 1

    #####
    # add a new measurement of just the rainfall data
    new_measurements.append(
        RainfallIntervalDataEntry({
            RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_START_TIMESTAMP_FIELD: first_rainfall_measurement[0],
            RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_END_TIMESTAMP_FIELD: second_rainfall_measurement[0],
            RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_AMT_FIELD: interval_rainfall_amt,
            RainfallIntervalDataEntry.CNV_FLOWWORKS_BARO_PRESSURE_FIELD: first_rainfall_measurement[2],
            RainfallIntervalDataEntry.CNV_FLOWWORKS_AIR_TEMPERATURE_FIELD: first_rainfall_measurement[3]
        })
    )

    return


def get_block_prelim_measurements(
        measurements: list[RainfallIntervalDataEntry],
        start_timestamp: datetime,
        end_timestamp: datetime) -> list[RainfallIntervalDataEntry]:
    """
    return the cosmo/cnv hydrometric measurements within the provided timestamps.
    assumes sort in ascending order by timestamp.

    :param measurements:
    :param start_timestamp:
    :param end_timestamp:
    """

    logger.debug("Determining block measurements for block (%s => %s)" % (start_timestamp, end_timestamp))

    # is end before start? then swap
    if end_timestamp < start_timestamp:
        logger.warning("Found block end_timestamp earlier than start_timestamp (%s vs %s). Swapping." % (end_timestamp, start_timestamp) )
        temp = end_timestamp
        end_timestamp = start_timestamp
        start_timestamp = temp

    block_measurements: list[RainfallIntervalDataEntry] = []

    block_measurement_count = 0
    for measurement in measurements:
        if start_timestamp <= measurement.get_timestamp() <= end_timestamp:
            block_measurements.append(measurement)
            block_measurement_count += 1

    if TRACE_LOGGING:
        logger.debug("Found %d preliminary measurements in block %s => %s" %
            (
                block_measurement_count,
                start_timestamp,
                end_timestamp
            )
        )

    return block_measurements


def collect_interval_data(db_config_filename, db_importer):
    # different from correlation, as we're keeping cosmo and cnv hydrometric measurements that do not correlate
    # also are not narrowing to the time window where measurements from both datasets are present
    #
    # use a collect_cosmo_and_cnvhydro_measurements method that correlates measurements, but also stores any
    # uncorrelatable cosmo measurements and cnv hydrometric measurements
    #
    # next window through rainfall and correlate 10-minute rainfall measurements for anything with a measurement
    # finally, determine rainfall amounts that do not correlate with measurements

    # cosmo sites may be bound to different cnv hydrometric and cnv flowworks sites in the future

    # for each cosmo conductivity and temperature measurement, determine if there are correlatable cnv hydrometric
    # revised flow, and cnv flowworks rainfall measurements
    # add the two best ones together

    ############################3
    # multiple cosmo sites, one flowworks site, one hydrometrics.

    measurements = {}

    # this is not a simple correlation.
    # we till want uncorrelateable measurements in the final dataset
    # some measurements will correlate with measurements from one other dataset but not all
    # in any combination

    # sort function for a list of RainfallIntervalDataEntry for our specific purposes
    # TODO: maybe fold this into DataEntry later on. decide on a default sort order:
    # TODO: how do we sort a collection of DataEntry depending on the subclass implementation?
    def sort_prelim_measurements(obj: RainfallIntervalDataEntry):
        if not isinstance(obj, RainfallIntervalDataEntry):
            raise Exception(
                "Found something that isn't RainfallIntervalDataEntry in measurement list when attempting to sort")

        cosmo_timestamp = obj.get_cosmo_timestamp()
        cnv_hydrometric_timestamp = obj.get_cnv_hydrometric_timestamp()

        # by cosmo date if that's the only measurement
        # by cosmo date if both measurements exist
        # by cnv hydro date if that's the only measurement
        # else error log
        if cosmo_timestamp is None:
            if cnv_hydrometric_timestamp is not None:
                return cnv_hydrometric_timestamp
            else:
                # neither defined -> invalid
                pass
        else:
            # both cosmo and cnv_hydro measurements, go with cosmo
            # though since this is a correlated measurement, the timestamps will be close
            return cosmo_timestamp

        raise Exception("Measurement missing timestamp and cannot be sorted:\n%s" % obj.to_s())

    # cosmo site is our primary dataset
    #
    # for multiple cosmo sites using the same cnv hydro site, the cnv hydro site is retrieved and stepped through
    # multiple times. this is preferable to caching the cnv hydro measurements (which can be very large) in memory
    for cosmo_site in COSMO_SITES:

        print("Retrieving measurements for cosmo_site %s and associated hydrometrics data" % cosmo_site)

        # returns a partially sorted array of measurements
        # sql queries typically return results ordered by date in ASC order, and windowing is done from old to new
        # essentially a flat array of sorted cosmo-only, sorted cnv-hydro-only, and sorted composite
        # need everything sorted by which date fields are present

        prelim_measurements = collect_cosmo_and_cnvhydro_measurements(cosmo_site, SOURCE_DB_CNV_HYDROMETRICS_SITE,
                                                                      db_config_filename)

        print("Sorting measurements for cosmo_site %s" % cosmo_site)

        sorting_start_time = timeit.default_timer()

        # sort dataset in-place by measurement timestamp for easier correlation with rainfall values
        prelim_measurements.sort(key=sort_prelim_measurements)

        sorting_elapsed_time = (timeit.default_timer() - sorting_start_time)
        print("Sorted preliminary measurements completed in %.3f sec" % sorting_elapsed_time)

        # debugging
        # print("Sorted measurements:")
        # for measurement in prelim_measurements:
        #     print ("%s\n------" % measurement.to_s())

        #################
        # correlate 10-minute rainfall amounts with accumulated measurements
        final_measurements = correlate_rainfall_data(prelim_measurements, cosmo_site, SOURCE_DB_CNV_FLOWWORKS_SITE,
                                                     db_config_filename)

        log_msg = "Adding %s site measurements to importer..." % cosmo_site
        print("%s" % log_msg)
        logger.info(log_msg)

        #################################################
        # add measurements to importer
        for measurement in final_measurements:
            if TRACE_LOGGING:
                logger.debug("Adding measurement:\n%s" % measurement.to_s())
            db_importer.add(measurement)

        log_msg = "Processing %s site completed" % cosmo_site
        print("%s" % log_msg)
        logger.info(log_msg)


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
    db_importer.set_importer_name("rainfall-interval-data")
    db_importer.set_schema(SCHEMA)

    print("Running rainfall interval data collection")
    collect_interval_data(db_config_filename, db_importer)

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
    # --dry-run                          read data dump file and output sql statements.
    # -cfg interval-data.json            database config     not required
    ############################

    # reads sys.argv
    parser = argparse.ArgumentParser(
        description='Compile rainfall interval data')
    parser.add_argument('--dry-run', action='store_const', const=1, dest='dryrun',
                        help='Output database insert statements. Does not write to database.')
    parser.add_argument('-cfg', nargs=1, dest='db_cfg_file',
                        help='Database config file in json format. Ex: interval.json')

    # call main with parsed args
    main(parser.parse_args())
