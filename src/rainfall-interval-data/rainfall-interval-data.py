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

# increment to step through measurements in a COSMO table
# shouldn't be too big
# 7 days
COSMO_SCAN_INCREMENT = 60 * 60 * 24 * 7

CNV_FLOWWORKS_SCAN_INCREMENT = 60 * 60 * 24 * 7

CNV_HYDROMETRIC_SCAN_INCREMENT = 60 * 60 * 24 * 7

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
                        cursor.execute("SELECT EXISTS(SELECT 1 FROM %s.%s LIMIT 1) AS is_not_empty;" % (target_db, sensor))
                        if cursor.fetchone()[0]:
                            # TODO: we have write access to insert, should also be able to drop. for now just throw an exception
                            # add a -f flag to shell args, if not present, prompt for user confirmation to proceed with drop
                            raise PrecheckFailedException("Destination table %s.%s is not empty." % (target_db, sensor))
                        else:
                            logger.debug("Destination table %s.%s is empty. Proceeding." % (target_db, sensor))

                    # target database validated. cache database name, so we can reference when running the correlation
                    # TODO: necessary?
                    global TARGET_DATABASE
                    TARGET_DATABASE = target_db

            except Error as e:
                logger.error("Error checking databases", e)
    except Error as e:
        logger.error("Error connecting to database", e)


# return start and end dates for both cosmo conductivity measurements, and cnv rainfall measurements
def get_cosmo_measurements_date_window(cursor, sensor_name):
    ###################
    # determine cosmo start datetime (earliest conductivity measurement)
    # cosmo datetimes are split across date and time fields

    # TODO move this and other template reads to precheck
    cosmo_date_search_template = Template(open("sql/get-cosmo-timestamp.sql.template").read())
    cosmo_date_search_sql = cosmo_date_search_template.substitute(DB=SOURCE_DB_NSSK_COSMO, SITE=sensor_name,
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
            "Could not determine latest conductivity measurement timestamp for site %s in target database" % sensor_name
        )

    log_msg = "Determined latest conductivity measurement timestamp %s" % cosmo_end_time
    logger.info(log_msg)
    print(log_msg)

    return cosmo_start_time, cosmo_end_time


# return start and end dates for cnv rainfall measurements
def get_cnv_flowworks_measurements_date_window(cursor, sensor_name):
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


def collect_cosmo_and_cnvhydro_data(cosmo_site_name, cnv_hydro_site_name, db_config_filename):
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
                        cosmo_block_start_date = cosmo_date_inc.date()
                        cosmo_block_end_date = (
                                    cosmo_date_inc + datetime.timedelta(seconds=COSMO_SCAN_INCREMENT)).date()

                        # retrieve the cnv hydrometric data for the corresponding date range, +/- CORRELATION_WINDOW
                        cnv_hydro_block_start_time = cosmo_block_start_date - datetime.timedelta(
                            seconds=CNV_HYDROMETRIC_CORRELATION_WINDOW)
                        cnv_hydro_block_end_time = cosmo_block_end_date + datetime.timedelta(
                            seconds=CNV_HYDROMETRIC_CORRELATION_WINDOW)

                        cosmo_block_measurements_sql = cosmo_measurements_query_template.substitute(
                            DB=SOURCE_DB_NSSK_COSMO,
                            COSMO_SITE=cosmo_site_name,
                            COSMO_START_DATETIME=cosmo_block_start_date,
                            COSMO_END_DATETIME=cosmo_block_end_date
                        )

                        cnv_hydrometric_block_measurements_sql = cnv_hydrometric_measurements_query_template.substitute(
                            CNV_HYDROMETRIC_SITE=cnv_hydro_site_name,
                            CNV_HYDROMETRIC_START_DATETIME=cnv_hydro_block_start_time,
                            CNV_HYDROMETRIC_END_DATETIME=cnv_hydro_block_end_time
                        )

                        ###########
                        # get cosmo measurements for block
                        # use a dict of measurement timestamps => DataEntry. we want quick determination of uniqueness
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
                        logger.debug("Retrieving cnv hydrometric  measurements with query:\n%s",
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
                            except DataValidationException as e:
                                logger.error(
                                    "Validation error building DataEntry from cnv hydrometric measurement (%s) checking databases. Discarding" % data_entry,
                                    exc_info=True)

                            # grab next row
                            row = cursor.fetchone()

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

                        for cosmo_measurement_timestamp in list(cosmo_block_measurements.keys()):

                            for cnv_hydrometric_timestamp in list(cnv_hydrometric_block_measurements.keys()):
                                if abs((
                                               cosmo_measurement_timestamp - cnv_hydrometric_timestamp).total_seconds()) < CNV_HYDROMETRIC_CORRELATION_WINDOW:
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

                        # increment source block start time
                        # ensure overlapping time windows are managed by query, and here
                        # query should be date_field >= start_date and date_field < end_date
                        cosmo_date_inc = cosmo_date_inc + datetime.timedelta(seconds=COSMO_SCAN_INCREMENT)

                    print("\nFinished processing cosmo and cnv hydrometric measurements. Consolidating...")
                    # all blocks are finished processing

                    ##################################
                    # TODO: add any preceding or trailing cnv hydrometric data

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


def correlate_rainfall_data(prelim_measurements, cnv_flowworks_site, db_config_filename):
    final_measurements = []

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

    return final_measurements

def collect_interval_data(db_config_filename, db_importer):
    # different from correlation, as we're keeping cosmo and cnv hydrometric measurements that do not correlate
    # also are not narrowing to the time window where measurements from both datasets are present
    #
    # use a collect_cosmo_and_cnvhydro_measurements method that correlates measurements, but stores the uncorrelatable ones
    # next window through rainfall and correlate 10-minute rainfall measurements for anything with a measurement
    # finally, determine rainfall amounts that do not correlate with measurements

    # cosmo sites may be bound to different cnv hydrometric and cnv flowworks sites

    # determine start and end of cosmo data
    # determine start and end of rainfall data

    # get time-window slice of cosmo data
    # get time-window slice of rainfall data

    # for each cosmo conductivity and temperature measurement, determine if there are correlatable rainfall measurements
    # add the two best ones together
    # attempt to correlate revised flow

    ############################3
    # multiple cosmo sites, one flowworks site, one hydrometrics. dont redo flowworks/hydrometrics retrieval
    # how to store measurements to later access
    # dont want to hardcode
    # dict of site to list[DataEntry]?

    # sort the measurement DataEntrys by timestamp for better access later

    measurements = {}

    # retrieve once...or maybe not there could be many flowworks sites.
    # TODO: some mechanism of mapping and lazyloading these measurement sets
    # TODO: executive decision on keeping a large dataset in memory vs retrieving it for each site
    # could be many sites, or not
    # TODO: re-enable. right now skipping for testing correlation

    # this is not a simple correlation.
    # we till want uncorrelateable measurements in the final dataset
    # some measurements will correlate with measurements from one other dataset but not all
    # in any combination

    # sort function for a list of RainfallIntervalDataEntry for our specific purposes
    # TODO: maybe fold this into DataEntry later on. decide on a default sort order:
    # TODO: how do we sort a collection of DataEntry depending on the subclass implementation?
    def sort_measurements(obj: RainfallIntervalDataEntry):
        if not isinstance(obj, RainfallIntervalDataEntry):
            raise Exception("Found something that isn't RainfallIntervalDataEntry in measurement list when attempting to sort")

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

        # returns a partially sorted array of measurements
        # sql queries typically return results ordered by date in ASC order, and windowing is done from old to new
        # essentially a flat array of sorted cosmo-only, sorted cnv-hydro-only, and sorted composite
        # need everything sorted by which date fields are present

        # TODO: dict of mapping?? -> maybe it's fine as a list
        # measurements[cosmo_site] =
        prelim_measurements = collect_cosmo_and_cnvhydro_data(cosmo_site, SOURCE_DB_CNV_HYDROMETRICS_SITE,
                                                                   db_config_filename)

        print("Sorting measurements for cosmo_site %s" % cosmo_site)

        sorting_start_time = timeit.default_timer()

        # sort dataset in-place by measurement timestamp for easier correlation with rainfall values
        prelim_measurements.sort(key=sort_measurements)

        sorting_elapsed_time = (timeit.default_timer() - sorting_start_time)
        print("Sorted preliminary measurements completed in %.3f sec" % sorting_elapsed_time)

        # debugging
        # print("Sorted measurements:")
        # for measurement in prelim_measurements:
        #     print ("%s\n------" % measurement.to_s())

        #################
        # correlate 10-minute rainfall amounts with accumulated measurements
        final_measurements = correlate_rainfall_data(prelim_measurements, SOURCE_DB_CNV_FLOWWORKS_SITE, db_config_filename)


        #################################################
        # add measurements to importer
        for measurement in final_measurements:

            if TRACE_LOGGING:
                logger.debug("Adding measurement:\n%s" % measurement.to_s())
            db_importer.add(measurement)

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
