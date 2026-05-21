import argparse
import logging
import pprint
import sys
import timeit
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from string import Template

from mysql.connector import connect, Error

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
log = logging.getLogger(__name__)

log.setLevel(logging.DEBUG)
logging.getLogger("RainfallIntervalDataEntry").setLevel(logging.INFO)
logging.getLogger("DBImporter").setLevel(logging.INFO)

# warning: slows processing a lot since every database query is logged
TRACE_LOGGING = False

# set these for testing specific date intervals, as the full dataset takes a long time
OVERRIDE_START_DATETIME = None
OVERRIDE_END_DATETIME = None
#OVERRIDE_START_DATETIME = "2020-01-01 00:00:00"
#OVERRIDE_END_DATETIME = "2020-04-01 00:00:00"


# time window to search for a corresponding conductivity value
# +/- 2 minutes, 29 seconds. in seconds
# cnv hydrometrics measurements are every 5 minutes
# adjust with any changes to rainfall measurement frequency
CNV_HYDROMETRIC_CORRELATION_WINDOW = (2 * 60) + 29

# time window for two summed 5-min rainfall measurements, to be correlated with existing cosmo/cnv hydro measurements
# +/- 5 minutes. in seconds
CNV_FLOWWORKS_CORRELATION_WINDOW = 5 * 60

# increment to step through measurements in a COSMO table
# shouldn't be too big
BLOCK_SCAN_WINDOW_DAYS = 35
COSMO_SCAN_INCREMENT = 60 * 60 * 24 * BLOCK_SCAN_WINDOW_DAYS

# increment to step through measurements in a cnv flowworks table
# shouldn't be too big
# 1 day for test value
CNV_FLOWWORKS_SCAN_INCREMENT = 60 * 60 * 24 * BLOCK_SCAN_WINDOW_DAYS

# increment to step through measurements in a cnv hydrometric table
# shouldn't be too big
# 7 days
CNV_HYDROMETRIC_SCAN_INCREMENT = 60 * 60 * 24 * BLOCK_SCAN_WINDOW_DAYS

# ideally how far apart rainfall measurements are apart timewise for consideration in 10-minute intervals
# in seconds
RAINFALL_INTERVAL_LEN = 5 * 60

# buffer to account for measurement drift in RAINFALL_INTERVAL_LEN
# in seconds
RAINFALL_INTERVAL_BUFFER = 60

# correlating rainfall measurements to cosmo/cnv hydro measurements
# in seconds
RAINFALL_CORRELATION_THRESHOLD = 45

# cached result size of querying time blocks for datasets
BLOCK_QUERY_BATCH_SIZE = 2000

############
# database info
SOURCE_DB_NSSK_COSMO = "NSSK_COSMO"
SOURCE_DB_CNV_HYDROMETRIC = "NSSK_CNV_HYDROMETRIC"
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

# TODO: use these and implement in other importers as warranted as a project-wide constant
ORDER_ASC = "ASC"
ORDER_DESC = "DESC"


def print_meas_correl_status(correlated_count, cosmo_count, cnv_hydro_count, start_datetime, end_datetime):
    """
    Print running status of measurement accumulation of CoSMo, CNV Hydrometric, and any correlated measurements
    between the two datasets.

    """
    print(
        (f"\rInterval[{start_datetime} => {end_datetime}]: "
         f"Correlated: {correlated_count}, "
         f"CoSMo: {cosmo_count}, "
         f"CNV Hydro: {cnv_hydro_count}"),
        end='',
        flush=True
    )


def precheck(conf_file):
    # need a destination database/table for the correlated data
    # database created by setup script? => yes
    # table created by setup script? => yes

    config = DBConfigFactory.build(conf_file)

    #################
    # set overrides for start and end
    if DBConfig.CONFIG_DATASET_START in config:
        global OVERRIDE_START_DATETIME
        OVERRIDE_START_DATETIME = config[DBConfig.CONFIG_DATASET_START]
        log.info(f"Using config file Dataset Start of {OVERRIDE_START_DATETIME}")

    if DBConfig.CONFIG_DATASET_END in config:
        global OVERRIDE_END_DATETIME
        OVERRIDE_END_DATETIME = config[DBConfig.CONFIG_DATASET_END]
        log.info(f"Using config file Dataset End of {OVERRIDE_END_DATETIME}")

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
                    cursor.execute(f"SHOW DATABASES LIKE '{target_db}';")

                    cursor.fetchall()

                    if cursor.rowcount != 1:
                        raise PrecheckFailedException(f"Could not find {target_db} target database")
                    else:
                        log.debug(f"Found target database {target_db}")

                    # nssk cosmo source database
                    cursor.reset()
                    cursor.execute(f"SHOW DATABASES LIKE '{SOURCE_DB_NSSK_COSMO}';")
                    cursor.fetchall()

                    if cursor.rowcount != 1:
                        raise PrecheckFailedException(f"Could not find source database {SOURCE_DB_NSSK_COSMO}")
                    else:
                        log.debug(f"Found source database {SOURCE_DB_NSSK_COSMO}")

                    # cnv rainfall source database
                    cursor.reset()
                    cursor.execute(f"SHOW DATABASES LIKE '{SOURCE_DB_CNV_FLOWWORKS}';")
                    cursor.fetchall()

                    if cursor.rowcount != 1:
                        raise PrecheckFailedException(f"Could not find source database {SOURCE_DB_CNV_FLOWWORKS}" )
                    else:
                        log.debug(f"Found source database {SOURCE_DB_CNV_FLOWWORKS}")

                    # check that a table for each sensor exists
                    for sensor in COSMO_SITES:
                        log.debug(f"Precheck Cosmo sensor site {target_db}.{sensor}")

                        cursor.reset()
                        # cursor.execute(f"use {target_db};")
                        # cursor.execute(f"SHOW TABLES LIKE '{target_db}.{sensor}';")
                        cursor.execute((
                            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " 
                            f"WHERE TABLE_SCHEMA='{target_db}' AND TABLE_NAME LIKE '{sensor}';"
                        ))

                        cursor.fetchall()
                        if cursor.rowcount != 1:
                            raise PrecheckFailedException(f"Could not find sensor table {target_db}.{sensor} in target database")
                        else:
                            log.debug(f"Found Sensor Table {sensor}")

                        # check that destination table is empty
                        cursor.reset()

                        # returns int 1 if not empty, and int 0 if empty
                        is_empty_query = f"SELECT EXISTS(SELECT 1 FROM {target_db}.{sensor} LIMIT 1) AS is_not_empty;"
                        #is_empty_query = f"SELECT (SELECT COUNT(*) FROM {target_db}.{sensor} ) > 0 AS is_not_empty;"
                        cursor.execute(is_empty_query)
                        result = cursor.fetchone()[0]
                        cursor.reset()
                        if result == 1:
                            log.debug(f"Truncating destination table {target_db}.{sensor}: {result}")

                            cursor.execute(f"truncate table {target_db}.{sensor}")

                            # check that truncate succeeded and table is empty
                            cursor.execute(is_empty_query)
                            result = cursor.fetchone()[0]
                            if result == 0:
                                log.debug(f"Destination table {target_db}.{sensor} is empty: {result}. Proceeding.")
                            else:
                                raise PrecheckFailedException((
                                    f"Table {target_db}.{sensor} is not empty after truncation. "
                                    f"result: {result}"
                                ))
                        else:
                            log.debug(f"Destination table {target_db}.{sensor} is empty: {result}. Proceeding.")

                log.info("Precheck Passed!")
            except Error as e:
                log.error("Error checking databases", e)
                raise
    except Error as e:
        log.error("Error connecting to database", e)
        raise

# return start and end dates for both cosmo conductivity measurements
def get_cosmo_measurements_date_window(cursor, sensor_name):
    ###################
    # determine cosmo start datetime (earliest conductivity measurement)
    # cosmo datetimes are split across date and time fields

    # TODO move this and other template reads to precheck. file existence check should not happen this late
    cosmo_date_search_template = Template(open("sql/get-cosmo-timestamp.sql.template").read())
    cosmo_date_search_sql = cosmo_date_search_template.substitute(
        DB=SOURCE_DB_NSSK_COSMO,
        SITE=sensor_name,
        ORDER=ORDER_ASC
    )

    if TRACE_LOGGING:
        log.debug(f"cosmo start date search sql:\n{cosmo_date_search_sql}")

    cursor.execute(cosmo_date_search_sql)
    row = cursor.fetchall()
    if cursor.rowcount == 1:
        # expect date in mysq datetime format
        # row[0] is a tuple containing a datetime, get value with row[0][0]
        cosmo_start_time = row[0][0]

        if TRACE_LOGGING:
            log.debug(f"cosmo raw start datetime: {cosmo_start_time}:\n{pprint.pformat(row[0])}")
    else:
        # TODO: custom exception
        raise Exception(
            f"Could not determine earliest cosmo measurement timestamp for site {sensor_name} in target database"
        )

    log_msg = f"Determined earliest cosmo measurement timestamp {cosmo_start_time}"
    log.info(log_msg)
    print(f"\t{log_msg}")

    # determine end datetime (most recent conductivity measurement)
    # cosmo datetimes are split across date and time fields

    cosmo_date_search_sql = cosmo_date_search_template.substitute(
        DB=SOURCE_DB_NSSK_COSMO,
        SITE=sensor_name,
        ORDER=ORDER_DESC
    )

    if TRACE_LOGGING:
        log.debug("cosmo end date search sql:\n%s" % cosmo_date_search_sql)

    cursor.execute(cosmo_date_search_sql)
    row = cursor.fetchall()
    if cursor.rowcount == 1:
        # row[0] is a tuple containing a datetime, get value with row[0][0]
        cosmo_end_time = row[0][0]
    else:
        # TODO: custom exception
        raise Exception(
            f"Could not determine latest cosmo measurement timestamp for site {sensor_name} in target database"
        )

    log_msg = f"Determined latest cosmo measurement timestamp {cosmo_end_time}"
    log.info(log_msg)
    print(f"\t{log_msg}")

    return cosmo_start_time, cosmo_end_time


# return start and end dates for cnv rainfall measurements
def get_cnv_flowworks_measurements_date_window(cursor, sensor_name):
    ###################
    # determine cnv_flowworks start datetime

    # TODO move this and other template reads to precheck
    cnv_flowworks_date_search_template = Template(open("sql/get-cnv-flowworks-timestamp.sql.template").read())
    cnv_flowworks_date_search_sql = cnv_flowworks_date_search_template.substitute(
        DB=SOURCE_DB_CNV_FLOWWORKS,
        SITE=sensor_name,
        ORDER=ORDER_ASC
    )

    if TRACE_LOGGING:
        log.debug(f"cnv flowworks start date search sql:\n{cnv_flowworks_date_search_sql}")

    cursor.execute(cnv_flowworks_date_search_sql)
    row = cursor.fetchall()
    if cursor.rowcount == 1:
        # expect date in mysq datetime format
        # print("raw start datetime: %s" % row[0][0])

        # pprint(row[0])

        # row[0] is a tuple containing a datetime, get value with row[0][0]
        cnv_flowworks_start_time = row[0][0]
    else:
        # TODO: custom exception
        raise Exception(
            f"Could not determine earliest rainfall measurement timestamp for site {sensor_name} in target database"
        )

    log_msg = f"Determined earliest rainfall measurement timestamp {cnv_flowworks_start_time}"
    log.info(log_msg)
    print(f"\t{log_msg}")

    # determine end datetime (most recent conductivity measurement)
    # cosmo datetimes are split across date and time fields

    cnv_flowworks_date_search_sql = cnv_flowworks_date_search_template.substitute(
        DB=SOURCE_DB_CNV_FLOWWORKS,
        SITE=sensor_name,
        ORDER="DESC"
    )

    if TRACE_LOGGING:
        log.debug(f"cnv_flowworks end date search sql:\n{cnv_flowworks_date_search_sql}")

    cursor.execute(cnv_flowworks_date_search_sql)
    row = cursor.fetchall()
    if cursor.rowcount == 1:
        # row[0] is a tuple containing a datetime, get value with row[0][0]
        cnv_flowworks_end_time = row[0][0]
    else:
        # TODO: custom exception
        raise Exception(
            f"Could not determine latest rainfall measurement timestamp for site {sensor_name} in target database"
        )

    log_msg = f"Determined latest rainfall measurement timestamp {cnv_flowworks_end_time}"
    log.info(log_msg)
    print(f"\t{log_msg}")

    return cnv_flowworks_start_time, cnv_flowworks_end_time


# return start and end dates for cnv hydrometric measurements
def get_cnv_hydrometric_measurements_date_window(cursor, sensor_name):
    ###################
    # determine cnv_hydrometric start datetime

    # TODO move this and other template reads to precheck
    cnv_hydrometric_date_search_template = Template(open("sql/get-cnv-hydrometric-timestamp.sql.template").read())
    cnv_hydrometric_date_search_sql = cnv_hydrometric_date_search_template.substitute(SITE=sensor_name, ORDER=ORDER_ASC)

    log.debug("cnv_hydrometric start date search sql:\n%s" % cnv_hydrometric_date_search_sql)

    cursor.execute(cnv_hydrometric_date_search_sql)
    row = cursor.fetchall()
    if cursor.rowcount == 1:
        # expect date in mysq datetime format
        # print("raw start datetime: %s" % row[0][0])

        # pprint(row[0])

        # row[0] is a tuple containing a datetime, get value with row[0][0]
        cnv_hydrometric_start_time = row[0][0]

        # TODO: check that we got a datetime here and everywhere else
    else:
        # TODO: custom exception
        raise Exception(
            "Could not determine earliest hydrometric measurement timestamp for site %s in target database" %
            sensor_name)

    log_msg = "Determined earliest hydrometric measurement timestamp %s" % cnv_hydrometric_start_time
    log.info(log_msg)
    print(f"\t{log_msg}")

    # determine end datetime (most recent cnv hydrometric measurement)
    # cnv hydrometric datetimes are split across date and time fields

    cnv_hydrometric_date_search_sql = cnv_hydrometric_date_search_template.substitute(SITE=sensor_name, ORDER=ORDER_DESC)

    if TRACE_LOGGING:
        log.debug("cnv_hydrometric end date search sql:\n%s" % cnv_hydrometric_date_search_sql)

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

    log_msg = f"Determined latest hydrometric timestamp {cnv_hydrometric_end_time}"
    log.info(log_msg)
    print(f"\t{log_msg}")

    return cnv_hydrometric_start_time, cnv_hydrometric_end_time


def collect_cosmo_and_cnvhydro_measurements(
        cosmo_site_name, cnv_hydro_site_name, db_config_filename
) -> list[RainfallIntervalDataEntry]:
    # different from strict correlation, as we're keeping cosmo and cnv hydrometric measurements that do not correlate
    # also are not narrowing to the time window where measurements from both datasets are present
    #
    # iterate in chunks over cosmo and cnv hydrometric datasets. correlation should be done in the chunk time window
    # to save a lot of unnecessary processing. TODO: maybe a temp mongodb for staging work (local or remote)

    # what happens if there's very skewed timestamps for measurements (i.e. cnv hydro starts 2 years before cosmo)?
    # story: a new cosmo site with conductivity data comes online
    #
    # we still want these measurements because maybe we have rainfall data from that time period
    # cosmo is our primary dataset where we look to find time-correlated values in the cnv hydro dataset
    # after all regular correlation processing is done, check if the first cnv hydro measurement is before the first cosmo
    # measurement
    # yes => use cnv hydro measurement template to retrieve start_cnv_hydro to start_cosmo. still page through this
    # just add these to the final measurement set. no need to check for correlation.

    # what happens if cosmo stops well before cnv hydrometric?
    # just like above, but do the comparison with the dataset end timestamps

    # what if there's a large gap in cosmo measurements but not cnv hydro?
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

    # sort function for a list of RainfallIntervalDataEntry for our specific purposes
    # TODO: maybe fold this into DataEntry later on. decide on a default sort order:
    # TODO: how do we sort a collection of DataEntry depending on the subclass implementation?
    def sort_prelim_measurements(obj: RainfallIntervalDataEntry):
        if not isinstance(obj, RainfallIntervalDataEntry):
            raise Exception(
                "Found something that isn't RainfallIntervalDataEntry in preliminary measurement list when attempting to sort")

        if TRACE_LOGGING:
            log.debug(f"Sorting object {obj.to_s()} with timestamp str {obj.get_timestamp_str()}")

        # sorting by string rather than datetime should be faster
        return obj.get_timestamp_str()

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

                    # determine if we're doing a production run of the dataset, or just a test window
                    # OVERRIDE_START_DATETIME must be set if testing
                    if OVERRIDE_START_DATETIME is None:
                        # normal execution
                        # get cosmo date range
                        (cosmo_start_time,
                         cosmo_end_time) = get_cosmo_measurements_date_window(cursor, cosmo_site_name)

                        # get cnv hydro date range
                        # might not need this if cosmo is our primary dataset. we might not care if there are no
                        # cnv hydro measurements for a time period as there's nothing to correlate

                        (cnv_hydro_start_time,
                         cnv_hydro_end_time) = get_cnv_hydrometric_measurements_date_window(cursor, cnv_hydro_site_name)

                        dataset_start: datetime = cosmo_start_time
                        if cosmo_start_time > cnv_hydro_start_time:
                            dataset_start = cnv_hydro_start_time

                        dataset_end = cosmo_end_time
                        if cosmo_end_time < cnv_hydro_end_time:
                            dataset_end = cnv_hydro_end_time
                    else:
                        # set start and end dates for test window
                        # if OVERRIDING_END_DATETIME is unset, use the current time
                        log_str = (
                            "==================="
                            f"OVERRIDING_START_DATETIME for CoSMo/CNV Hydrometric to {OVERRIDE_START_DATETIME}"
                            "==================="
                        )
                        log.warning(log_str)
                        print(log_str)

                        dataset_start = datetime.strptime(OVERRIDE_START_DATETIME, MYSQL_DATE_FMT)

                        if OVERRIDE_END_DATETIME is not None:
                            dataset_end = datetime.strptime(OVERRIDE_END_DATETIME, MYSQL_DATE_FMT)
                        else:
                            dataset_end = datetime.now()
                        log_str = (
                            "==================="
                            f"OVERRIDING_END_DATETIME for CoSMo/CNV Hydrometric to {OVERRIDE_END_DATETIME}"
                            "==================="
                        )
                        log.warning(log_str)
                        print(log_str)

                    log_str = (f"Running CoSMo/CNV Hydrometric correlation between {dataset_start} and {dataset_end} "
                    f"for CoSMo site {cosmo_site_name} and CNV Hydrometric site {cnv_hydro_site_name}")
                    log.info(log_str)
                    print(log_str)

                    # cosmo is our "primary" dataset used in this correlation
                    dataset_inc :datetime = dataset_start

                    correlated_measurement_count = 0
                    cosmo_measurement_count = 0
                    cnv_hydrometric_measurement_count = 0

                    # cosmo and other datasets can be pretty large. step through the table in time blocks
                    # even though we correlate with a buffer, we are still keeping/using uncorrelated data
                    while dataset_inc <= dataset_end:

                        # determine window and build queries
                        # need full date for cosmo
                        # need full date for cnv hydro
                        cosmo_block_start_datetime :datetime = dataset_inc
                        cosmo_block_end_datetime :datetime = (
                                dataset_inc + timedelta(seconds=COSMO_SCAN_INCREMENT))

                        if cosmo_block_end_datetime > dataset_end:
                            cosmo_block_end_datetime = dataset_end

                        log.debug((
                            f"Starting scan of cosmo block {cosmo_block_start_datetime}"
                            f" -> {cosmo_block_end_datetime}"
                        ))

                        # retrieve the cnv hydrometric data for the time of the cosmo block, +/- CORRELATION_WINDOW
                        cnv_hydro_block_start_datetime :datetime = cosmo_block_start_datetime - timedelta(
                            seconds=CNV_HYDROMETRIC_CORRELATION_WINDOW)
                        cnv_hydro_block_end_datetime :datetime = cosmo_block_end_datetime + timedelta(
                            seconds=CNV_HYDROMETRIC_CORRELATION_WINDOW)

                        if cnv_hydro_block_end_datetime > dataset_end:
                            cnv_hydro_block_end_datetime = dataset_end

                        cosmo_block_measurements_sql = cosmo_measurements_query_template.substitute(
                            DB=SOURCE_DB_NSSK_COSMO,
                            COSMO_SITE=cosmo_site_name,
                            COSMO_START_DATETIME=cosmo_block_start_datetime,
                            COSMO_END_DATETIME=cosmo_block_end_datetime
                        )

                        cnv_hydrometric_block_measurements_sql = cnv_hydrometric_measurements_query_template.substitute(
                            DB=SOURCE_DB_CNV_HYDROMETRIC,
                            CNV_HYDROMETRIC_SITE=cnv_hydro_site_name,
                            CNV_HYDROMETRIC_START_DATETIME=cnv_hydro_block_start_datetime,
                            CNV_HYDROMETRIC_END_DATETIME=cnv_hydro_block_end_datetime
                        )

                        ###########
                        # get cosmo measurements for block
                        # use a dict of measurement timestamps => DataEntry. we want quick determination of uniqueness
                        # by timestamp in MYSQL format

                        log.debug((
                            "Starting results retrieval of cosmo block "
                            f"{cosmo_block_start_datetime} -> {cosmo_block_end_datetime}"
                        ))

                        cosmo_block_measurements = {}
                        if TRACE_LOGGING:
                            log.debug(f"Retrieving cosmo measurements with query:\n{cosmo_block_measurements_sql}")

                        cursor.execute(cosmo_block_measurements_sql)

                        # retrieve cosmo measurements for the time block
                        # None measurements are expected occasionally
                        while True:
                            rows = cursor.fetchmany(BLOCK_QUERY_BATCH_SIZE)
                            if not rows:  # Empty list = no more results
                                break

                            for row in rows:
                                # query should return schema CosmoTimestamp, Conductivity, TemperatureWater
                                # needs to be a datetime here for time comparison below
                                measurement_timestamp = row[0]

                                if not isinstance(measurement_timestamp, datetime):
                                    raise Exception("CoSMo measurement timestamp must be datetime")

                                cosmo_timestamp_key = measurement_timestamp.strftime(MYSQL_DATE_FMT)

                                data_entry = {
                                    RainfallIntervalDataEntry.COSMO_TIMESTAMP_FIELD: cosmo_timestamp_key,
                                    RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD: row[1],
                                    RainfallIntervalDataEntry.COSMO_TEMPERATURE_WATER_FIELD: row[2]
                                }
                                try:
                                    new_entry = RainfallIntervalDataEntry(data_entry)

                                    new_entry.set_db_destination(target_destination_db)

                                    cosmo_block_measurements[cosmo_timestamp_key] = new_entry
                                except DataValidationException as e:
                                    log.error((
                                        "Validation error building DataEntry from cosmo measurement "
                                        f"({pprint.pformat(data_entry)}) checking databases. Discarding"
                                    ))
                                    raise e
                        ### end of cosmo block measurement retrieval

                        log.debug((
                            "Ending results retrieval of cosmo block "
                            f"{cosmo_block_start_datetime} -> {cosmo_block_end_datetime}"
                        ))

                        ###########
                        # get cnv hydro measurement for block +/- correlation window
                        cnv_hydrometric_block_measurements = {}

                        log.debug((
                            "Starting results retrieval of cnv hydrometric block "
                            f"{cnv_hydro_block_start_datetime} -> {cnv_hydro_block_end_datetime}"
                        ))

                        if TRACE_LOGGING:
                            log.debug((
                                "Retrieving cnv hydrometric measurements with query:\n"
                                f"{cnv_hydrometric_block_measurements_sql}"
                            ))

                        # reset for the next query
                        cursor.reset()

                        cursor.execute(cnv_hydrometric_block_measurements_sql)

                        # retrieve cosmo measurements for the time block
                        # None measurements are expected occasionally
                        while True:
                            rows = cursor.fetchmany(BLOCK_QUERY_BATCH_SIZE)
                            if not rows:  # Empty list = no more results
                                break

                            for row in rows:
                                # query should return schema CNVHydrometricTimestamp, RevisedFinalStage
                                measurement_timestamp = row[0]

                                if not isinstance(measurement_timestamp, datetime):
                                    raise Exception("CNV Hydrometric measurement timestamp must be datetime")

                                cnv_hydrometric_timestamp_key = measurement_timestamp.strftime(MYSQL_DATE_FMT)

                                data_entry = {
                                    RainfallIntervalDataEntry.CNV_HYDROMETRIC_TIMESTAMP_FIELD: cnv_hydrometric_timestamp_key,
                                    RainfallIntervalDataEntry.CNV_HYDROMETRIC_REVISED_STAGE_FIELD: row[1]
                                }
                                try:

                                    new_entry = RainfallIntervalDataEntry(data_entry)

                                    # use the cnv hydro site name associated with the cosmo site name
                                    # only one cnv hydro site now so hardcode it
                                    new_entry.set_db_destination(target_destination_db)

                                    cnv_hydrometric_block_measurements[cnv_hydrometric_timestamp_key] = new_entry

                                    # TODO: log something here at trace?

                                except DataValidationException as e:
                                    log.error((
                                        "Validation error building DataEntry from cnv hydrometric measurement "
                                        f"({pprint.pformat(data_entry)}) checking databases. Discarding"
                                    ))
                                    raise e

                        ### end of cnv hydro block measurement retrieval

                        log.debug("Retrieving cnv hydrometric measurements completed.")

                        log.debug((
                            "Ending results retrieval of cnv hydrometric block "
                            f"{cnv_hydro_block_start_datetime} -> {cnv_hydro_block_end_datetime}"
                        ))

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

                        log.debug(f"found cosmo block rows: {len(cosmo_block_measurements)}")
                        log.debug(f"found cnv hydrometric block rows: {len(cnv_hydrometric_block_measurements)}")


                        # iterate over new key lists since we're deleting elements from *_block_measurements
                        for cosmo_measurement_timestamp_key in list(cosmo_block_measurements.keys()):

                            # search for a correlated cnv hydro measurement
                            found_correlation = False

                            for cnv_hydrometric_timestamp_key in list(cnv_hydrometric_block_measurements.keys()):

                                cosmo_measurement_timestamp = datetime.strptime(cosmo_block_measurements[cosmo_measurement_timestamp_key].get(RainfallIntervalDataEntry.COSMO_TIMESTAMP_FIELD), MYSQL_DATE_FMT)
                                cnv_hydrometric_timestamp = datetime.strptime(cnv_hydrometric_block_measurements[cnv_hydrometric_timestamp_key].get(RainfallIntervalDataEntry.CNV_HYDROMETRIC_TIMESTAMP_FIELD), MYSQL_DATE_FMT)

                                if abs((cosmo_measurement_timestamp - cnv_hydrometric_timestamp).total_seconds()) < CNV_HYDROMETRIC_CORRELATION_WINDOW:
                                    # correlated timestamp
                                    # set cnv hydro values in cosmo measurement
                                    log.debug((
                                        f"Correlating cosmo measurement at {cosmo_measurement_timestamp_key}"
                                        f" with cnv hydrometric measurement at {cnv_hydrometric_timestamp_key}"
                                    ))

                                    found_correlation = True

                                    cosmo_measurement = cosmo_block_measurements[cosmo_measurement_timestamp_key]

                                    if TRACE_LOGGING:
                                        log.debug(f"\tCorrelated cosmo measurement:\n{cosmo_measurement.to_s()}")

                                    cnv_hydrometric_measurement = cnv_hydrometric_block_measurements[
                                        cnv_hydrometric_timestamp_key
                                    ]

                                    if TRACE_LOGGING:
                                        log.debug((
                                            "\tCorrelated cnv hydrometric measurement:\n"
                                            f"{cnv_hydrometric_measurement.to_s()}"
                                        ))

                                    # create new composite measurement
                                    # any cosmo measurement could be missing, or any set that has multiple measurements
                                    composite_measurement = RainfallIntervalDataEntry({
                                        RainfallIntervalDataEntry.COSMO_TIMESTAMP_FIELD: cosmo_measurement_timestamp_key,
                                        RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD: cosmo_measurement.get_cosmo_conductivity(),
                                        RainfallIntervalDataEntry.COSMO_TEMPERATURE_WATER_FIELD: cosmo_measurement.get_cosmo_temperature_water(),
                                        RainfallIntervalDataEntry.CNV_HYDROMETRIC_TIMESTAMP_FIELD: cnv_hydrometric_timestamp_key,
                                        RainfallIntervalDataEntry.CNV_HYDROMETRIC_REVISED_STAGE_FIELD: cnv_hydrometric_measurement.get_cnv_hydrometric_revised_stage(),
                                    })

                                    composite_measurement.set_db_destination(target_destination_db)

                                    correlated_measurements.append(composite_measurement)

                                    # below todo may apply to earlier implementation
                                    # TODO: these deletions may not be valid since we're iterating over the blocks by
                                    # iterator rather than by index

                                    # delete source cosmo measurement
                                    #del cosmo_block_measurements[cosmo_measurement_timestamp]
                                    if cosmo_block_measurements.pop(cosmo_measurement_timestamp_key, None) is None:
                                        raise Exception(("Could not remove cosmo measurement at "
                                                         f"{cosmo_measurement_timestamp_key} from block measurements"))

                                    # delete source cnv hydrometric measurement
                                    # del cnv_hydrometric_block_measurements[cnv_hydrometric_timestamp]
                                    if cnv_hydrometric_block_measurements.pop(cnv_hydrometric_timestamp_key, None) is None:
                                        raise Exception(("Could not remove cnv hydro measurement at "
                                                         f"{cnv_hydrometric_timestamp_key} from block measurements"))

                                    correlated_measurement_count += 1
                                    print_meas_correl_status(correlated_measurement_count, cosmo_measurement_count,
                                                             cnv_hydrometric_measurement_count,
                                                             cosmo_block_start_datetime, cosmo_block_end_datetime)

                                    # we found a correlated measurement. we only want the first one that matches the
                                    # correlation criteria, and don't care about the remaining cnv hydro measurements
                                    break
                                # else keep searching for a correlating cnv hydrometric measurement

                            # no correlation of a cosmo measurement to a cnv hydrometric measurement,
                            # unless the break was triggered
                            if not found_correlation:
                                log.debug((
                                    f"Cosmo measurement at {cosmo_measurement_timestamp_key} is not correlated "
                                    "with a CNV Hydrometric measurement")
                                )

                                # do not remove measurements from block, will be added later


                        # here the measurement block has finished processing

                        # add remaining cosmo measurements to cosmo_measurements{}, and set the db destination
                        for measurement_key, data_entry in cosmo_block_measurements.items():
                            if measurement_key not in cosmo_measurements:
                                data_entry.set_db_destination(target_destination_db)
                                cosmo_measurements[measurement_key] = data_entry

                                cosmo_measurement_count += 1
                                print_meas_correl_status(correlated_measurement_count, cosmo_measurement_count,
                                                         cnv_hydrometric_measurement_count,
                                                         cosmo_block_start_datetime, cosmo_block_end_datetime)
                            else:
                                msg = (
                                    (f"Measurement key '{measurement_key} => {data_entry.to_s()}' already "
                                     "exists tallying uncorrelated cosmo measurements:"
                                     f"{cnv_hydro_measurements[measurement_key].to_s()}")
                                )
                                log.warning(msg)

                        # add remaining cnv hydro measurements to cnv_hydro_measurements{}, and set the db destination
                        #cnv_hydro_measurements.update(cnv_hydrometric_block_measurements)
                        for measurement_key, data_entry in cnv_hydrometric_block_measurements.items():
                            if measurement_key not in cnv_hydro_measurements:
                                data_entry.set_db_destination(target_destination_db)
                                cnv_hydro_measurements[measurement_key] = data_entry

                                cnv_hydrometric_measurement_count += 1
                                print_meas_correl_status(correlated_measurement_count, cosmo_measurement_count,
                                                         cnv_hydrometric_measurement_count,
                                                         cosmo_block_start_datetime, cosmo_block_end_datetime)
                            else:
                                # likely inevitable to see duplicates as the second dataset that we're windowing through
                                # will overlap other windows with the correlation time buffer
                                msg = (
                                    (f"Measurement key '{measurement_key} => {data_entry.to_s()}' already "
                                     "exists tallying uncorrelated cnv hydro measurements:"
                                     f"{cnv_hydro_measurements[measurement_key].to_s()}")
                                )

                                log.warning(msg)

                        log.debug((
                            f"Ending scan of cosmo block {cosmo_block_start_datetime}"
                            f" -> {cosmo_block_end_datetime}"
                        ))

                        # increment source block start time
                        # ensure overlapping time windows are managed by query, and here
                        # query should be date_field >= start_date and date_field < end_date
                        dataset_inc = dataset_inc + timedelta(seconds=COSMO_SCAN_INCREMENT)

                    print("\nFinished processing cosmo and cnv hydrometric measurements. Consolidating...")
                    # all blocks are finished processing

                    # print out of how many measurements we found
                    log_msg = f"Total cosmo measurement count: {len(cosmo_measurements.keys())}"
                    print("%s" % log_msg, flush=True)
                    log.info(log_msg)

                    log_msg = f"Total cnv hydrometric measurement count: {len(cnv_hydro_measurements.keys())}"
                    print("%s" % log_msg, flush=True)
                    log.info(log_msg)

                    log_msg = f"Total correlated measurement count: {len(correlated_measurements)}"
                    print("%s" % log_msg, flush=True)
                    log.info(log_msg)

                    ##################################
                    # consolidate measurements from our component measurements
                    # cosmo_measurements, cnv_hydro_measurements, correlated_measurements
                    final_measurements.extend(cosmo_measurements.values())
                    final_measurements.extend(cnv_hydro_measurements.values())
                    final_measurements.extend(correlated_measurements)

                    correlation_processing_elapsed_time = (timeit.default_timer() - correlation_processing_start_time)
                    log_msg = "Completed rainfall correlation Processing in %.3f sec" % correlation_processing_elapsed_time
                    print("\n%s" % log_msg, flush=True)
                    log.info(log_msg)

                    print(f"Sorting {len(final_measurements)} preliminary measurements for cosmo_site {cosmo_site_name}")

                    sorting_start_time = timeit.default_timer()

                    # sort dataset in-place by measurement timestamp for easier correlation with rainfall values
                    final_measurements.sort(key=sort_prelim_measurements)

                    sorting_elapsed_time = (timeit.default_timer() - sorting_start_time)
                    print("Sorted preliminary measurements completed in %.3f sec" % sorting_elapsed_time)

                    correlation_processing_elapsed_time = (timeit.default_timer() - correlation_processing_start_time)
                    log_msg = "Completed CoSMo/CNV Hydrometric correlation processing in %.3f sec" % correlation_processing_elapsed_time
                    print("\n%s" % log_msg, flush=True)
                    log.info(log_msg)

            except Error as e:
                log.error("Error checking databases", exc_info=True)
                raise e
    except Error as e:
        log.error("Error connecting to database", exc_info=True)
        raise e

    return final_measurements


def correlate_rainfall_intervals(prelim_measurements, cosmo_site, cnv_flowworks_site, db_config_filename
                                 ) -> dict[str, RainfallIntervalDataEntry]:
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
    final_measurements: dict[str, RainfallIntervalDataEntry] = {}

    # measurements correlated with rainfall intervals
    correlated_rainfall_measurements: dict[str, RainfallIntervalDataEntry] = {}

    # measurements correlated with rainfall intervals
    uncorrelated_rainfall_measurements: dict[str, RainfallIntervalDataEntry] = {}

    config = DBConfigFactory.build(db_config_filename)

    log.info("Beginning rainfall correlation process")

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

                    if OVERRIDE_START_DATETIME is None:
                        # get cnv flowworks date range for block
                        (cnv_flowworks_start_datetime,
                         cnv_flowworks_end_datetime) = get_cnv_flowworks_measurements_date_window(cursor,
                                                                                                  cnv_flowworks_site)
                    else:
                        # set start and end dates for test window
                        # if OVERRIDING_END_DATETIME is unset, use the current time
                        log_str = (
                            "==================="
                            f"OVERRIDING_START_DATETIME for CNV Flowworks to {OVERRIDE_START_DATETIME}"
                            "==================="
                        )
                        log.warning(log_str)
                        print(log_str)
                        cnv_flowworks_start_datetime = datetime.strptime(OVERRIDE_START_DATETIME, MYSQL_DATE_FMT)

                        if OVERRIDE_END_DATETIME is not None:
                            cnv_flowworks_end_datetime = datetime.strptime(OVERRIDE_END_DATETIME, MYSQL_DATE_FMT)
                        else:
                            cnv_flowworks_end_datetime = datetime.now()

                        log_str = (
                            "==================="
                            f"OVERRIDING_END_DATETIME for CNV Flowworks to {OVERRIDE_END_DATETIME}"
                            "==================="
                        )
                        log.warning(log_str)
                        print(log_str)


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

                    # orphaned measurements: far from previous and next measurement. trailing measurement defined but
                    # does not comprise a 10-minute interval with the next element. these are discarded.

                    # flowworks dataset

                    # cnv flowworks is our "primary" dataset used in this correlation
                    cnv_flowworks_inc = cnv_flowworks_start_datetime

                    # running tally of how many 10-minute intervals we've collected
                    rainfall_measurement_count = 0

                    trailing_rainfall_measurement = None

                    def is_valid_rainfall_measurement(rf_m):
                        return (
                            rf_m is not None and
                            isinstance(rf_m, (list, tuple)) and
                            len(rf_m) >= 2 and
                            isinstance(rf_m[0], datetime) and
                            isinstance(rf_m[1], Decimal) )

                    # datasets can be pretty large. step through the table in time blocks
                    while cnv_flowworks_inc <= cnv_flowworks_end_datetime:

                        # determine window and build queries
                        cnv_flowworks_block_start_datetime = cnv_flowworks_inc
                        cnv_flowworks_block_end_datetime = (
                                cnv_flowworks_inc + timedelta(seconds=CNV_FLOWWORKS_SCAN_INCREMENT))

                        if cnv_flowworks_block_end_datetime > cnv_flowworks_end_datetime:
                            cnv_flowworks_block_end_datetime = cnv_flowworks_end_datetime

                        cnv_flowworks_block_measurements_sql = cnv_flowworks_measurements_query_template.substitute(
                            DB=SOURCE_DB_CNV_FLOWWORKS,
                            SITE=cnv_flowworks_site,
                            CNV_FLOWWORKS_START_DATETIME=cnv_flowworks_block_start_datetime,
                            CNV_FLOWWORKS_END_DATETIME=cnv_flowworks_block_end_datetime
                        )

                        log.debug("Starting scan of cnv flowworks %s block %s => %s" % (
                            cosmo_site,
                            cnv_flowworks_block_start_datetime,
                            cnv_flowworks_block_end_datetime)
                        )

                        if trailing_rainfall_measurement is not None:
                            log.debug((
                                f"Beginning block with trailing measurement from "
                                f"previous block: {trailing_rainfall_measurement[0].strftime(MYSQL_DATE_FMT)}"
                                f" => {pprint.pformat(trailing_rainfall_measurement)}"
                            ))

                        if TRACE_LOGGING:
                            log.debug("Retrieving CNV Flowworks measurement block with query:\n%s",
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
                        row_i = 0
                        while row_i < row_count:

                            log.debug(f"Processing measurement from index {row_i}")

                            measurement = rows[row_i]

                            if is_valid_rainfall_measurement(measurement):
                                if TRACE_LOGGING:
                                    log.debug((
                                        f"Processing valid measurement {measurement[0].strftime(MYSQL_DATE_FMT)}"
                                        f" => {pprint.pformat(measurement)}"
                                    ))
                            else:
                                # trailing_rainfall_measurement defined, but something isn't right
                                # raise exception because this is very bad, cannot continue
                                raise Exception(f"Found malformed measurement:\n{pprint.pformat(measurement)}")

                            # row[0]: measurement_timestamp
                            # row[1]: 5-minute rainfall amount

                            # the last iteration may have ended on a value we need to consider
                            if trailing_rainfall_measurement is not None:
                                log.debug((
                                    "Found trailing_rainfall_measurement: "
                                    f"{trailing_rainfall_measurement[0].strftime(MYSQL_DATE_FMT)}"
                                    f" => {pprint.pformat(trailing_rainfall_measurement)}"
                                ))

                                # trailing_rainfall_measurement[0]: measurement_timestamp
                                # trailing_rainfall_measurement[1]: 5-minute rainfall amount

                                # consider this the first measurement of a potential pair with row[i]
                                # rather than row[i] and row[i+1]

                                # sanity check trailing_rainfall_measurement
                                if is_valid_rainfall_measurement(trailing_rainfall_measurement):
                                    # valid trailing_rainfall_measurement
                                    # sanity check measurement timestamps so we haven't moved the window incorrectly
                                    if measurement[0] == trailing_rainfall_measurement[0]:
                                        raise Exception((
                                            "Current measurement and trailing measurement have the same timestamp:\n"
                                            f"trailing: {trailing_rainfall_measurement[0].strftime(MYSQL_DATE_FMT)}"
                                            f" => {pprint.pformat(trailing_rainfall_measurement)}\n"
                                            f"current: {measurement[0].strftime(MYSQL_DATE_FMT)}"
                                            f" => {pprint.pformat(measurement)}"
                                        ))
                                    else:
                                        # okay - trailing_rainfall_measurement valid, and not the same
                                        # process below
                                        if TRACE_LOGGING:
                                            log.debug((
                                                "Trailing measurement sanity check passed. "
                                                "Considering measurements- first: "
                                                f"{trailing_rainfall_measurement[0].strftime(MYSQL_DATE_FMT)}"
                                                f" => {pprint.pformat(trailing_rainfall_measurement)}\n"
                                                f"second: {measurement[0].strftime(MYSQL_DATE_FMT)}"
                                                f" => {pprint.pformat(measurement)}"
                                            ))
                                else:
                                    # trailing_rainfall_measurement defined, but something isn't right
                                    raise Exception(f"Trailing measurement malformed: {pprint.pformat(trailing_rainfall_measurement)}")

                                # valid trailing measurement

                                # check if we can make a 10-minute interval out of measurement_i and trailing_rainfall_measurement
                                # is trailing_rainfall_measurement and row[0] 5 mins apart? account for some buffer
                                # yes => place in results, continue loop (jump to next iteration, None out trailing_rainfall_measurement)
                                # no => continue execution, try same with next element in rows

                                # if trailing_rainfall_measurement is valid, but too far away from row[i], we need to
                                # either consider row[i] and row[i+1] or ****re-set the trailing measurement and continue
                                # the loop

                                # ***************************

                                first_rainfall_measurement = list(trailing_rainfall_measurement)
                                second_rainfall_measurement = list(measurement)

                                # unset trailing_rainfall_measurement now that we're done with it
                                trailing_rainfall_measurement = None

                                row_i += 1
                            else:
                                # no trailing measurement

                                if row_i == row_count - 1:
                                    # is this the last element? cant make a 10-minute interval out of a single
                                    # measurement
                                    # set our trailing_rainfall_measurement and consider it with the next measurement
                                    # block
                                    trailing_rainfall_measurement = list(measurement)

                                    log.debug((
                                        "Setting trailing_rainfall_measurement for next block: "
                                        f"{trailing_rainfall_measurement[0].strftime(MYSQL_DATE_FMT)}"
                                        f" => {pprint.pformat(trailing_rainfall_measurement)}"
                                    ))

                                    # count this row as processed
                                    row_i += 1

                                    # should prompt retrieval of next block
                                    continue
                                else:
                                    # no trailing measurement to consider
                                    # not the last measurement in the block
                                    first_rainfall_measurement = list(measurement)
                                    second_rainfall_measurement = list(rows[row_i + 1])

                                    # count this row as processed
                                    # XXXexpect an additional increment before the end of the loop to account for the second
                                    # measurement retrieved above

                                    # XXXactually increment this later depending on how we can form an interval
                                    #row_i += 2

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

                                log.debug((
                                    "Found first/second rainfall measurements comprise a 10-minute interval.\n"
                                    f"first: {first_rainfall_measurement[0].strftime(MYSQL_DATE_FMT)}"
                                    " => "
                                    f"{first_rainfall_measurement}\n"
                                    f"second: {second_rainfall_measurement[0].strftime(MYSQL_DATE_FMT)}" 
                                    " => "
                                    f"{second_rainfall_measurement}"
                                ))

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
                                print(f"\r\t10-minute Rainfall measurements processed: {rainfall_measurement_count}",
                                      end='',
                                      flush=True)

                                row_i += 2
                            else:

                                log.debug((
                                    "Found first/second measurements do not comprise a 10-minute interval.\n"
                                    f"first: {first_rainfall_measurement[0].strftime(MYSQL_DATE_FMT)}"
                                    " => "
                                    f"{first_rainfall_measurement}\n"
                                    f"second: {second_rainfall_measurement[0].strftime(MYSQL_DATE_FMT)}" 
                                    " => "
                                    f"{second_rainfall_measurement}"
                                ))
                                # cannot make an interval. no measurement tallying this iteration
                                # possible the second measurement is the first measurement in the next of a 10-min
                                # pair of measurements

                                # TODO: fix- only orphaned if trailing measurement was used
                                # logger.debug((
                                #     "Orphaned measurement "
                                #     f"{first_rainfall_measurement[0].strftime(MYSQL_DATE_FMT)} "
                                #     " => "
                                #     f"{first_rainfall_measurement}"
                                # ))

                                log.debug("Setting trailing measurement for next loop iteration")

                                # consider the second_rainfall_measurement as the first element next as the trailing_rainfall_measurement
                                trailing_rainfall_measurement = list(second_rainfall_measurement)

                                # since measurements m1 and m2 are no longer considered candidates for an interval,
                                # increment row twice (here, and outside this block) because the second measurement here
                                # will be the trailing measurement for the next iteration

                                # increment earlier
                                row_i += 2

                            # increment the row index for the next loop iteration
                            #row_i += 1

                        # end loop iterating rows

                        # shouldn't need to call cursor.reset() since we're fetching everything from the cursor


                        log.debug("Ending scan of cnv flowworks %s block %s => %s" %
                                  (cosmo_site, cnv_flowworks_block_start_datetime, cnv_flowworks_block_end_datetime))

                        # add uncorrelated measurements in this block to the running list
                        # comparison of original block size to the remaining measurements in the block
                        log.debug(
                            "Adding %d measurements uncorrelated with rainfall from initial block size %d" %
                            (len(block_measurements), initial_block_measurement_count)
                        )

                        for data_entry in block_measurements:
                            ts_str = data_entry.get_timestamp_str()

                            if ts_str not in uncorrelated_rainfall_measurements:
                                uncorrelated_rainfall_measurements[ts_str] = data_entry
                            else:
                                log.warning(f"Skipping duplicate uncorrelated measurement at : {ts_str}")

                        # increment source block start time
                        # ensure overlapping time windows are managed by query, and here
                        # query should be date_field >= start_date and date_field < end_date
                        # existence of a trailing measurement should not affect this, so long as the mysql query is
                        # exclusive of the block end datetime
                        cnv_flowworks_inc = cnv_flowworks_inc + timedelta(seconds=CNV_FLOWWORKS_SCAN_INCREMENT)

                correlation_processing_elapsed_time = (timeit.default_timer() - correlation_processing_start_time)
                log_msg = "Completed rainfall interval correlation processing in %.3f sec" % correlation_processing_elapsed_time
                print("\n%s" % log_msg, flush=True)
                log.info(log_msg)
            except Error as e:
                log.error("Error checking databases", exc_info=True)
                raise e
    except Error as e:
        log.error("Error connecting to database", exc_info=True)
        raise e



    ##########################
    # form our set of correlated and uncorrelated measurements

    # add measurements that do not correlate with 10-minute rainfall intervals to the final list
    final_measurements |= uncorrelated_rainfall_measurements

    # add measurements that do correlate with 10-minute rainfall intervals to the final list
    for timestamp, new_measurement in correlated_rainfall_measurements.items():
        if TRACE_LOGGING:
            log.debug(f"Adding new correlated measurement at {timestamp}: {new_measurement.to_s()}")
        new_measurement.set_db_destination(cosmo_site)
        final_measurements[timestamp] = new_measurement

    return final_measurements


def add_rainfall_interval_measurement(
        first_rainfall_measurement: list,
        second_rainfall_measurement: list,
        site: str,
        measurements: list[RainfallIntervalDataEntry],
        new_measurements: dict[str, RainfallIntervalDataEntry]
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
    if first_rainfall_measurement[0] > second_rainfall_measurement[0]:
        log.warning("Found second rainfall measurement before first rainfall measurement")
        temp = second_rainfall_measurement
        second_rainfall_measurement = first_rainfall_measurement
        first_rainfall_measurement = temp

    # the earlier rainfall timestamp is considered the middle of the interval
    # a rainfall measurement is the rainfall amount recorded in the previous 5 minutes
    target_timestamp = first_rainfall_measurement[0]

    # beyond this threshold, a list of sorted candidate measurements has no candidates near enough in time
    latest_valid_measurement_datetime = target_timestamp + timedelta(seconds=RAINFALL_CORRELATION_THRESHOLD)

    # we're adding the rainfall amount to something, so record it outside the loop
    interval_rainfall_amt = float(first_rainfall_measurement[1]) + float(second_rainfall_measurement[1])

    # search the measurements list for a cosmo/cnvhydro measurement that can be correlated with the rainfall measurement
    # otherwise add a new measurement with just the rainfall data, since there's no correlated cosmo/cnvhydro
    measurement_i = 0
    for measurement in measurements:


        measurement_timestamp_str = measurement.get_timestamp_str()

        # i dont think this is right to do here. this is the block search space, and we want to determine if
        # the rainfall measurement, correlated or not, is a duplicate of something we already have
        #
        # if measurement_timestamp_str in new_measurements:
        #     log.warning(f"Skipping duplicate potential correlated measurement at : {measurement_timestamp_str}")
        #     del measurements[measurement_i]
        #
        #     # mitigated by the del?
        #     #measurement_i += 1
        #
        #     return

        #########################################
        measurement_timestamp = measurement.get_timestamp()

        # is the measurement timestamp close enough in time to the rainfall timestamp?
        if abs((measurement_timestamp - target_timestamp).total_seconds()) < RAINFALL_CORRELATION_THRESHOLD:
            # we can correlate this measurement with our rainfall data

            log.debug((
                f"Rainfall measurement at {target_timestamp} correlates to cosmo/cnvhydro measurement at "
                f"{measurement_timestamp} for site {site}:\n"
                f"first: {first_rainfall_measurement}\nsecond: {second_rainfall_measurement}"
            ))

            # create a new copy to modify
            new_measurement = measurement.copy()

            # set the fields in our new correlated measurement
            # schema guarantees rainfall, airtemp, baro pressure
            # rainfall is summed, air temp and baro pressure taken from first measurement as they don't change much over
            # 5 minutes


            new_measurement.set(RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_AMT_FIELD, interval_rainfall_amt)
            new_measurement.set(RainfallIntervalDataEntry.CNV_FLOWWORKS_BARO_PRESSURE_FIELD,
                                first_rainfall_measurement[2])
            new_measurement.set(RainfallIntervalDataEntry.CNV_FLOWWORKS_AIR_TEMPERATURE_FIELD,
                                first_rainfall_measurement[3])

            new_measurement.set(RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_START_TIMESTAMP_FIELD,
                                first_rainfall_measurement[0])
            new_measurement.set(RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_END_TIMESTAMP_FIELD,
                                second_rainfall_measurement[0])

            # copy() should set the db destination



            # think this part is done with the conditional early in the loop
            ########################################
            # TODO: resolve, should cut down on the last of the duplicates
            # check if we've already processed this due to window boundary issues
            # could be a cnv hydro measurement, or cosmo, or both
            # if new_measurements
            #     log.debug("Skipping...already have seen this measurement")
            #     return

            # does the measurement timestamp exist in our processed timestamps?
            # TODO: look at refactoring new_measurements into a dict, if manual scan cuts down on the last duplicates
            # found_it = False
            # for processed_measurement in new_measurements:
            #     if new_measurement.get_timestamp_str() == processed_measurement.get_timestamp_str():
            #         found_it = True
            #         break
            #
            # if found_it:
            #     log.info(f"Already saw measurement {new_measurement.to_s()}. Skipping...")
            #
            #     # already processed this measurement earlier, so bail out of adding it
            #     return

            log.debug(f"Adding new correlated measurement for site {site}:\n{new_measurement.to_s()}")
            # add the new measurement to the list of new measurements
            new_measurements[measurement_timestamp_str] = new_measurement

            # delete the original measurement
            # only expect a single correlation, and to cut down on search space
            del measurements[measurement_i]


            return
        elif measurement_timestamp > latest_valid_measurement_datetime:
            log.debug((
                f"Rainfall measurement at {target_timestamp} does not correlate to cosmo/cnvhydro measurement for "
                f"site {site}:\nfirst: {first_rainfall_measurement}\nsecond: {second_rainfall_measurement}"
            ))

            # we're past the target timestamp of a sorted list, then there's no correlation
            # use a break so we add a new measurement if measurements is empty
            break
        else:
            # continue the search
            pass

        # else keep searching for a potential correlation
        measurement_i += 1

    #####
    # add a new measurement of just the rainfall data.
    # we've either run out of search space entirely, or of correlated cosmo/cnvhydro measurements close enough
    rainfall_data_measurement = RainfallIntervalDataEntry({
            RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_START_TIMESTAMP_FIELD: first_rainfall_measurement[0],
            RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_END_TIMESTAMP_FIELD: second_rainfall_measurement[0],
            RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_AMT_FIELD: interval_rainfall_amt,
            RainfallIntervalDataEntry.CNV_FLOWWORKS_BARO_PRESSURE_FIELD: first_rainfall_measurement[2],
            RainfallIntervalDataEntry.CNV_FLOWWORKS_AIR_TEMPERATURE_FIELD: first_rainfall_measurement[3]
        })

    # does the measurement timestamp exist in our processed timestamps?
    # TODO: look at refactoring new_measurements into a dict, if manual scan cuts down on the last duplicates
    # found_it = False
    # for processed_measurement in new_measurements:
    #     if rainfall_data_measurement.get_timestamp_str() == processed_measurement.get_timestamp_str():
    #         found_it = True
    #         break
    #
    # if found_it:
    #     log.info(f"Already saw measurement {rainfall_data_measurement.to_s()}. Skipping...")
    #
    #     # already processed this measurement earlier, so bail out of adding it
    #     return

    log.debug(
        f"Adding uncorrelated rainfall measurement for site {site}:\n{rainfall_data_measurement.to_s()}"
    )

    ts_str = rainfall_data_measurement.get_timestamp_str()
    if ts_str not in new_measurements:
        new_measurements[ts_str] = rainfall_data_measurement
    else:
        log.warning(f"Skipping adding duplicate uncorrelated rainfall measurement {rainfall_data_measurement.to_s()}")


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

    log.debug("Determining block measurements for block (%s => %s)" % (start_timestamp, end_timestamp))

    # is end before start? then swap
    if end_timestamp < start_timestamp:
        log.warning("Found block end_timestamp earlier than start_timestamp (%s vs %s). Swapping." % (end_timestamp, start_timestamp))
        temp = end_timestamp
        end_timestamp = start_timestamp
        start_timestamp = temp

    block_measurements: list[RainfallIntervalDataEntry] = []

    block_measurement_count = 0
    for measurement in measurements:
        if start_timestamp <= measurement.get_timestamp() <= end_timestamp:
            block_measurements.append(measurement)
            block_measurement_count += 1

    log.debug(f"Found {block_measurement_count} preliminary measurements in block {start_timestamp} => {end_timestamp}")

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

    # cosmo site is our primary dataset
    #
    # for multiple cosmo sites using the same cnv hydro site, the cnv hydro site is retrieved and stepped through
    # multiple times. this is preferable to caching the cnv hydro measurements (which can be very large) in memory
    for cosmo_site in COSMO_SITES:

        print("Retrieving measurements for cosmo_site %s and associated cnv hydrometric data" % cosmo_site)

        # returns a partially sorted array of measurements
        # sql queries typically return results ordered by date in ASC order, and windowing is done from old to new
        # essentially a flat array of sorted cosmo-only, sorted cnv-hydro-only, and sorted composite
        # need everything sorted by which date fields are present

        prelim_measurements = collect_cosmo_and_cnvhydro_measurements(cosmo_site, SOURCE_DB_CNV_HYDROMETRICS_SITE,
                                                                      db_config_filename)



        # debugging
        # print("Sorted measurements:")
        # for measurement in prelim_measurements:
        #     print ("%s\n------" % measurement.to_s())

        #################
        # correlate 10-minute rainfall amounts with accumulated measurements
        final_measurements = correlate_rainfall_intervals(prelim_measurements, cosmo_site, SOURCE_DB_CNV_FLOWWORKS_SITE,
                                                          db_config_filename)

        log_msg = f"Adding {cosmo_site} site measurements to importer..."
        print("\n%s" % log_msg)
        log.info(log_msg)

        #################################################
        # add measurements to importer
        for timestamp, measurement in final_measurements.items():
            if TRACE_LOGGING:
                log.debug("Adding measurement:\n%s" % measurement.to_s())
            db_importer.add(measurement)

        log_msg = "Processing %s site completed" % cosmo_site
        print("%s" % log_msg)
        log.info(log_msg)
        print("=============================================================================")


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
        log.info(log_msg)
        print(log_msg)
        dry_run = True
    else:
        # data import - make sure we have a config file to connect to the database
        if db_config_filename is None:
            print("Error- Need a DB config file for the import")
            exit(1)

        log_msg = "Executing import"
        log.info(log_msg)
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
        log.info(log_msg)

        print("Exiting...")

    log.info("Exiting...")


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
