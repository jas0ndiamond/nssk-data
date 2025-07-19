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

logFile = "interval-data.log"

logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
logging.getLogger("IntervalDataDataEntry").setLevel(logging.INFO)
logging.getLogger("DBImporter").setLevel(logging.INFO)

# time window to search for a corresponding conductivity value
# +/- 5 minutes. in seconds
CORRELATION_WINDOW = 5 * 60

# increment to step through measurements in a COSMO table
# shouldn't be too big
# 7 days
COSMO_SCAN_INCREMENT = 60 * 60 * 24 * 7

CNV_FLOWWORKS_SCAN_INCREMENT = 60 * 60 * 24 * 7

CNV_HYDROMETRIC_SCAN_INCREMENT = 60 * 60 * 24 * 7

SENSORS = [
    "WAGG01",
    "WAGG03"
]

SOURCE_DB_NSSK_COSMO = "NSSK_COSMO"
SOURCE_DB_CNV_FLOWWORKS = "NSSK_CNV_FLOWWORKS"

# only one site in dataset
SOURCE_DB_CNV_FLOWWORKS_SITE = "CNVRain"

# only one site in dataset
SOURCE_DB_CNV_HYDROMETRICS_SITE = "WaggCreek"

COSMO_SCHEMA = [
    "CosmoTimestamp",
    "Conductivity",
    "WaterTemperature"
]

SCHEMA = [
    "CosmoTimestamp",
    "Conductivity",
    "WaterTemperature"
    "RevisedStage",
    "BarometricPressure",
    "AirTemperature",
    "RainfallStart"
    "RainfallEnd"
    "RainfallAmount"
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
                    cursor.execute("SHOW DATABASES LIKE '%s';" % SOURCE_DB_CNV_FLOWWORKS)
                    cursor.fetchall()

                    if cursor.rowcount != 1:
                        # TODO: custom exception
                        raise Exception("Could not find source database %s" % SOURCE_DB_CNV_FLOWWORKS)
                    else:
                        logger.debug("Found source database %s" % SOURCE_DB_CNV_FLOWWORKS)

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

# return start and end dates for both cosmo conductivity measurements, and cnv rainfall measurements
def get_cosmo_measurements_date_window(cursor, sensor_name):
    ###################
    # determine cosmo start datetime (earliest conductivity measurement)
    # cosmo datetimes are split across date and time fields

    # TODO move this and other template reads to precheck
    cosmo_date_search_template = Template(open("sql/get-cosmo-timestamp.sql.template").read())
    cosmo_date_search_sql = cosmo_date_search_template.substitute(SITE=sensor_name, ORDER="ASC")

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

    cosmo_date_search_sql = cosmo_date_search_template.substitute(SITE=sensor_name, ORDER="DESC")

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
    cnv_flowworks_date_search_sql = cnv_flowworks_date_search_template.substitute(SITE=sensor_name, ORDER="ASC")

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

def collect_interval_data(sensor, db_config_filename, db_importer):

    # cosmo sites may be bound to different cnv hydrometric and cnv flowworks sites

    # determine start and end of cosmo data
    # determine start and end of rainfall data

    # get time-window slice of cosmo data
    # get time-window slice of rainfall data

    # for each
    # for each cosmo conductivity and temperature measurement, determine if there are correlatable rainfall measurements
        # add the two best ones together
        # attempt to correlate revised flow

    measurements = []

    cosmo_measurements = collect_cosmo_measurements(sensor, db_config_filename)

    cnv_rainfall_measurements = collect_cnv_flowworks_measurements(SOURCE_DB_CNV_FLOWWORKS_SITE, db_config_filename)

    # combine with cnv_rainfall measurements

    cnv_hydrometrics_measurements = collect_cnv_hydrometric_measurements(SOURCE_DB_CNV_HYDROMETRICS_SITE, db_config_filename)

    # combine in cnv_hydrometrics



def collect_cnv_flowworks_measurements(sensor_name, db_config_filename):
    config = DBConfigFactory.build(db_config_filename)

    cnv_rainfall_measurements = []

    # TODO: check exception block messages and flow
    try:
        with (connect(
                host=config[DBConfig.CONFIG_HOST],
                port=int(config[DBConfig.CONFIG_PORT]),
                user=config[DBConfig.CONFIG_USER],
                password=config[DBConfig.CONFIG_PASS]
        ) as connection):
            config[DBConfig.CONFIG_PASS] = None

            cnv_flowworks_measurements = []

            try:
                with connection.cursor() as cursor:
                    (cnv_flowworks_start_time, cnv_flowworks_end_time) = get_cnv_flowworks_measurements_date_window(cursor, sensor_name)

                    print(("==========\n" +
                           "cnv_flowworks_start_time: %s\n" +
                           "cnv_flowworks_time: %s\n"
                           ) %
                          (cnv_flowworks_start_time,
                           cnv_flowworks_end_time)
                          )

                    cnv_flowworks_measurement_count = 0

                    cnv_flowworks_date_i = cnv_flowworks_start_time

                    cnv_flowworks_query_template = Template(
                        open("sql/get-cnv-flowworks-measurements.sql.template").read()
                    )

                    correlation_processing_start_time = timeit.default_timer()

                    # even though we're not correlating between datasets, iterate over cosmo in blocks
                    # the measurement retrieval query will return a lot of results
                    while cnv_flowworks_date_i <= cnv_flowworks_end_time:
                        # retrieve a block of sensor data.
                        # should be okay if end date is past cnv_flowworks_time- nothing will be pulled from the db
                        # start: cnv_flowworks_date_i
                        # end: cnv_flowworks_date_i + CORRELATION_INCREMENT
                        cnv_flowworks_block_start_date = cnv_flowworks_date_i
                        cnv_flowworks_block_end_date = cnv_flowworks_date_i + datetime.timedelta(seconds=CNV_FLOWWORKS_SCAN_INCREMENT)

                        ####################
                        # run block query
                        #
                        # build query from template
                        # run query
                        # iterate and process results
                        cnv_flowworks_measurements_query = cnv_flowworks_query_template.substitute(
                            SITE=sensor_name,
                            CNV_FLOWWORKS_START_DATETIME=cnv_flowworks_block_start_date,
                            CNV_FLOWWORKS_END_DATETIME=cnv_flowworks_block_end_date
                        )

                        logger.debug("Running CNV Flowworks measurements query:\n%s", cnv_flowworks_measurements_query)

                        cursor.execute(cnv_flowworks_measurements_query)

                        row = cursor.fetchone()
                        while row is not None:
                            # CosmoTimeStamp, Conductivity, TemperatureWater

                            # rainfall needs to be summed into 10 minute intervals, but only if measurements are 5 mins apart


                            data_entry = {
                                RainfallIntervalDataEntry.CNV_FLOWWORKS_TIMESTAMP_FIELD: row[0],
                                RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_FIELD: row[1]
                            }

                            cnv_flowworks_measurements.append( RainfallIntervalDataEntry(data_entry) )

                            cnv_flowworks_measurement_count += 1

                            print("\r\tCNV Flowworks measurement accumulated: %d" % cnv_flowworks_measurement_count, end='', flush=True)

                            # grab next row
                            row = cursor.fetchone()

                        # increment source block start time
                        # ensure overlapping time windows are managed by query, and here
                        # query should be date_field >= start_date and date_field < end_date
                        cnv_flowworks_date_i = cnv_flowworks_date_i + datetime.timedelta(seconds=CNV_FLOWWORKS_SCAN_INCREMENT)

                    correlation_processing_elapsed_time = (timeit.default_timer() - correlation_processing_start_time)
                    log_msg = "Completed CNV Flowworks measurement accumulation in %.3f sec" % correlation_processing_elapsed_time
                    print("\n%s" % log_msg, flush=True)
                    logger.info(log_msg)

            except Error as e:
                logger.error("Error checking databases", e)
    except Error as e:
        logger.error("Error connecting to database", e)

    return cnv_rainfall_measurements

def collect_cosmo_measurements(sensor_name, db_config_filename):
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

            cosmo_measurements = []

            try:
                with connection.cursor() as cursor:

                    (cosmo_start_time, cosmo_end_time) = get_cosmo_measurements_date_window(cursor, sensor_name)

                    print(("==========\n" +
                           "cosmo_start_time: %s\n" +
                           "cosmo_end_time: %s\n"
                           ) %
                          (cosmo_start_time,
                           cosmo_end_time)
                          )

                    cosmo_measurement_count = 0

                    cosmo_date_i = cosmo_start_time

                    cosmo_query_template = Template(
                        open("sql/get-cosmo-measurements.sql.template").read()
                    )

                    correlation_processing_start_time = timeit.default_timer()

                    # even though we're not correlating between datasets, iterate over cosmo in blocks
                    # the measurement retrieval query will return a lot of results
                    while cosmo_date_i <= cosmo_end_time:
                        # retrieve a block of sensor data.
                        # should be okay if end date is past cosmo_end_time- nothing will be pulled from the db
                        # start: cosmo_date_i
                        # end: cosmo_date_i + CORRELATION_INCREMENT
                        cosmo_block_start_date = cosmo_date_i
                        cosmo_block_end_date = cosmo_date_i + datetime.timedelta(seconds=COSMO_SCAN_INCREMENT)

                        ####################
                        # run block query
                        #
                        # build query from template
                        # run query
                        # iterate and process results
                        cosmo_measurements_query = cosmo_query_template.substitute(
                            COSMO_SITE=sensor_name,
                            COSMO_START_DATETIME=cosmo_block_start_date,
                            COSMO_END_DATETIME=cosmo_block_end_date
                        )

                        logger.debug("Running CoSMo measurements query:\n%s", cosmo_measurements_query)

                        cursor.execute(cosmo_measurements_query)

                        row = cursor.fetchone()
                        while row is not None:
                            # CosmoTimeStamp, Conductivity, TemperatureWater

                            data_entry = {
                                RainfallIntervalDataEntry.COSMO_TIMESTAMP_FIELD: row[0],
                                RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD: row[1],
                                RainfallIntervalDataEntry.COSMO_TEMPERATURE_WATER_FIELD: row[2]
                            }

                            # create the entry, no attempt to correlate timestamps as cosmo is processed first
                            cosmo_measurements.append( RainfallIntervalDataEntry(data_entry) )

                            cosmo_measurement_count += 1

                            print("\r\tCoSMo measurement accumulated: %d" % cosmo_measurement_count, end='', flush=True)

                            # grab next row
                            row = cursor.fetchone()

                        # increment source block start time
                        # ensure overlapping time windows are managed by query, and here
                        # query should be date_field >= start_date and date_field < end_date
                        cosmo_date_i = cosmo_date_i + datetime.timedelta(seconds=COSMO_SCAN_INCREMENT)

                correlation_processing_elapsed_time = (timeit.default_timer() - correlation_processing_start_time)
                log_msg = "Completed CoSMo measurement accumulation in %.3f sec" % correlation_processing_elapsed_time
                print("\n%s" % log_msg, flush=True)
                logger.info(log_msg)

            except Error as e:
                logger.error("Error checking databases", e)
    except Error as e:
        logger.error("Error connecting to database", e)

    return cosmo_measurements

def collect_cnv_hydrometric_measurements(sensor_name, db_config_filename):
    config = DBConfigFactory.build(db_config_filename)

    cnv_hydrometric_measurements = []

    # TODO: check exception block messages and flow
    try:
        with (connect(
                host=config[DBConfig.CONFIG_HOST],
                port=int(config[DBConfig.CONFIG_PORT]),
                user=config[DBConfig.CONFIG_USER],
                password=config[DBConfig.CONFIG_PASS]
        ) as connection):
            config[DBConfig.CONFIG_PASS] = None

            cnv_hydrometric_measurements = []

            try:
                with connection.cursor() as cursor:
                    (cnv_hydrometric_start_time, cnv_hydrometric_end_time) = get_cnv_hydrometric_measurements_date_window(cursor, sensor_name)

                    print(("==========\n" +
                           "cnv_hydrometric_start_time: %s\n" +
                           "cnv_hydrometric_time: %s\n"
                           ) %
                          (cnv_hydrometric_start_time,
                           cnv_hydrometric_end_time)
                          )

                    cnv_hydrometric_measurement_count = 0

                    cnv_hydrometric_date_i = cnv_hydrometric_start_time

                    cnv_hydrometric_query_template = Template(
                        open("sql/get-cnv-hydrometric-measurements.sql.template").read()
                    )

                    correlation_processing_start_time = timeit.default_timer()

                    # even though we're not correlating between datasets, iterate over cosmo in blocks
                    # the measurement retrieval query will return a lot of results
                    while cnv_hydrometric_date_i <= cnv_hydrometric_end_time:
                        # retrieve a block of sensor data.
                        # should be okay if end date is past cnv_flowworks_time- nothing will be pulled from the db
                        # start: cnv_flowworks_date_i
                        # end: cnv_flowworks_date_i + CORRELATION_INCREMENT
                        cnv_hydrometric_block_start_date = cnv_hydrometric_date_i
                        cnv_hydrometric_block_end_date = cnv_hydrometric_date_i + datetime.timedelta(seconds=CNV_HYDROMETRIC_SCAN_INCREMENT)

                        ####################
                        # run block query
                        #
                        # build query from template
                        # run query
                        # iterate and process results
                        cnv_hydrometric_measurements_query = cnv_hydrometric_query_template.substitute(
                            SITE=sensor_name,
                            CNV_HYDROMETRIC_START_DATETIME=cnv_hydrometric_block_start_date,
                            CNV_HYDROMETRIC_END_DATETIME=cnv_hydrometric_block_end_date
                        )

                        logger.debug("Running CNV Hydrometric measurements query:\n%s", cnv_hydrometric_measurements_query)

                        cursor.execute(cnv_hydrometric_measurements_query)

                        row = cursor.fetchone()
                        while row is not None:
                            # CosmoTimeStamp, Conductivity, TemperatureWater

                            data_entry = {
                                COSMO_SCHEMA[0]: row[0],
                                COSMO_SCHEMA[1]: row[1],
                                COSMO_SCHEMA[2]: row[2]
                            }

                            cnv_hydrometric_measurements.append(data_entry)

                            cnv_hydrometric_measurement_count += 1

                            print("\r\tCNV Hydrometric measurement accumulated: %d" % cnv_hydrometric_measurement_count, end='', flush=True)

                            # grab next row
                            row = cursor.fetchone()

                        # increment source block start time
                        # ensure overlapping time windows are managed by query, and here
                        # query should be date_field >= start_date and date_field < end_date
                        cosmo_date_i = cosmo_date_i + datetime.timedelta(seconds=CNV_HYDROMETRIC_SCAN_INCREMENT)

                    correlation_processing_elapsed_time = (timeit.default_timer() - correlation_processing_start_time)
                    log_msg = "Completed CNV Hydrometric measurement accumulation in %.3f sec" % correlation_processing_elapsed_time
                    print("\n%s" % log_msg, flush=True)
                    logger.info(log_msg)

            except Error as e:
                logger.error("Error checking databases", e)
    except Error as e:
        logger.error("Error connecting to database", e)

    return cnv_hydrometric_measurements

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

    # for a sensor
    #   get the start date and end date
    for sensor in SENSORS:
        print("Running rainfall interval data collection for sensor %s" % sensor)
        collect_interval_data(sensor, db_config_filename, db_importer)

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
