# debug resource
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
from RainfallEventMeasurementsDataEntry import RainfallEventMeasurementsDataEntry

# compile rainfall event data for the cnv region

# designed to run once after both cosmo-import and cnv-rainfall-import have run.

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

logFile = "rainfall-event-data.log"

logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
logging.getLogger("RainfallEventMeasurementsDataEntry").setLevel(logging.INFO)
logging.getLogger("DBImporter").setLevel(logging.INFO)

# time window to search for a corresponding conductivity value
# +/- 5 minutes. in seconds
CORRELATION_WINDOW = 5 * 60

COSMO_SENSOR_SITES = [
    "WAGG01",
    "WAGG03"
]

SCHEMA = [
    RainfallEventMeasurementsDataEntry.CNV_TIMESTAMP_FIELD,
    RainfallEventMeasurementsDataEntry.CNV_RAINFALL_AMOUNT_FIELD,
    RainfallEventMeasurementsDataEntry.CNV_AIR_TEMPERATURE_FIELD,
    RainfallEventMeasurementsDataEntry.COSMO_CONDUCTANCE_RESULT_FIELD,
    RainfallEventMeasurementsDataEntry.DNV_WHITEWATER_FLOW_READING_FIELD,
    RainfallEventMeasurementsDataEntry.RAINFALL_EVENT_ID_FIELD
]

SOURCE_DB_NSSK_COSMO = "NSSK_COSMO"
SOURCE_DB_CNV_RAINFALL = "NSSK_CNV_RAINFALL"
SOURCE_DB_DNV_WHITEWATER = "NSSK_DNV_WHITEWATER"

# only one site in dataset
SOURCE_DB_CNV_RAINFALL_SITE = "CNV"

# only one site in dataset
SOURCE_DB_DNV_WHITEWATER_SITE = "DNV"

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
            config[DBConfig.CONFIG_USER] = None
            config[DBConfig.CONFIG_PASS] = None

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

                    # check that our destination tables exist
                    for sensor in COSMO_SENSOR_SITES:
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


def load_rainfall_event_intervals(cursor):
    events = []

    rainfall_events_sql = open("../rainfall-event-data/sql/get-rainfall-events.sql").read()

    cursor.execute(rainfall_events_sql)

    row = cursor.fetchone()
    while row is not None:
        logger.debug("Retrieved rainfall event [%s] => [%s]: %s" % (row[0], row[1], row[2]))
        events.append([row[0], row[1], row[2]])
        row = cursor.fetchone()

    return events


# load the rainfall data for the given time interval. return a list of tuples that will be used in correlation steps.
def load_cnv_rainfall_data(cursor, start_datetime, end_datetime):
    cnv_rainfall_template = Template(
        open("../rainfall-event-data/sql/get-cnv-data-for-rainfall-event.sql.template").read())

    get_cnv_rainfall_data_query_sql = cnv_rainfall_template.substitute(
        CNV_RAINFALL_START_DATETIME=start_datetime,
        CNV_RAINFALL_END_DATETIME=end_datetime,
        SITE=SOURCE_DB_CNV_RAINFALL_SITE
    )

    cursor.execute(get_cnv_rainfall_data_query_sql)

    row = cursor.fetchone()

    data = []

    # TODO: error/exception on None check
    while row is not None:
        data.append(
            {
                RainfallEventMeasurementsDataEntry.CNV_TIMESTAMP_FIELD: row[0],
                RainfallEventMeasurementsDataEntry.CNV_RAINFALL_AMOUNT_FIELD: float(row[1]),
                RainfallEventMeasurementsDataEntry.CNV_AIR_TEMPERATURE_FIELD: float(row[2])
            }
        )

        row = cursor.fetchone()

    return data


def correlate_with_cosmo_conductance(cursor, sensor_site, cnv_rainfall_timestamp):
    logger.debug("Attempting to correlate a CoSMo measurement with CNV Rainfall timestamp %s for site %s"
                 % (cnv_rainfall_timestamp, sensor_site))

    # default measurement is a tuple with None values for measurement timestamp and value
    correlated_measurement = (None, None)

    search_cosmo_start_datetime = cnv_rainfall_timestamp - datetime.timedelta(
        seconds=CORRELATION_WINDOW)
    search_cosmo_end_datetime = cnv_rainfall_timestamp + datetime.timedelta(
        seconds=CORRELATION_WINDOW)

    cosmo_conductance_template = Template(
        open("../rainfall-event-data/sql/get-cosmo-data-for-rainfall-event.sql.template").read()
    )

    cosmo_conductance_query_sql = cosmo_conductance_template.substitute(
        COSMO_START_DATETIME=search_cosmo_start_datetime,
        COSMO_END_DATETIME=search_cosmo_end_datetime,
        COSMO_SITE=sensor_site
    )

    cursor.execute(cosmo_conductance_query_sql)

    row = cursor.fetchone()

    # there may not always be a correlated measurement
    if row is not None:
        # determine best result

        best_measurement_timestamp = None
        best_measurement_value = None

        # in seconds
        closest_timestamp_distance = 999999

        while row is not None:

            # check if the timestamp in this row is better
            if best_measurement_timestamp is None:
                best_measurement_timestamp = row[0]
                best_measurement_value = float(row[1])
            else:
                timestamp_distance = int(abs((cnv_rainfall_timestamp - row[0]).total_seconds()))

                # is this measurement is closer to the cnv_timestamp than the existing best measurement?
                if timestamp_distance < closest_timestamp_distance:

                    best_measurement_timestamp = row[0]
                    best_measurement_value = float(row[1])

                    logger.debug("Found new best CoSMo measurement %s => %s." %
                                 (best_measurement_timestamp, best_measurement_value))

                    closest_timestamp_distance = timestamp_distance
                else:
                    # this measurement is not closer to the cnv timestamp than the existing best measurement
                    logger.debug("Sticking with existing best CoSMo measurement. Continuing...")

            row = cursor.fetchone()

        if best_measurement_timestamp is not None and best_measurement_value is not None:
            correlated_measurement = (best_measurement_timestamp, best_measurement_value)
        else:
            msg = "Error correlating CoSMo measurement to CNV Timestamp"
            logger.error(msg)
            raise Exception(msg)
    else:
        # no measurement in search window
        # logging here might be noisy given that there will be gaps in measurements
        pass

    return correlated_measurement


# return a dnv whitewater flow reading within the correlation threshold for cnv_timestamp.
# if multiple results in the search window are returned from the database, return the one nearest to the cnv_timestamp
def correlate_with_dnv_flow_reading(cursor, cnv_rainfall_timestamp):
    logger.debug("Attempting to correlate a DNV Whitewater measurement with CNV Rainfall timestamp %s"
                 % cnv_rainfall_timestamp)

    # default measurement is a tuple with None values for measurement timestamp and value
    correlated_measurement = (None, None)

    # limited to 600 results
    dnv_whitewater_template = Template(
        open("../rainfall-event-data/sql/get-dnv-whitewater-data-for-rainfall-event.sql.template").read())

    search_dnv_whitewater_start_datetime = cnv_rainfall_timestamp - datetime.timedelta(
        seconds=CORRELATION_WINDOW)
    search_dnv_whitewater_end_datetime = cnv_rainfall_timestamp + datetime.timedelta(
        seconds=CORRELATION_WINDOW)

    get_dnv_whitewater_query_sql = dnv_whitewater_template.substitute(
        DNV_WHITEWATER_START_DATETIME=search_dnv_whitewater_start_datetime,
        DNV_WHITEWATER_END_DATETIME=search_dnv_whitewater_end_datetime,
        SITE=SOURCE_DB_DNV_WHITEWATER_SITE
    )

    cursor.execute(get_dnv_whitewater_query_sql)

    row = cursor.fetchone()

    # there may not always be a correlated measurement
    if row is not None:

        # determine best result

        best_measurement_timestamp = None
        best_measurement_value = None

        # in seconds
        closest_timestamp_distance = 999999

        while row is not None:

            # check if the timestamp in this row is better
            if best_measurement_timestamp is None:
                best_measurement_timestamp = row[0]
                best_measurement_value = float(row[1])
            else:
                timestamp_distance = int(abs((cnv_rainfall_timestamp - row[0]).total_seconds()))

                # is this measurement is closer to the cnv_timestamp than the existing best measurement?
                if timestamp_distance < closest_timestamp_distance:

                    best_measurement_timestamp = row[0]
                    best_measurement_value = float(row[1])

                    logger.debug("Found new best DNV Whitewater measurement %s => %s." %
                                 (best_measurement_timestamp, best_measurement_value))

                    closest_timestamp_distance = timestamp_distance
                else:
                    # this measurement is not closer to the cnv timestamp than the existing best measurement
                    logger.debug("Sticking with existing best DNV Whitewater measurement. Continuing...")

            row = cursor.fetchone()

        if best_measurement_timestamp is not None and best_measurement_value is not None:
            correlated_measurement = (best_measurement_timestamp, best_measurement_value)
        else:
            msg = "Error correlating DNV Whitewater measurement to CNV Timestamp"
            logger.error(msg)
            raise Exception(msg)
    else:
        # no measurement in search window
        # logging here might be noisy given that there will be gaps in measurements
        pass

    return correlated_measurement


# compile fields to comprise a rainfall event, with rainfall events known
def compile_rainfall_event_data(db_config_filename, db_importer):
    config = DBConfigFactory.build(db_config_filename)

    try:
        with (connect(
                host=config[DBConfig.CONFIG_HOST],
                port=int(config[DBConfig.CONFIG_PORT]),
                user=config[DBConfig.CONFIG_USER],
                password=config[DBConfig.CONFIG_PASS],
                database=config[DBConfig.CONFIG_DBASE]
        ) as connection):
            config[DBConfig.CONFIG_PASS] = None
            config[DBConfig.CONFIG_USER] = None

            # open a connection to the database host
            # we can reuse the cursor so long as each template specifies the database for every table. good practice.

            try:
                with connection.cursor() as cursor:

                    # for each sensor
                    #   for each distinct rainfall event id in the rainfall events table
                    #       create dataentry object
                    #       get start/end dates of event
                    #       populate rainfall and air temperatures within date range
                    #       correlate cosmo conductance data with cnv timestamps within date range
                    #       correlate dnv whitewater data with cnv timestamps within date range

                    # [start_timestamp, end_timestamp, event_id]
                    rainfall_events = load_rainfall_event_intervals(cursor)

                    compiled_measurement_count = 0

                    event_compilation_processing_start_time = timeit.default_timer()
                    for rainfall_event in rainfall_events:
                        (start_datetime, end_datetime, event_id) = rainfall_event

                        compiled_event_data = {}

                        # {CNV_TIMESTAMP_FIELD, CNV_RAINFALL_AMOUNT_FIELD, CNV_AIRTEMP_FIELD}
                        for cnv_rainfall_measurement in load_cnv_rainfall_data(cursor, start_datetime, end_datetime):

                            # pprint(cnsv_rainfall_measurement)

                            # set rainfall event id
                            compiled_event_data[
                                RainfallEventMeasurementsDataEntry.RAINFALL_EVENT_ID_FIELD
                            ] = event_id

                            # retrieve cnv measurement data
                            compiled_event_data[
                                RainfallEventMeasurementsDataEntry.CNV_TIMESTAMP_FIELD
                            ] = cnv_rainfall_measurement.get(
                                RainfallEventMeasurementsDataEntry.CNV_TIMESTAMP_FIELD)

                            compiled_event_data[
                                RainfallEventMeasurementsDataEntry.CNV_RAINFALL_AMOUNT_FIELD
                            ] = cnv_rainfall_measurement.get(
                                RainfallEventMeasurementsDataEntry.CNV_RAINFALL_AMOUNT_FIELD)

                            compiled_event_data[
                                RainfallEventMeasurementsDataEntry.CNV_AIR_TEMPERATURE_FIELD
                            ] = cnv_rainfall_measurement.get(
                                RainfallEventMeasurementsDataEntry.CNV_AIR_TEMPERATURE_FIELD)

                            # correlate dnv whitewater flow reading with cnv rainfall measurement

                            # (timestamp, value)
                            (correlated_flow_timestamp, correlated_flow_value) = correlate_with_dnv_flow_reading(
                                cursor,
                                cnv_rainfall_measurement.get(RainfallEventMeasurementsDataEntry.CNV_TIMESTAMP_FIELD)
                            )

                            compiled_event_data[
                                RainfallEventMeasurementsDataEntry.DNV_WHITEWATER_FLOW_READING_FIELD
                            ] = correlated_flow_value

                            # correlate conductance measurements with cnv rainfall measurement for each site
                            for sensor_site in COSMO_SENSOR_SITES:
                                # Need to set sensor site as destination in DataEntry object
                                # even if no conductance value can be correlated

                                (conductance_timestamp, conductance_value) = correlate_with_cosmo_conductance(
                                    cursor,
                                    sensor_site,
                                    cnv_rainfall_measurement.get(RainfallEventMeasurementsDataEntry.CNV_TIMESTAMP_FIELD)
                                )

                                # each sensor needs its own copy of compiled_event_data
                                final_compiled_event_data = compiled_event_data.copy()

                                final_compiled_event_data[
                                  RainfallEventMeasurementsDataEntry.COSMO_CONDUCTANCE_RESULT_FIELD
                                ] = conductance_value

                                logger.debug("Final compiled event data entry: %s" % final_compiled_event_data)

                                data_entry = RainfallEventMeasurementsDataEntry(final_compiled_event_data)
                                data_entry.set_db_destination(sensor_site)

                                db_importer.add(data_entry)

                                print("\r\tRainfall Event measurements processed: %d" %
                                      compiled_measurement_count, end='', flush=True)

                                compiled_measurement_count += 1

                    event_compilation_processing_elapsed_time = (timeit.default_timer() -
                                                                 event_compilation_processing_start_time
                                                                 )

                    log_msg = ("Completed rainfall event compilation Processing in %.3f sec" %
                               event_compilation_processing_elapsed_time
                               )
                    print("\n%s" % log_msg, flush=True)
                    logger.info(log_msg)

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
    print("Precheck passed. Running rainfall event compilation...")

    # build the importer. target database is defined in the config
    db_importer = DBImporter(db_config_filename)
    db_importer.set_importer_name("rainfall-events")
    db_importer.set_schema(SCHEMA)

    compile_rainfall_event_data(db_config_filename, db_importer)

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
    # -cfg rainfall-event-data.json                          database config     not required
    ############################

    # reads sys.argv
    parser = argparse.ArgumentParser(
        description='Compute rainfall event data consolidation and store results in the database.')
    parser.add_argument('--dry-run', action='store_const', const=1, dest='dryrun',
                        help='Output database insert statements. Does not write to database.')
    parser.add_argument('-cfg', nargs=1, dest='db_cfg_file',
                        help='Database config file in json format. Ex: rainfall-event-data.json')

    # call main with parsed args
    main(parser.parse_args())
