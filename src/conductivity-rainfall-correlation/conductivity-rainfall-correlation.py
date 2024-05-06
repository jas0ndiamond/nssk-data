from mysql.connector import connect, Error, IntegrityError
from datetime import datetime

from pathlib import Path
import sys
import argparse
import logging
import timeit

from src.importer.DBImporter import DBImporter

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

from src.importer.DBConfig import DBConfig
from src.importer.DBConfigFactory import DBConfigFactory

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
    "WAGG02",
    "WAGG03"
]

SCHEMA = [
    "CosmoTimeStamp",
    "Conductance",
    "RainfallAmount"
]

SOURCE_DB_NSSK_COSMO = "NSSK_COSMO"
SOURCE_DB_CNV_RAINFALL = "NSSK_CNV_RAINFALL"


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

            except Error as e:
                logger.error("Error checking databases", e)
    except Error as e:
        logger.error("Error connecting to database", e)

    # check 1
    # check that the source databases exist


def run_correlation(sensor_name, db_importer):



    # determine start datetime (earliest conductivity measurement)
    # cosmo datetimes are split across date and time fields

    # determine end datetime (most recent conductivity measurement)
    # cosmo datetimes are split across date and time fields

    # resolve conductivity values for times in the sensor table

    # new or update? => always new, note duplicates and check measurement differences

    correlated_value_count = 0

    # iterate over cosmo timestamps and resolved values
    # resolved dates +/- resolution window
    # do any math to take or compute the conductivity
    # if no more cosmo timestamps for a time interval
    # get the next interval
    # repeat as above

    pass


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
        run_correlation(sensor, db_importer)


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
