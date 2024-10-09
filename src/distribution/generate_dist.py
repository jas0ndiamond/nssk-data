# dump database tables into csv files, zip up for deployment onto nssk.jas0n.ca
import re
import shutil
import zipfile
from os.path import exists
from string import Template
from time import strftime, localtime
# dist html page
# links to each zipped html file
# link to zip file containing all other zip files
from zipfile import ZipFile
import argparse
import csv
import sys
import os
from mysql.connector import connect, Error
from pathlib import Path

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

from src.importer.DBConfigFactory import DBConfigFactory, DBConfig

########################
DEFAULT_CONFIG_FILE = "./conf/config.json"

FETCH_SIZE = 5000

########################
# database and tables

COSMO_DB = "NSSK_COSMO"
COSMO_TIMESTAMP_FIELD = ""
COSMO_SITES = [
    "WAGG01",
    "WAGG03"
]

CNV_RAINFALL_DB = "NSSK_CNV_RAINFALL"
CNV_RAINFALL_TIMESTAMP_FIELD = "MeasurementTimestamp"
CNV_RAINFALL_SITES = [
    "CNV"
]

DNV_WHITEWATER_DB = "NSSK_DNV_WHITEWATER"
DNV_WHITEWATER_TIMESTAMP_FIELD = "MeasurementTimestamp"
DNV_WHITEWATER_SITES = [
    "DNV"
]

NSSK_CONDUCTIVITY_RAINFALL_CORRELATION_DB = "NSSK_CONDUCTIVITY_RAINFALL_CORRELATION"
NSSK_CONDUCTIVITY_RAINFALL_CORRELATION_TIMESTAMP_FIELD = "CosmoTimeStamp"
NSSK_CONDUCTIVITY_RAINFALL_CORRELATION_SITES = [
    "WAGG01",
    "WAGG03"
]

NSSK_RAINFALL_EVENTS_DB = "NSSK_RAINFALL_EVENT_DATA"
NSSK_RAINFALL_EVENTS_TIMESTAMP_FIELD = "EVENT_START_TIMESTAMP"
NSSK_RAINFALL_EVENTS_TABLE = "RAINFALL_EVENTS"

NSSK_RAINFALL_EVENT_DATA_DB = "NSSK_RAINFALL_EVENT_DATA"
NSSK_RAINFALL_EVENT_DATA_TIMESTAMP_FIELD = "CNV_RAINFALL_TIMESTAMP"
NSSK_RAINFALL_EVENT_DATA_SITES = [
    "WAGG01",
    "WAGG03"
]

########################
# html file indexing everything
DUMP_HTML_FILE = "index.html"
DUMP_HTML_TITLE = "NSSK Database Dumps"
DIST_FILE = "nssk-data-dist.zip"
TEMP_DIR = "./tmp/"
FAVICON_DIR = "./res/favicon/"

########################
# dump files
# TODO move all this to a json file

DUMP_FILES_CNV_RAINFALL = {
    "CNV": "nssk_cnv_rainfall.csv"
}

DUMP_FILES_DNV_WHITEWATER = {
    "DNV": "nssk_dnv_whitewater.csv"
}

# dump file for each CoSMo site

DUMP_FILES_COSMO = {
    "HAST01": "nssk_cosmo.HAST01.csv",
    "HAST02": "nssk_cosmo.HAST02.csv",
    "HAST03": "nssk_cosmo.HAST03.csv",

    "MACK02": "nssk_cosmo.MACK02.csv",
    "MACK03": "nssk_cosmo.MACK03.csv",
    "MACK04": "nssk_cosmo.MACK04.csv",
    "MACK05": "nssk_cosmo.MACK05.csv",

    "MISS01": "nssk_cosmo.MISS01.csv",

    "MOSQ02": "nssk_cosmo.MOSQ02.csv",
    "MOSQ03": "nssk_cosmo.MOSQ03.csv",
    "MOSQ04": "nssk_cosmo.MOSQ04.csv",
    "MOSQ05": "nssk_cosmo.MOSQ05.csv",

    "WAGG01": "nssk_cosmo.WAGG01.csv",
    "WAGG02": "nssk_cosmo.WAGG02.csv",
    "WAGG03": "nssk_cosmo.WAGG03.csv",
}

DUMP_FILES_CONDUCTIVITY_RAINFALL_CORRELATION = {
    "WAGG01": "nssk_conductivity_rainfall_correlation.WAGG01.csv",
    "WAGG03": "nssk_conductivity_rainfall_correlation.WAGG03.csv",
}

DUMP_FILES_RAINFALL_EVENTS = {
    "CNV": "nssk_rainfall_events.csv"
}

DUMP_FILES_RAINFALL_EVENT_DATA = {
    "WAGG01": "nssk_rainfall_event_data.WAGG01.csv",
    "WAGG03": "nssk_rainfall_event_data.WAGG03.csv"
}


##############################

def precheck(db_config_filename):
    # need a destination database/table for the correlated data
    # database created by setup script? => yes
    # table created by setup script? => yes

    print("Running precheck")

    config = DBConfigFactory.build(db_config_filename)

    try:
        with (connect(
                host=config[DBConfig.CONFIG_HOST],
                port=int(config[DBConfig.CONFIG_PORT]),
                user=config[DBConfig.CONFIG_USER],
                password=config[DBConfig.CONFIG_PASS]
        ) as connection):
            config[DBConfig.CONFIG_PASS] = None
            config[DBConfig.CONFIG_USER] = None

            try:
                with connection.cursor() as cursor:

                    # check databases/tables exist. exception otherwise
                    pass

            except Error as e:
                print("Error checking databases", e)
    except Error as e:
        print("Error connecting to database", e)


def run_dump(db_config_filename):
    config = DBConfigFactory.build(db_config_filename)

    success = False

    # make temp dir if it doesn't exist
    if not os.path.isdir(TEMP_DIR):
        os.makedirs(TEMP_DIR)

    try:
        with (connect(
                host=config[DBConfig.CONFIG_HOST],
                port=int(config[DBConfig.CONFIG_PORT]),
                user=config[DBConfig.CONFIG_USER],
                password=config[DBConfig.CONFIG_PASS]
        ) as connection):
            config[DBConfig.CONFIG_PASS] = None
            config[DBConfig.CONFIG_USER] = None

            try:
                with connection.cursor() as cursor:

                    print("Dumping CNV Rainfall")
                    dump_cnv_rainfall(cursor)

                    print("Dumping DNV Whitewater")
                    dump_dnv_whitewater(cursor)

                    print("Dumping CoSMo")
                    dump_cosmo(cursor)

                    # dump_cosmo_correlation
                    print("Dumping Conductivity-Rainfall Correlation")
                    dump_conductivity_rainfall_correlation(cursor)

                    # dump rainfall events
                    print("Dumping Rainfall Events")
                    dump_rainfall_events(cursor)

                    # dump rainfall event data
                    print("Dumping Rainfall Event Data")
                    dump_rainfall_event_data(cursor)

                    success = True

            except Error as e:
                print("Error checking databases", e)
    except Error as e:
        print("Error connecting to database", e)

    return success


def dump_cnv_rainfall(cursor):
    for site in DUMP_FILES_CNV_RAINFALL:
        # manually write header
        cursor.execute("describe %s.%s" % (CNV_RAINFALL_DB, site))
        rows = cursor.fetchall()

        schema = []
        for row in rows:
            schema.append(row[0])

        # dump table contents
        with (open(TEMP_DIR + DUMP_FILES_CNV_RAINFALL[site], 'w') as writer):
            csv_writer = csv.writer(writer, quoting=csv.QUOTE_ALL)
            csv_writer.writerow(schema)

            query = "SELECT * FROM %s.%s ORDER BY %s ASC" % (CNV_RAINFALL_DB, site, CNV_RAINFALL_TIMESTAMP_FIELD)

            # print("Query: %s" % query)
            cursor.execute(query)

            rows = cursor.fetchmany(FETCH_SIZE)

            while rows is not None and rows:
                csv_writer.writerows(rows)
                rows = cursor.fetchmany(FETCH_SIZE)


def dump_dnv_whitewater(cursor):
    for site in DUMP_FILES_DNV_WHITEWATER:
        # manually write header
        cursor.execute("describe %s.%s" % (DNV_WHITEWATER_DB, site))
        rows = cursor.fetchall()

        schema = []
        for row in rows:
            schema.append(row[0])

        # dump table contents
        with (open(TEMP_DIR + DUMP_FILES_DNV_WHITEWATER[site], 'w') as writer):
            csv_writer = csv.writer(writer, quoting=csv.QUOTE_ALL)
            csv_writer.writerow(schema)

            query = (
                    "SELECT * FROM %s.%s ORDER BY %s ASC" %
                    (DNV_WHITEWATER_DB, site, DNV_WHITEWATER_TIMESTAMP_FIELD)
            )

            # print("Query: %s" % query)
            cursor.execute(query)

            rows = cursor.fetchmany(FETCH_SIZE)

            while rows is not None and rows:
                csv_writer.writerows(rows)
                rows = cursor.fetchmany(FETCH_SIZE)


def dump_cosmo(cursor):
    for site in DUMP_FILES_COSMO:
        # manually write header
        cursor.execute("describe %s.%s" % (COSMO_DB, site))
        rows = cursor.fetchall()

        schema = []
        for row in rows:
            schema.append(row[0])

        # dump table contents
        with (open(TEMP_DIR + DUMP_FILES_COSMO[site], 'w') as writer):
            csv_writer = csv.writer(writer, quoting=csv.QUOTE_ALL)
            csv_writer.writerow(schema)

            query = "select * from %s.%s ORDER BY " % (COSMO_DB, site)
            query += "CAST(CONCAT_WS(' ', %s.%s.ActivityStartDate, %s.%s.ActivityStartTime) as DATETIME) ASC" % (
                COSMO_DB, site, COSMO_DB, site
            )

            # print("Query: %s" % query)
            cursor.execute(query)

            rows = cursor.fetchmany(FETCH_SIZE)

            while rows is not None and rows:
                csv_writer.writerows(rows)
                rows = cursor.fetchmany(FETCH_SIZE)


def dump_conductivity_rainfall_correlation(cursor):
    for site in NSSK_CONDUCTIVITY_RAINFALL_CORRELATION_SITES:
        # manually write header
        cursor.execute("describe %s.%s" % (NSSK_CONDUCTIVITY_RAINFALL_CORRELATION_DB, site))
        rows = cursor.fetchall()

        schema = []
        for row in rows:
            schema.append(row[0])

        # dump table contents
        with (open("%s%s" % (TEMP_DIR, DUMP_FILES_CONDUCTIVITY_RAINFALL_CORRELATION[site]), 'w') as writer):
            csv_writer = csv.writer(writer, quoting=csv.QUOTE_ALL)
            csv_writer.writerow(schema)

            query = "select * from %s.%s ORDER BY %s ASC" % (
                NSSK_CONDUCTIVITY_RAINFALL_CORRELATION_DB,
                site,
                NSSK_CONDUCTIVITY_RAINFALL_CORRELATION_TIMESTAMP_FIELD
            )

            # print("Query: %s" % query)
            cursor.execute(query)

            rows = cursor.fetchmany(FETCH_SIZE)

            while rows is not None and rows:
                csv_writer.writerows(rows)
                rows = cursor.fetchmany(FETCH_SIZE)


def dump_rainfall_events(cursor):
    # manually write header
    cursor.execute("describe %s.%s" % (NSSK_RAINFALL_EVENTS_DB, NSSK_RAINFALL_EVENTS_TABLE))
    rows = cursor.fetchall()

    schema = []
    for row in rows:
        schema.append(row[0])

    # dump table contents
    with (open("%s%s" % (TEMP_DIR, DUMP_FILES_RAINFALL_EVENTS["CNV"]), 'w') as writer):
        csv_writer = csv.writer(writer, quoting=csv.QUOTE_ALL)
        csv_writer.writerow(schema)

        query = "select * from %s.%s ORDER BY %s ASC" % (
            NSSK_RAINFALL_EVENTS_DB,
            NSSK_RAINFALL_EVENTS_TABLE,
            NSSK_RAINFALL_EVENTS_TIMESTAMP_FIELD
        )

        # print("Query: %s" % query)
        cursor.execute(query)

        rows = cursor.fetchmany(FETCH_SIZE)

        while rows is not None and rows:
            csv_writer.writerows(rows)
            rows = cursor.fetchmany(FETCH_SIZE)


def dump_rainfall_event_data(cursor):

    for site in DUMP_FILES_RAINFALL_EVENT_DATA:

        # manually write header
        cursor.execute("describe %s.%s" % (NSSK_RAINFALL_EVENTS_DB, site))
        rows = cursor.fetchall()

        schema = []
        for row in rows:
            schema.append(row[0])

        # dump table contents
        with (open("%s%s" % (TEMP_DIR, DUMP_FILES_RAINFALL_EVENT_DATA[site]), 'w') as writer):
            csv_writer = csv.writer(writer, quoting=csv.QUOTE_ALL)
            csv_writer.writerow(schema)

            query = "select * from %s.%s ORDER BY %s ASC" % (
                NSSK_RAINFALL_EVENTS_DB,
                site,
                NSSK_RAINFALL_EVENT_DATA_TIMESTAMP_FIELD
            )

            # print("Query: %s" % query)
            cursor.execute(query)

            rows = cursor.fetchmany(FETCH_SIZE)

            while rows is not None and rows:
                csv_writer.writerows(rows)
                rows = cursor.fetchmany(FETCH_SIZE)


def zip_dump_files():
    # zip expected csv files in tmp/

    for site in DUMP_FILES_CNV_RAINFALL:
        zip_dump_file(DUMP_FILES_CNV_RAINFALL[site])

    for site in DUMP_FILES_DNV_WHITEWATER:
        zip_dump_file(DUMP_FILES_DNV_WHITEWATER[site])

    for site in DUMP_FILES_COSMO:
        zip_dump_file(DUMP_FILES_COSMO[site])

    for site in DUMP_FILES_CONDUCTIVITY_RAINFALL_CORRELATION:
        zip_dump_file(DUMP_FILES_CONDUCTIVITY_RAINFALL_CORRELATION[site])

    for site in DUMP_FILES_RAINFALL_EVENTS:
        zip_dump_file(DUMP_FILES_RAINFALL_EVENTS[site])

    for site in DUMP_FILES_RAINFALL_EVENT_DATA:
        zip_dump_file(DUMP_FILES_RAINFALL_EVENT_DATA[site])


# zip a dump file in its own archive at archive root level
def zip_dump_file(dump_file):
    file = "%s%s" % (TEMP_DIR, dump_file)

    # check file exists
    if exists(file):
        print("Compressing dump file: %s" % dump_file)
        with ZipFile("%s.zip" % file, 'w', zipfile.ZIP_DEFLATED) as zip_h:
            zip_h.write(file, arcname="./%s" % dump_file)

        # remove source csv file
        os.remove(file)
    else:
        print("ERROR: encountered missing csv dump file when attempting compression: %s" % file)


def write_html_file():
    html_template = Template(open("templates/index.html.template").read())

    resource_entry_template = Template(open("templates/resource-entry.template.html").read())
    section_block_template = Template(open("templates/section-block.html.template").read())

    ##########
    # cnv

    cnv_rainfall_section_body = ""
    for name in DUMP_FILES_CNV_RAINFALL:
        zip_file = "%s.zip" % DUMP_FILES_CNV_RAINFALL[name]

        # file in the work dir (./tmp/file.csv.zip)
        zip_file_in_dist = "%s/%s" % (TEMP_DIR, zip_file)

        # http link to file deployed on webserver (./file.csv.zip)
        zip_file_link = "./%s" % zip_file

        cnv_rainfall_section_body += resource_entry_template.substitute(
            FILE=zip_file,
            LINK=zip_file_link,
            NAME=DUMP_FILES_CNV_RAINFALL[name],
            DESC="CNV Rainfall Description",
            SIZE="%.3f MB" % (os.path.getsize(zip_file_in_dist) / 1000000),
            CREATION_DATE=strftime('%Y-%m-%d %H:%M:%S', localtime(os.path.getctime(zip_file_in_dist)))
        )

    cnv_rainfall_section_block = section_block_template.substitute(
        SECTION_BODY=cnv_rainfall_section_body
    )

    ##########
    # dnv whitewater

    dnv_whitewater_section_body = ""
    for name in DUMP_FILES_DNV_WHITEWATER:
        zip_file = "%s.zip" % DUMP_FILES_DNV_WHITEWATER[name]

        # file in the work dir (./tmp/file.csv.zip)
        zip_file_in_dist = "%s/%s" % (TEMP_DIR, zip_file)

        # http link to file deployed on webserver (./file.csv.zip)
        zip_file_link = "./%s" % zip_file

        dnv_whitewater_section_body += resource_entry_template.substitute(
            FILE=zip_file,
            LINK=zip_file_link,
            NAME=DUMP_FILES_DNV_WHITEWATER[name],
            DESC="DNV Whitewater Description",
            SIZE="%.3f MB" % (os.path.getsize(zip_file_in_dist) / 1000000),
            CREATION_DATE=strftime('%Y-%m-%d %H:%M:%S', localtime(os.path.getctime(zip_file_in_dist)))
        )

    dnv_whitewater_section_block = section_block_template.substitute(
        SECTION_BODY=dnv_whitewater_section_body
    )

    ##########
    # CoSMo
    cosmo_section_body = ""
    for name in DUMP_FILES_COSMO:
        zip_file = "%s.zip" % DUMP_FILES_COSMO[name]

        # file in the work dir (./tmp/file.csv.zip)
        zip_file_in_dist = "%s/%s" % (TEMP_DIR, zip_file)

        # http link to file deployed on webserver (./file.csv.zip)
        zip_file_link = "./%s" % zip_file

        cosmo_section_body += resource_entry_template.substitute(
            FILE=zip_file,
            LINK=zip_file_link,
            NAME=DUMP_FILES_COSMO[name],
            DESC="CoSMo Site %s Description" % name,
            SIZE="%.3f MB" % (os.path.getsize(zip_file_in_dist) / 1000000),
            CREATION_DATE=strftime('%Y-%m-%d %H:%M:%S', localtime(os.path.getctime(zip_file_in_dist)))
        )

    cosmo_section_block = section_block_template.substitute(
        SECTION_BODY=cosmo_section_body
    )

    ##########
    # Conductivity-rainfall correlation

    conductivity_rainfall_correlation_section_body = ""
    for name in DUMP_FILES_CONDUCTIVITY_RAINFALL_CORRELATION:
        zip_file = "%s.zip" % DUMP_FILES_CONDUCTIVITY_RAINFALL_CORRELATION[name]

        # file in the work dir (./tmp/file.csv.zip)
        zip_file_in_dist = "%s/%s" % (TEMP_DIR, zip_file)

        # http link to file deployed on webserver (./file.csv.zip)
        zip_file_link = "./%s" % zip_file

        conductivity_rainfall_correlation_section_body += resource_entry_template.substitute(
            FILE=zip_file,
            LINK=zip_file_link,
            NAME=DUMP_FILES_CONDUCTIVITY_RAINFALL_CORRELATION[name],
            DESC="C/R CoSMo Site %s Description" % name,
            SIZE="%.3f MB" % (os.path.getsize(zip_file_in_dist) / 1000000),
            CREATION_DATE=strftime('%Y-%m-%d %H:%M:%S', localtime(os.path.getctime(zip_file_in_dist)))
        )

    conductivity_rainfall_correlation_section_block = section_block_template.substitute(
        SECTION_BODY=conductivity_rainfall_correlation_section_body
    )

    ##########
    # Rainfall Events

    rainfall_events_section_body = ""
    for name in DUMP_FILES_RAINFALL_EVENTS:
        zip_file = "%s.zip" % DUMP_FILES_RAINFALL_EVENTS[name]

        # file in the work dir (./tmp/file.csv.zip)
        zip_file_in_dist = "%s/%s" % (TEMP_DIR, zip_file)

        # http link to file deployed on webserver (./file.csv.zip)
        zip_file_link = "./%s" % zip_file

        rainfall_events_section_body += resource_entry_template.substitute(
            FILE=zip_file,
            LINK=zip_file_link,
            NAME=DUMP_FILES_RAINFALL_EVENTS[name],
            DESC="Rainfall Events for %s Description" % name,
            SIZE="%.3f MB" % (os.path.getsize(zip_file_in_dist) / 1000000),
            CREATION_DATE=strftime('%Y-%m-%d %H:%M:%S', localtime(os.path.getctime(zip_file_in_dist)))
        )

    rainfall_events_section_block = section_block_template.substitute(
        SECTION_BODY=rainfall_events_section_body
    )
    ##########
    # Rainfall Event Data

    rainfall_event_data_section_body = ""
    for name in DUMP_FILES_RAINFALL_EVENT_DATA:
        zip_file = "%s.zip" % DUMP_FILES_RAINFALL_EVENT_DATA[name]

        # file in the work dir (./tmp/file.csv.zip)
        zip_file_in_dist = "%s/%s" % (TEMP_DIR, zip_file)

        # http link to file deployed on webserver (./file.csv.zip)
        zip_file_link = "./%s" % zip_file

        rainfall_event_data_section_body += resource_entry_template.substitute(
            FILE=zip_file,
            LINK=zip_file_link,
            NAME=DUMP_FILES_RAINFALL_EVENT_DATA[name],
            DESC="Rainfall Event Data for %s Description" % name,
            SIZE="%.3f MB" % (os.path.getsize(zip_file_in_dist) / 1000000),
            CREATION_DATE=strftime('%Y-%m-%d %H:%M:%S', localtime(os.path.getctime(zip_file_in_dist)))
        )

    rainfall_event_data_section_block = section_block_template.substitute(
        SECTION_BODY=rainfall_event_data_section_body
    )

    ##############################
    dist_html_page = html_template.substitute(
        CNV_RAINFALL_BLOCK=cnv_rainfall_section_block,
        DNV_WHITEWATER_BLOCK=dnv_whitewater_section_block,
        COSMO_BLOCK=cosmo_section_block,
        CONDUCTIVITY_RAINFALL_CORRELATION_BLOCK=conductivity_rainfall_correlation_section_block,
        RAINFALL_EVENTS_BLOCK=rainfall_events_section_block,
        RAINFALL_EVENT_DATA_BLOCK=rainfall_event_data_section_block
    )

    # write everything to file
    with open("%s%s" % (TEMP_DIR, DUMP_HTML_FILE), 'w') as writer:
        writer.write(dist_html_page)


# create the dist from intermediate resources. this file gets deployed on some service medium
def create_dist():
    print("Building distribution from zipped dump files and resources")

    # create a zip file at "./" from:
    # zip files in tmp
    # tmp/index.html

    # copy resources from source dirs to work dir
    # res/robots.txt
    shutil.copyfile("./res/robots.txt", "%s/robots.txt" % TEMP_DIR)

    # res/nssk-banner
    shutil.copyfile("./res/nssk-banner.png", "%s/nssk-banner.png" % TEMP_DIR)

    # res/favicos
    favicon_files = os.listdir(FAVICON_DIR)
    for favicon_file in favicon_files:
        shutil.copy2("%s/%s" % (FAVICON_DIR, favicon_file), TEMP_DIR)

    # assemble zip file
    with ZipFile("./%s" % DIST_FILE, 'w', zipfile.ZIP_DEFLATED) as zip_h:
        for root, dirs, files in os.walk(TEMP_DIR):
            for file in files:
                print("Adding file to distribution: %s" % file)
                zip_h.write("%s/%s" % (TEMP_DIR, file), arcname="./%s" % file)


def cleanup():
    print("Cleaning up temporary resources")
    shutil.rmtree(TEMP_DIR)


##############################

def main(parsed_args):
    db_config_filename = None

    # handle parsed arguments
    if getattr(parsed_args, "db_cfg_file") is not None:
        db_config_filename = getattr(parsed_args, "db_cfg_file")[0]

    if db_config_filename is None:
        print("Error could not determine database config filename. Exiting...")
        exit(1)

    # TODO: switch on -k shell arg to skip cleanup. default is to run cleanup
    run_cleanup = False

    print("Creating distribution...")

    precheck(db_config_filename)

    dump_created = run_dump(db_config_filename)

    if dump_created:
        # generate zip file
        # html file linking to csv files
        # csv files
        zip_dump_files()
        write_html_file()
        create_dist()

        if run_cleanup:
            print("Cleaning up temp resources")
            cleanup()
        else:
            print("Skipping cleanup")
    else:
        print("Encountered error. ")


##############################
if __name__ == "__main__":
    ############################
    # shell args
    #
    # -cfg conductivity-rainfall-correlation.json            database config     not required
    ############################

    # reads sys.argv
    parser = argparse.ArgumentParser(
        description='Create a distribution for NSSK data dumps.')
    parser.add_argument('-cfg', nargs=1, dest='db_cfg_file',
                        help='Database config file in json format.')

    # call main with parsed args
    main(parser.parse_args())
