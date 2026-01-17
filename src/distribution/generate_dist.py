# dump database tables into csv files, zip up for deployment onto nssk.jas0n.ca
import argparse
import csv
import os
import shutil
import sys
import zipfile
from datetime import datetime
from os.path import exists
from pathlib import Path
from string import Template
from time import strftime, localtime
# dist html page
# links to each zipped html file
# link to zip file containing all other zip files
from zipfile import ZipFile

from mysql.connector import connect, Error
from xlsxwriter import Workbook

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

from src.importer.DBConfigFactory import DBConfigFactory, DBConfig

########################
# constants

DEFAULT_CONFIG_FILE = "conf/dist.prod.json"

FETCH_SIZE = 5000

SPECIFIC_CONDUCTANCE_THRESHOLD = 500

BYTES_IN_MB = 1000000

########################
# database and tables

# TODO: sites lists don't seem to be used. use dump file dicts instead?

NSSK_COSMO_DB = "NSSK_COSMO"

NSSK_CNV_FLOWWORKS_DB = "NSSK_CNV_FLOWWORKS"
NSSK_CNV_FLOWWORKS_TIMESTAMP_FIELD = "MeasurementTimestamp"

NSSK_DNV_FLOWWORKS_DB = "NSSK_DNV_FLOWWORKS"
NSSK_DNV_FLOWWORKS_TIMESTAMP_FIELD = "MeasurementTimestamp"

NSSK_CONDUCTIVITY_RAINFALL_CORRELATION_DB = "NSSK_CONDUCTIVITY_RAINFALL_CORRELATION"
NSSK_CONDUCTIVITY_RAINFALL_CORRELATION_TIMESTAMP_FIELD = "CosmoTimeStamp"

NSSK_RAINFALL_EVENTS_DB = "NSSK_RAINFALL_EVENT_DATA"
NSSK_RAINFALL_EVENTS_TIMESTAMP_FIELD = "EVENT_START_TIMESTAMP"
NSSK_RAINFALL_EVENTS_TABLE = "RAINFALL_EVENTS"

NSSK_RAINFALL_EVENT_DATA_DB = "NSSK_RAINFALL_EVENT_DATA"
NSSK_RAINFALL_EVENT_DATA_TIMESTAMP_FIELD = "CNV_FLOWWORKS_TIMESTAMP"

NSSK_CNV_HYDROMETRIC_DB = "NSSK_CNV_HYDROMETRIC"

NSSK_RAINFALL_INTERVAL_DATA_DB = "NSSK_RAINFALL_INTERVAL_DATA"

NSSK_WATERRANGERS_DB = "NSSK_WATERRANGERS"
NSSK_WATERRANGERS_TIMESTAMP_FIELD = "ObservedOn"

########################
# html file indexing everything
DUMP_HTML_FILE = "index.html"
DUMP_HTML_TITLE = "NSSK Database Dumps"
DIST_FILE = "nssk-data-dist.zip"
TEMP_DIR = "./tmp/"
FAVICON_DIR = "./res/favicon/"

########################
# rainfall interval data yearly stuff

RAINFALL_INTERVAL_START_YEAR = 2020

########################
# date format
MYSQL_DT_FMT = "%Y-%m-%d %H:%M:%S"
FILE_DT_FMT = "%Y%m%d_%H%M%S"

########################
# dump files
# TODO move all this to a json file

DUMP_FILES_CNV_FLOWWORKS = {
    "CNVRain": "nssk_cnv_flowworks.csv"
}

DUMP_FILES_DNV_FLOWWORKS = {
    "DNV": "nssk_dnv_flowworks.csv"
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

DUMP_FILES_RAINFALL_EVENT_DATA_AGGREGATE = {
    "WAGG01": "nssk_rainfall_event_data_aggregate.WAGG01.csv",
    "WAGG03": "nssk_rainfall_event_data_aggregate.WAGG03.csv"
}

DUMP_FILES_CNV_HYDROMETRIC = {
    "WaggCreek": "nssk_cnv_hydrometric.WaggCreek.csv"
}

DUMP_FILES_CHLORIDE_ACUITY = {
    "WAGG01": "nssk_chloride_acuity.WAGG01.csv",
    "WAGG03": "nssk_chloride_acuity.WAGG03.csv"
}

DUMP_FILES_RAINFALL_INTERVAL_DATA = {
    "WAGG01": "nssk_rainfall_interval_data.WAGG01.csv",
    "WAGG03": "nssk_rainfall_interval_data.WAGG03.csv",
}

# sites mapped to file prefixes
DUMP_FILES_RAINFALL_INTERVAL_TIMEBOUND_DATA = {
    "WAGG01": "nssk_rainfall_interval_data",
    "WAGG03": "nssk_rainfall_interval_data",
}

# flat list of generated csv files for the various time intervals
RAINFALL_INTERVAL_TIMEBOUND_GEN_CSV_FILES = []

# list of generated xlsx files from the csv files in RAINFALL_INTERVAL_TIMEBOUND_GEN_CSV_FILES
RAINFALL_INTERVAL_TIMEBOUND_GEN_XLSX_FILES = []

XLSX_RAINFALL_INTERVAL_DATA = {
    "All": "nssk_rainfall_interval_data.all.xlsx",
}

# sites mapped to file prefixes
XLSX_RAINFALL_INTERVAL_TIMEBOUND_DATA = {
    "All": "nssk_rainfall_interval_data",
}

DUMP_FILES_WATERRANGERS = {
    "MIS_M_01": "nssk_waterrangers_mis_m_01.csv",
    "MIS_E_01": "nssk_waterrangers_mis_e_01.csv",
    "MIS_W_01": "nssk_waterrangers_mis_w_01.csv",
    "MOS_M_01": "nssk_waterrangers_mos_m_01.csv",
    "WAG_E_01": "nssk_waterrangers_wag_e_01.csv",
    "WAG_E_02": "nssk_waterrangers_wag_e_02.csv",
    "WAG_E_03": "nssk_waterrangers_wag_e_03.csv",
    "WAG_E_05": "nssk_waterrangers_wag_e_05.csv",
    "WAG_E_06a": "nssk_waterrangers_wag_e_06a.csv",
    "WAG_E_06b": "nssk_waterrangers_wag_e_06b.csv",
    "WAG_E_07": "nssk_waterrangers_wag_e_07.csv",
    "WAG_M_01": "nssk_waterrangers_wag_m_01.csv",
    "WAG_M_02": "nssk_waterrangers_wag_m_02.csv",
    "WAG_M_03": "nssk_waterrangers_wag_m_03.csv",
    "WAG_W_02a": "nssk_waterrangers_wag_w_02a.csv",
    "WAG_W_02b": "nssk_waterrangers_wag_w_02b.csv",
    "WAG_W_03": "nssk_waterrangers_wag_w_03.csv"
}

# the file creation time for dist resources so they can be more easily associated with a single point in time
DIST_TIMESTAMP = datetime.now()

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

                    # TODO: check databases/tables exist. exception otherwise
                    # what if this is a botched install?
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

                    print("Dumping CNV Flowworks")
                    dump_cnv_flowworks(cursor)

                    print("Dumping DNV Flowworks")
                    dump_dnv_flowworks(cursor)

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

                    # dump rainfall event totals
                    # needs to execute after rainfall event data
                    print("Dumping Rainfall Event Data Aggregate")
                    dump_rainfall_event_data_aggregate(cursor)

                    # dump rainfall interval data
                    print("Dumping Rainfall Interval Data")
                    dump_rainfall_interval_data(cursor)

                    # dump rainfall interval data
                    print("Dumping Rainfall Interval Data - Yearly")

                    # populate ranges for rainfall interval data starting from july 2020
                    # interval starts july 01 00:00:00, and ends either one year later or right now

                    current_year = DIST_TIMESTAMP.strftime("%Y")

                    print(("\tGenerating Rainfall Interval Yearly resources"
                        f" from {RAINFALL_INTERVAL_START_YEAR} to {current_year}"
                    ))

                    # this will typically end at the upcoming july
                    for i in range(RAINFALL_INTERVAL_START_YEAR, int(current_year)):
                        start_datetime = datetime.strptime(f"{i}-07-01 00:00:00", MYSQL_DT_FMT)
                        end_datetime = datetime.strptime(f"{i+1}-07-01 00:00:00", MYSQL_DT_FMT)

                        if end_datetime > DIST_TIMESTAMP:
                            # mostly for file naming: the database can handle future dates,
                            # which won't have measurements
                            end_datetime = DIST_TIMESTAMP

                        print(f"\t\tAdding range: {start_datetime.strftime(MYSQL_DT_FMT)} -> {end_datetime.strftime(MYSQL_DT_FMT)}")
                        dump_rainfall_interval_timebound(cursor, start_datetime, end_datetime)

                    # dump cnv hydrometric
                    print("Dumping CNV Hydrometric")
                    dump_cnv_hydrometric_data(cursor)

                    # dump chloride acuity
                    print("Dumping Chloride Acuity")
                    dump_chloride_acuity(cursor)

                    # dump waterrangers
                    print("Dumping Waterrangers")
                    dump_waterrangers(cursor)

                    success = True

            except Error as e:
                print("Error checking databases", e)
    except Error as e:
        print("Error connecting to database", e)

    return success

def dump_rainfall_interval_data(cursor):
    for site in DUMP_FILES_RAINFALL_INTERVAL_DATA:
        query_template = Template(open("templates/sql/rainfall-interval-data.sql.template").read())

        cursor.execute("describe %s.%s" % (NSSK_RAINFALL_INTERVAL_DATA_DB, site))

        schema = []
        for row in  cursor.fetchall():
            schema.append(row[0])

        csv_file = TEMP_DIR + DUMP_FILES_RAINFALL_INTERVAL_DATA[site]

        print("\tWriting file %s" % csv_file)

        # dump table contents
        with (open(csv_file, 'w') as writer):
            csv_writer = csv.writer(writer, quoting=csv.QUOTE_ALL)

            # write our schema
            csv_writer.writerow(schema)

            query = query_template.substitute(
                DB=NSSK_RAINFALL_INTERVAL_DATA_DB,
                SITE=site
            )

            # print("Query: %s" % query)
            cursor.execute(query)

            rows = cursor.fetchmany(FETCH_SIZE)

            # write csv file
            while rows is not None and rows:
                csv_writer.writerows(rows)
                rows = cursor.fetchmany(FETCH_SIZE)

    #########################################
    # all sites processed, create the consolidated xlsx file out of the component csv files
    # each site name should span all columns
    # time series data all side by side. no correlation across sites
    ######### Site 1 ############|######### Site 2 ############|
    # site1-field1,site1-field2,site1-field3,site2-field1,site2-field2,site2-field3
    # ...
    # will not be parsable csv

    interval_data_file_consolidated = TEMP_DIR + XLSX_RAINFALL_INTERVAL_DATA["All"]

    workbook = Workbook(interval_data_file_consolidated)
    worksheet = workbook.add_worksheet()

    start_col = 0
    column_buffer = 6

    header_format = workbook.add_format({
        'bold': True,
        'align': 'center',
        'valign': 'vcenter',
        'bg_color': '#D7E4BC',
        'border': 1
    })

    gap_format = workbook.add_format({'bg_color': 'black'})

    for site in DUMP_FILES_RAINFALL_INTERVAL_DATA:
        csv_file = TEMP_DIR + DUMP_FILES_RAINFALL_INTERVAL_DATA[site]

        print("\tMerging in file %s to xlsx %s" % (csv_file, interval_data_file_consolidated))

        # Open CSV to read header row only (first line)
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)

            cols_per_file = len(headers)  # Number of columns in each CSV

        # Merge header to span all columns in each csv file schema
        worksheet.merge_range(0, start_col, 0, start_col + cols_per_file - 1, f'{site}',
                              header_format)

        # Write header row with formatting
        for c, header in enumerate(headers):
            worksheet.write(1, start_col + c, header, header_format)
            # Set column width based on header length (with minimum)
            col_width = max(len(header)+ column_buffer, 10)
            worksheet.set_column(start_col + c, start_col + c, col_width)

        # Reopen CSV to stream data rows without loading full file
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            for r, row in enumerate(reader, start=2):
                for c, value in enumerate(row):
                    worksheet.write(r, start_col + c, value)

        # Fill the gap column after CSV data columns with black background
        gap_col = start_col + cols_per_file
        # Calculate the number of rows to fill (assuming minimum 2 header rows + data rows)
        num_rows = r if 'r' in locals() else 2  # fallback if no data rows

        for row_num in range(num_rows + 1):  # +1 for 0-based row count including header row 0
            worksheet.write_blank(row_num, gap_col, None, gap_format)

        start_col += cols_per_file + 1  # Move start column for next CSV, with 1 column gap

    workbook.close()
    print("\tCombined XLSX file created: %s" % interval_data_file_consolidated)

def dump_rainfall_interval_timebound(cursor, start_timestamp, end_timestamp):

    # convert datetimes start_timestamp and end_timestamp to filename-friendly formats
    start_ts_str = start_timestamp.strftime(FILE_DT_FMT)
    end_ts_str = end_timestamp.strftime(FILE_DT_FMT)

    interval_csv_files = []

    for site in DUMP_FILES_RAINFALL_INTERVAL_TIMEBOUND_DATA:
        query_template = Template(open("templates/sql/rainfall-interval-data-timebound.sql.template").read())

        cursor.execute("describe %s.%s" % (NSSK_RAINFALL_INTERVAL_DATA_DB, site))

        schema = []
        for row in  cursor.fetchall():
            schema.append(row[0])

        csv_filename = f"{DUMP_FILES_RAINFALL_INTERVAL_TIMEBOUND_DATA[site]}.{site}_{start_ts_str}.to.{end_ts_str}.csv"

        csv_filepath = f"{TEMP_DIR}{csv_filename}"

        print("\tWriting file %s" % csv_filename)

        # dump table contents
        with (open(csv_filepath, 'w') as writer):
            csv_writer = csv.writer(writer, quoting=csv.QUOTE_ALL)

            # write our schema
            csv_writer.writerow(schema)

            query = query_template.substitute(
                DB=NSSK_RAINFALL_INTERVAL_DATA_DB,
                SITE=site,
                START_DATETIME=start_timestamp,
                END_DATETIME=end_timestamp
            )

            # print("Query: %s" % query)
            cursor.execute(query)

            rows = cursor.fetchmany(FETCH_SIZE)

            # write csv file
            while rows is not None and rows:
                csv_writer.writerows(rows)
                rows = cursor.fetchmany(FETCH_SIZE)

        # this interval's csv files for generating its own xlsx files
        interval_csv_files.append( (csv_filename, csv_filepath, site, start_ts_str, end_ts_str) )

        # a collection of all the yearly csv files for many years
        RAINFALL_INTERVAL_TIMEBOUND_GEN_CSV_FILES.append( (csv_filename, csv_filepath, site, start_ts_str, end_ts_str) )
    #########################################
    # all sites processed, create one consolidated xlsx from the csv files generated above
    #
    # each site name should span all columns
    # time series data all side by side. no correlation across sites
    ######### Site 1 ############|######### Site 2 ############|
    # site1-field1,site1-field2,site1-field3,site2-field1,site2-field2,site2-field3
    # ...
    # will not be parsable csv

    print(f"\tCreating combined XLSX file from:")
    for (csv_filename, csv_filepath, site, start_ts_str, end_ts_str)  in interval_csv_files:
        print(f"\t\t{site}: {csv_filename}")

    xlsx_prefix = XLSX_RAINFALL_INTERVAL_TIMEBOUND_DATA["All"]

    interval_data_file_consolidated = f"{xlsx_prefix}.all_{start_ts_str}.to.{end_ts_str}.xlsx"
    interval_data_file_consolidated_path = f"{TEMP_DIR}{interval_data_file_consolidated}"

    workbook = Workbook(interval_data_file_consolidated_path)
    worksheet = workbook.add_worksheet()

    start_col = 0
    column_buffer = 6

    header_format = workbook.add_format({
        'bold': True,
        'align': 'center',
        'valign': 'vcenter',
        'bg_color': '#D7E4BC',
        'border': 1
    })

    gap_format = workbook.add_format({'bg_color': 'black'})

    for (csv_filename, csv_filepath, site, start_ts_str, end_ts_str) in interval_csv_files:

        print("\tMerging in file %s to xlsx %s" % (csv_filepath, interval_data_file_consolidated_path))

        # Open CSV to read header row only (first line)
        with open(csv_filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)

            cols_per_file = len(headers)  # Number of columns in each CSV

        # Merge header to span all columns in each csv file schema
        worksheet.merge_range(0, start_col, 0, start_col + cols_per_file - 1, f'{site}',
                              header_format)

        # Write header row with formatting
        for c, header in enumerate(headers):
            worksheet.write(1, start_col + c, header, header_format)
            # Set column width based on header length (with minimum)
            col_width = max(len(header)+ column_buffer, 10)
            worksheet.set_column(start_col + c, start_col + c, col_width)

        # Reopen CSV to stream data rows without loading full file
        with open(csv_filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            for r, row in enumerate(reader, start=2):
                for c, value in enumerate(row):
                    worksheet.write(r, start_col + c, value)

        # Fill the gap column after CSV data columns with black background
        gap_col = start_col + cols_per_file
        # Calculate the number of rows to fill (assuming minimum 2 header rows + data rows)
        num_rows = r if 'r' in locals() else 2  # fallback if no data rows

        for row_num in range(num_rows + 1):  # +1 for 0-based row count including header row 0
            worksheet.write_blank(row_num, gap_col, None, gap_format)

        start_col += cols_per_file + 1  # Move start column for next CSV, with 1 column gap

    workbook.close()
    print("\tCombined XLSX file created: %s" % interval_data_file_consolidated_path)

    RAINFALL_INTERVAL_TIMEBOUND_GEN_XLSX_FILES.append(
        (interval_data_file_consolidated, interval_data_file_consolidated_path)
    )

def dump_cnv_flowworks(cursor):
    for site in DUMP_FILES_CNV_FLOWWORKS:
        # manually write header
        cursor.execute("describe %s.%s" % (NSSK_CNV_FLOWWORKS_DB, site))
        rows = cursor.fetchall()

        schema = []
        for row in rows:
            schema.append(row[0])

        csv_file = TEMP_DIR + DUMP_FILES_CNV_FLOWWORKS[site]

        print("\tWriting file %s" % csv_file)

        # dump table contents
        with (open(csv_file, 'w') as writer):
            csv_writer = csv.writer(writer, quoting=csv.QUOTE_ALL)
            csv_writer.writerow(schema)

            query = "SELECT * FROM %s.%s ORDER BY %s ASC" % (NSSK_CNV_FLOWWORKS_DB, site, NSSK_CNV_FLOWWORKS_TIMESTAMP_FIELD)

            # print("Query: %s" % query)
            cursor.execute(query)

            rows = cursor.fetchmany(FETCH_SIZE)

            while rows is not None and rows:
                csv_writer.writerows(rows)
                rows = cursor.fetchmany(FETCH_SIZE)

def dump_dnv_flowworks(cursor):
    for site in DUMP_FILES_DNV_FLOWWORKS:
        # manually write header
        cursor.execute("describe %s.%s" % (NSSK_DNV_FLOWWORKS_DB, site))
        rows = cursor.fetchall()

        schema = []
        for row in rows:
            schema.append(row[0])

        csv_file = TEMP_DIR + DUMP_FILES_DNV_FLOWWORKS[site]

        print("\tWriting file %s" % csv_file)

        # dump table contents
        with (open(csv_file, 'w') as writer):
            csv_writer = csv.writer(writer, quoting=csv.QUOTE_ALL)
            csv_writer.writerow(schema)

            query = (
                    "SELECT * FROM %s.%s ORDER BY %s ASC" %
                    (NSSK_DNV_FLOWWORKS_DB, site, NSSK_DNV_FLOWWORKS_TIMESTAMP_FIELD)
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
        cursor.execute("describe %s.%s" % (NSSK_COSMO_DB, site))
        rows = cursor.fetchall()

        schema = []
        for row in rows:
            schema.append(row[0])

        csv_file = TEMP_DIR + DUMP_FILES_COSMO[site]

        print("\tWriting file %s" % csv_file)

        # dump table contents
        with (open(csv_file, 'w') as writer):
            csv_writer = csv.writer(writer, quoting=csv.QUOTE_ALL)
            csv_writer.writerow(schema)

            query = "select * from %s.%s ORDER BY " % (NSSK_COSMO_DB, site)
            query += "CAST(CONCAT_WS(' ', %s.%s.ActivityStartDate, %s.%s.ActivityStartTime) as DATETIME) ASC" % (
                NSSK_COSMO_DB, site, NSSK_COSMO_DB, site
            )

            # print("Query: %s" % query)
            cursor.execute(query)

            rows = cursor.fetchmany(FETCH_SIZE)

            while rows is not None and rows:
                csv_writer.writerows(rows)
                rows = cursor.fetchmany(FETCH_SIZE)

def dump_conductivity_rainfall_correlation(cursor):

    for site in DUMP_FILES_CONDUCTIVITY_RAINFALL_CORRELATION:
        # manually write header
        cursor.execute("describe %s.%s" % (NSSK_CONDUCTIVITY_RAINFALL_CORRELATION_DB, site))
        rows = cursor.fetchall()

        schema = []
        for row in rows:
            schema.append(row[0])

        csv_file = TEMP_DIR + DUMP_FILES_CONDUCTIVITY_RAINFALL_CORRELATION[site]

        print("\tWriting file %s" % csv_file)

        # dump table contents
        with (open(csv_file, 'w') as writer):
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

    csv_file = TEMP_DIR + DUMP_FILES_RAINFALL_EVENTS["CNV"]

    print("\tWriting file %s" % csv_file)

    # dump table contents
    with (open(csv_file, 'w') as writer):
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

        csv_file = TEMP_DIR + DUMP_FILES_RAINFALL_EVENT_DATA[site]

        print("\tWriting file %s" % csv_file)

        # dump table contents
        with (open(csv_file, 'w') as writer):
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

def dump_rainfall_event_data_aggregate(cursor):
    # read query template from file
    query_template = Template(open("templates/sql/rainfall-event-data-aggregate.sql.template").read())

    for site in DUMP_FILES_RAINFALL_EVENT_DATA_AGGREGATE:

        # manually write header

        # the query is hardcoded, so hardcode the schema
        schema = [
            "EventID",
            "StartTimestamp",
            "EndTimestamp",
            "TotalRainfall",
            "TotalFlow"
        ]

        csv_file = TEMP_DIR + DUMP_FILES_RAINFALL_EVENT_DATA_AGGREGATE[site]

        print("\tWriting file %s" % csv_file)

        # dump table contents
        with (open(csv_file, 'w') as writer):
            csv_writer = csv.writer(writer, quoting=csv.QUOTE_ALL)
            csv_writer.writerow(schema)

            query = query_template.substitute(
                DB=NSSK_RAINFALL_EVENTS_DB,
                RAINFALL_EVENTS_TABLE="RAINFALL_EVENTS",
                SITE=site
            )

            # print("Query: %s" % query)
            cursor.execute(query)

            rows = cursor.fetchmany(FETCH_SIZE)

            while rows is not None and rows:
                csv_writer.writerows(rows)
                rows = cursor.fetchmany(FETCH_SIZE)

def dump_waterrangers(cursor):
    for site in DUMP_FILES_WATERRANGERS:
        # manually write header
        cursor.execute("describe %s.%s" % (NSSK_WATERRANGERS_DB, site))
        rows = cursor.fetchall()

        schema = []
        for row in rows:
            schema.append(row[0])

        csv_file = TEMP_DIR + DUMP_FILES_WATERRANGERS[site]

        print("\tWriting file %s" % csv_file)

        # dump table contents
        with (open(csv_file, 'w') as writer):
            csv_writer = csv.writer(writer, quoting=csv.QUOTE_ALL)
            csv_writer.writerow(schema)

            query = "select * from %s.%s ORDER BY %s ASC" % (
                NSSK_WATERRANGERS_DB,
                site,
                NSSK_WATERRANGERS_TIMESTAMP_FIELD
            )

            # print("Query: %s" % query)
            cursor.execute(query)

            rows = cursor.fetchmany(FETCH_SIZE)

            while rows is not None and rows:
                csv_writer.writerows(rows)
                rows = cursor.fetchmany(FETCH_SIZE)

def dump_cnv_hydrometric_data(cursor):
    # run a query rather than dump a table

    for site in DUMP_FILES_CNV_HYDROMETRIC:
        # manually write header
        cursor.execute("describe %s.%s" % (NSSK_CNV_HYDROMETRIC_DB, site))
        rows = cursor.fetchall()

        schema = []
        for row in rows:
            schema.append(row[0])

        csv_file = TEMP_DIR + DUMP_FILES_CNV_HYDROMETRIC[site]

        print("\tWriting file %s" % csv_file)

        # dump table contents
        with (open(csv_file, 'w') as writer):
            csv_writer = csv.writer(writer, quoting=csv.QUOTE_ALL)
            csv_writer.writerow(schema)

            query = "select * from %s.%s ORDER BY " % (NSSK_CNV_HYDROMETRIC_DB, site)
            query += "%s.%s.MeasurementTimestamp ASC" % (NSSK_CNV_HYDROMETRIC_DB, site)

            # print("Query: %s" % query)
            cursor.execute(query)

            rows = cursor.fetchmany(FETCH_SIZE)

            while rows is not None and rows:
                csv_writer.writerows(rows)
                rows = cursor.fetchmany(FETCH_SIZE)

def dump_chloride_acuity(cursor):
    # read query template from file
    query_template = Template(open("templates/sql/chloride-acuity.sql.template").read())

    for site in DUMP_FILES_CHLORIDE_ACUITY:

        # manually write header

        # the query is hardcoded, so hardcode the schema
        schema = ["CosmoTimestamp","Specific_Conductance","Temperature_Water","Water_Level"]

        csv_file = TEMP_DIR + DUMP_FILES_CHLORIDE_ACUITY[site]

        print("\tWriting file %s" % csv_file)

        # dump table contents
        with (open(csv_file, 'w') as writer):
            csv_writer = csv.writer(writer, quoting=csv.QUOTE_ALL)
            csv_writer.writerow(schema)

            query = query_template.substitute(
                DB=NSSK_COSMO_DB,
                SITE=site,
                SPEC_CONDUCTANCE_THRESHOLD=SPECIFIC_CONDUCTANCE_THRESHOLD
            )

            # print("Query: %s" % query)
            cursor.execute(query)

            rows = cursor.fetchmany(FETCH_SIZE)

            while rows is not None and rows:
                csv_writer.writerows(rows)
                rows = cursor.fetchmany(FETCH_SIZE)

def zip_dump_files():
    # zip expected csv files in tmp/

    for site in DUMP_FILES_CNV_FLOWWORKS:
        zip_dump_file(DUMP_FILES_CNV_FLOWWORKS[site])

    for site in DUMP_FILES_DNV_FLOWWORKS:
        zip_dump_file(DUMP_FILES_DNV_FLOWWORKS[site])

    for site in DUMP_FILES_COSMO:
        zip_dump_file(DUMP_FILES_COSMO[site])

    for site in DUMP_FILES_CONDUCTIVITY_RAINFALL_CORRELATION:
        zip_dump_file(DUMP_FILES_CONDUCTIVITY_RAINFALL_CORRELATION[site])

    for site in DUMP_FILES_RAINFALL_EVENTS:
        zip_dump_file(DUMP_FILES_RAINFALL_EVENTS[site])

    for site in DUMP_FILES_RAINFALL_EVENT_DATA:
        zip_dump_file(DUMP_FILES_RAINFALL_EVENT_DATA[site])

    for site in DUMP_FILES_RAINFALL_EVENT_DATA_AGGREGATE:
        zip_dump_file(DUMP_FILES_RAINFALL_EVENT_DATA_AGGREGATE[site])

    for site in DUMP_FILES_RAINFALL_INTERVAL_DATA:
        zip_dump_file(DUMP_FILES_RAINFALL_INTERVAL_DATA[site])

    # TODO: rainfall interval data consolidated

    for site in DUMP_FILES_CNV_HYDROMETRIC:
        zip_dump_file(DUMP_FILES_CNV_HYDROMETRIC[site])

    for site in DUMP_FILES_CHLORIDE_ACUITY:
        zip_dump_file(DUMP_FILES_CHLORIDE_ACUITY[site])

    for site in DUMP_FILES_WATERRANGERS:
        zip_dump_file(DUMP_FILES_WATERRANGERS[site])

# zip a dump file in its own archive at archive root level
def zip_dump_file(dump_file):
    file = "%s%s" % (TEMP_DIR, dump_file)

    # check file exists
    if exists(file):
        print("Compressing dump file: %s" % dump_file)
        with ZipFile("%s.zip" % file, 'w', zipfile.ZIP_DEFLATED) as zip_h:
            zip_h.write(file, arcname="./%s" % dump_file)

        # remove source csv file
        # actually keep this to offer both zipped and raw files
        #os.remove(file)
    else:
        print("ERROR: encountered missing csv dump file when attempting compression: %s" % file)

def write_html_file():
    html_template = Template(open("templates/index.html.template").read())

    csv_entry_template = Template(open("templates/csv-entry.template.html").read())
    xlsx_entry_template = Template(open("templates/xlsx-entry.template.html").read())
    section_block_template = Template(open("templates/section-block.html.template").read())

    ##########
    # cnv

    cnv_flowworks_section_body = ""
    for name in DUMP_FILES_CNV_FLOWWORKS:
        csv_file = DUMP_FILES_CNV_FLOWWORKS[name]

        zip_file = "%s.zip" % DUMP_FILES_CNV_FLOWWORKS[name]

        # http links to files deployed on webserver (./file.csv, ./file.csv.zip)
        file_in_dist = "%s/%s" % (TEMP_DIR, csv_file)

        # http link to file deployed on webserver (./file.csv)
        zip_file_link = "./%s" % zip_file
        csv_file_link = "./%s" % csv_file

        cnv_flowworks_section_body += csv_entry_template.substitute(
            FILE=csv_file,
            RESOURCE_LINK=csv_file_link,
            ZIP_LINK=zip_file_link,
            NAME=DUMP_FILES_CNV_FLOWWORKS[name],
            DESC="CNV Flowworks data",
            SIZE="%.3f MB" % (os.path.getsize(file_in_dist) / BYTES_IN_MB),
            CREATION_DATE=DIST_TIMESTAMP.strftime(MYSQL_DT_FMT)
        )

    cnv_flowworks_section_block = section_block_template.substitute(
        SECTION_BODY=cnv_flowworks_section_body
    )

    ##########
    # dnv flowworks

    dnv_flowworks_section_body = ""
    for name in DUMP_FILES_DNV_FLOWWORKS:
        csv_file = DUMP_FILES_DNV_FLOWWORKS[name]

        zip_file = "%s.zip" % DUMP_FILES_DNV_FLOWWORKS[name]

        # file in the work dir (./tmp/file.csv)
        file_in_dist = "%s/%s" % (TEMP_DIR, csv_file)

        # http links to files deployed on webserver (./file.csv, ./file.csv.zip)
        zip_file_link = "./%s" % zip_file
        csv_file_link = "./%s" % csv_file

        dnv_flowworks_section_body += csv_entry_template.substitute(
            FILE=csv_file,
            RESOURCE_LINK=csv_file_link,
            ZIP_LINK=zip_file_link,
            NAME=DUMP_FILES_DNV_FLOWWORKS[name],
            DESC="DNV Flowworks data",
            SIZE="%.3f MB" % (os.path.getsize(file_in_dist) / BYTES_IN_MB),
            CREATION_DATE=DIST_TIMESTAMP.strftime(MYSQL_DT_FMT)
        )

    dnv_flowworks_section_block = section_block_template.substitute(
        SECTION_BODY=dnv_flowworks_section_body
    )

    ##########
    # CoSMo
    cosmo_section_body = ""
    for name in DUMP_FILES_COSMO:
        csv_file = DUMP_FILES_COSMO[name]

        zip_file = "%s.zip" % DUMP_FILES_COSMO[name]

        # file in the work dir (./tmp/file.csv)
        file_in_dist = "%s/%s" % (TEMP_DIR, csv_file)

        # http links to files deployed on webserver (./file.csv, ./file.csv.zip)
        zip_file_link = "./%s" % zip_file
        csv_file_link = "./%s" % csv_file

        cosmo_section_body += csv_entry_template.substitute(
            FILE=csv_file,
            RESOURCE_LINK=csv_file_link,
            ZIP_LINK=zip_file_link,
            NAME=DUMP_FILES_COSMO[name],
            DESC="CoSMo DFO data for site %s" % name,
            SIZE="%.3f MB" % (os.path.getsize(file_in_dist) / BYTES_IN_MB),
            CREATION_DATE=DIST_TIMESTAMP.strftime(MYSQL_DT_FMT)
        )

    cosmo_section_block = section_block_template.substitute(
        SECTION_BODY=cosmo_section_body
    )

    ##########
    # Conductivity-rainfall correlation

    conductivity_rainfall_correlation_section_body = ""
    for name in DUMP_FILES_CONDUCTIVITY_RAINFALL_CORRELATION:
        csv_file = DUMP_FILES_CONDUCTIVITY_RAINFALL_CORRELATION[name]
        zip_file = "%s.zip" % DUMP_FILES_CONDUCTIVITY_RAINFALL_CORRELATION[name]

        # file in the work dir (./tmp/file.csv)
        file_in_dist = "%s/%s" % (TEMP_DIR, csv_file)

        # http links to files deployed on webserver (./file.csv, ./file.csv.zip)
        zip_file_link = "./%s" % zip_file
        csv_file_link = "./%s" % csv_file

        conductivity_rainfall_correlation_section_body += csv_entry_template.substitute(
            FILE=csv_file,
            RESOURCE_LINK=csv_file_link,
            ZIP_LINK=zip_file_link,
            NAME=DUMP_FILES_CONDUCTIVITY_RAINFALL_CORRELATION[name],
            DESC="C/R for CoSMo Site %s" % name,
            SIZE="%.3f MB" % (os.path.getsize(file_in_dist) / BYTES_IN_MB),
            CREATION_DATE=DIST_TIMESTAMP.strftime(MYSQL_DT_FMT)
        )

    conductivity_rainfall_correlation_section_block = section_block_template.substitute(
        SECTION_BODY=conductivity_rainfall_correlation_section_body
    )

    ##########
    # Rainfall Events

    rainfall_events_section_body = ""
    for name in DUMP_FILES_RAINFALL_EVENTS:
        csv_file = DUMP_FILES_RAINFALL_EVENTS[name]
        zip_file = "%s.zip" % DUMP_FILES_RAINFALL_EVENTS[name]

        # file in the work dir (./tmp/file.csv)
        file_in_dist = "%s/%s" % (TEMP_DIR, csv_file)

        # http links to files deployed on webserver (./file.csv, ./file.csv.zip)
        zip_file_link = "./%s" % zip_file
        csv_file_link = "./%s" % csv_file

        rainfall_events_section_body += csv_entry_template.substitute(
            FILE=csv_file,
            RESOURCE_LINK=csv_file_link,
            ZIP_LINK=zip_file_link,
            NAME=DUMP_FILES_RAINFALL_EVENTS[name],
            DESC="Rainfall Events for %s" % name,
            SIZE="%.3f MB" % (os.path.getsize(file_in_dist) / BYTES_IN_MB),
            CREATION_DATE=DIST_TIMESTAMP.strftime(MYSQL_DT_FMT)
        )

    rainfall_events_section_block = section_block_template.substitute(
        SECTION_BODY=rainfall_events_section_body
    )
    ##########
    # Rainfall Event Data

    rainfall_event_data_section_body = ""
    for name in DUMP_FILES_RAINFALL_EVENT_DATA:
        csv_file = DUMP_FILES_RAINFALL_EVENT_DATA[name]
        zip_file = "%s.zip" % DUMP_FILES_RAINFALL_EVENT_DATA[name]

        # file in the work dir (./tmp/file.csv)
        file_in_dist = "%s/%s" % (TEMP_DIR, csv_file)

        # http links to files deployed on webserver (./file.csv, ./file.csv.zip)
        zip_file_link = "./%s" % zip_file
        csv_file_link = "./%s" % csv_file

        rainfall_event_data_section_body += csv_entry_template.substitute(
            FILE=csv_file,
            RESOURCE_LINK=csv_file_link,
            ZIP_LINK=zip_file_link,
            NAME=DUMP_FILES_RAINFALL_EVENT_DATA[name],
            DESC="Rainfall Event Data for site %s" % name,
            SIZE="%.3f MB" % (os.path.getsize(file_in_dist) / BYTES_IN_MB),
            CREATION_DATE=DIST_TIMESTAMP.strftime(MYSQL_DT_FMT)
        )

    rainfall_event_data_section_block = section_block_template.substitute(
        SECTION_BODY=rainfall_event_data_section_body
    )

    ##########
    # Rainfall Event Data Aggregate

    rainfall_event_data_aggregate_section_body = ""
    for name in DUMP_FILES_RAINFALL_EVENT_DATA_AGGREGATE:
        csv_file = DUMP_FILES_RAINFALL_EVENT_DATA_AGGREGATE[name]
        zip_file = "%s.zip" % DUMP_FILES_RAINFALL_EVENT_DATA_AGGREGATE[name]

        # file in the work dir (./tmp/file.csv)
        file_in_dist = "%s/%s" % (TEMP_DIR, csv_file)

        # http links to files deployed on webserver (./file.csv, ./file.csv.zip)
        zip_file_link = "./%s" % zip_file
        csv_file_link = "./%s" % csv_file

        rainfall_event_data_aggregate_section_body += csv_entry_template.substitute(
            FILE=csv_file,
            RESOURCE_LINK=csv_file_link,
            ZIP_LINK=zip_file_link,
            NAME=DUMP_FILES_RAINFALL_EVENT_DATA_AGGREGATE[name],
            DESC="Rainfall Event Aggregate values for site %s" % name,
            SIZE="%.3f MB" % (os.path.getsize(file_in_dist) / BYTES_IN_MB),
            CREATION_DATE=DIST_TIMESTAMP.strftime(MYSQL_DT_FMT)
        )

    rainfall_event_data_aggregate_section_block = section_block_template.substitute(
        SECTION_BODY=rainfall_event_data_aggregate_section_body
    )

    ##########
    # Rainfall Interval Data
    rainfall_interval_data_section_body = ""

    # dump files
    for name in DUMP_FILES_RAINFALL_INTERVAL_DATA:
        csv_file = DUMP_FILES_RAINFALL_INTERVAL_DATA[name]
        zip_file = "%s.zip" % DUMP_FILES_RAINFALL_INTERVAL_DATA[name]

        # file in the work dir (./tmp/file.csv)
        file_in_dist = "%s/%s" % (TEMP_DIR, csv_file)

        # http links to files deployed on webserver (./file.csv, ./file.csv.zip)
        zip_file_link = "./%s" % zip_file
        csv_file_link = "./%s" % csv_file

        rainfall_interval_data_section_body += csv_entry_template.substitute(
            FILE=csv_file,
            RESOURCE_LINK=csv_file_link,
            ZIP_LINK=zip_file_link,
            NAME=DUMP_FILES_RAINFALL_INTERVAL_DATA[name],
            DESC="Rainfall 10-minute Interval Data for site %s" % name,
            SIZE="%.3f MB" % (os.path.getsize(file_in_dist) / BYTES_IN_MB),
            CREATION_DATE=DIST_TIMESTAMP.strftime(MYSQL_DT_FMT)
        )

    # xlsx files
    for name in XLSX_RAINFALL_INTERVAL_DATA:
        xlsx_file = XLSX_RAINFALL_INTERVAL_DATA[name]

        # file in the work dir (./tmp/file.csv)
        file_in_dist = "%s/%s" % (TEMP_DIR, xlsx_file)

        # http links to files deployed on webserver (./file.csv, ./file.csv.zip)
        xlsx_file_link = "./%s" % xlsx_file

        rainfall_interval_data_section_body += xlsx_entry_template.substitute(
            FILE=xlsx_file,
            RESOURCE_LINK=xlsx_file_link,
            NAME=XLSX_RAINFALL_INTERVAL_DATA[name],
            DESC="Rainfall 10-minute Interval Data for all sites",
            SIZE="%.3f MB" % (os.path.getsize(file_in_dist) / BYTES_IN_MB),
            CREATION_DATE=DIST_TIMESTAMP.strftime(MYSQL_DT_FMT)
    )

    rainfall_interval_data_section_block = section_block_template.substitute(
        SECTION_BODY=rainfall_interval_data_section_body
    )

    ##########
    # Rainfall Interval Data - yearly

    rainfall_interval_data_yearly_section_body = ""

    # dump files
    for (csv_filename, csv_filepath, site, start_ts_str, end_ts_str) in RAINFALL_INTERVAL_TIMEBOUND_GEN_CSV_FILES:
        zip_file = f"{csv_filename}.zip"

        # file in the work dir (./tmp/file.csv)
        file_in_dist = csv_filepath

        # http links to files deployed on webserver (./file.csv, ./file.csv.zip)
        zip_file_link = f"./{zip_file}"
        csv_file_link = f"./{csv_filename}"

        rainfall_interval_data_yearly_section_body += csv_entry_template.substitute(
            RESOURCE_LINK=csv_file_link,
            ZIP_LINK=zip_file_link,
            NAME=csv_filename,
            DESC=f"Rainfall 10-minute Interval Data - Yearly for site {site}",
            SIZE="%.3f MB" % (os.path.getsize(file_in_dist) / BYTES_IN_MB),
            CREATION_DATE=DIST_TIMESTAMP.strftime(MYSQL_DT_FMT)
        )

    # xlsx files

    # hardcode for now
    name = "all"
    for (xlsx_file,xlsx_path) in RAINFALL_INTERVAL_TIMEBOUND_GEN_XLSX_FILES:

        # file in the work dir (./tmp/file.csv)
        file_in_dist = xlsx_path

        # http links to files deployed on webserver (./file.csv, ./file.csv.zip)
        xlsx_file_link = xlsx_file

        rainfall_interval_data_yearly_section_body += xlsx_entry_template.substitute(
            FILE=xlsx_file,
            RESOURCE_LINK=xlsx_file_link,
            NAME=xlsx_file,
            DESC=f"Rainfall 10-minute Interval Data - Yearly for all sites",
            SIZE="%.3f MB" % (os.path.getsize(file_in_dist) / BYTES_IN_MB),
            CREATION_DATE=DIST_TIMESTAMP.strftime(MYSQL_DT_FMT)
    )

    rainfall_interval_data_yearly_section_block = section_block_template.substitute(
        SECTION_BODY=rainfall_interval_data_yearly_section_body
    )

    ##########
    # CNV Hydrometric

    cnv_hydrometric_section_body = ""
    for name in DUMP_FILES_CNV_HYDROMETRIC:
        csv_file = DUMP_FILES_CNV_HYDROMETRIC[name]
        zip_file = "%s.zip" % DUMP_FILES_CNV_HYDROMETRIC[name]

        # file in the work dir (./tmp/file.csv)
        file_in_dist = "%s/%s" % (TEMP_DIR, csv_file)

        # http links to files deployed on webserver (./file.csv, ./file.csv.zip)
        zip_file_link = "./%s" % zip_file
        csv_file_link = "./%s" % csv_file

        cnv_hydrometric_section_body += csv_entry_template.substitute(
            FILE=csv_file,
            RESOURCE_LINK=csv_file_link,
            ZIP_LINK=zip_file_link,
            NAME=DUMP_FILES_CNV_HYDROMETRIC[name],
            DESC="CNV Hydrometric Data for site %s" % name,
            SIZE="%.3f MB" % (os.path.getsize(file_in_dist) / BYTES_IN_MB),
            CREATION_DATE=DIST_TIMESTAMP.strftime(MYSQL_DT_FMT)
        )

    cnv_hydrometric_section_block = section_block_template.substitute(
        SECTION_BODY=cnv_hydrometric_section_body
    )

    ##########
    # chloride acuity
    chloride_acuity_section_body = ""
    for name in DUMP_FILES_CHLORIDE_ACUITY:
        csv_file = DUMP_FILES_CHLORIDE_ACUITY[name]
        zip_file = "%s.zip" % DUMP_FILES_CHLORIDE_ACUITY[name]

        # file in the work dir (./tmp/file.csv)
        file_in_dist = "%s/%s" % (TEMP_DIR, csv_file)

        # http links to files deployed on webserver (./file.csv, ./file.csv.zip)
        zip_file_link = "./%s" % zip_file
        csv_file_link = "./%s" % csv_file

        chloride_acuity_section_body += csv_entry_template.substitute(
            FILE=csv_file,
            RESOURCE_LINK=csv_file_link,
            ZIP_LINK=zip_file_link,
            NAME=DUMP_FILES_CHLORIDE_ACUITY[name],
            DESC="Chloride Acuity data for site %s" % name,
            SIZE="%.3f MB" % (os.path.getsize(file_in_dist) / BYTES_IN_MB),
            CREATION_DATE=DIST_TIMESTAMP.strftime(MYSQL_DT_FMT)
        )

    chloride_acuity_section_body = section_block_template.substitute(
        SECTION_BODY=chloride_acuity_section_body
    )

    ##########
    # Waterrangers
    waterrangers_section_body = ""
    for name in DUMP_FILES_WATERRANGERS:
        csv_file = DUMP_FILES_WATERRANGERS[name]
        zip_file = "%s.zip" % DUMP_FILES_WATERRANGERS[name]

        # file in the work dir (./tmp/file.csv)
        file_in_dist = "%s/%s" % (TEMP_DIR, csv_file)

        # http links to files deployed on webserver (./file.csv, ./file.csv.zip)
        zip_file_link = "./%s" % zip_file
        csv_file_link = "./%s" % csv_file

        waterrangers_section_body += csv_entry_template.substitute(
            FILE=csv_file,
            RESOURCE_LINK=csv_file_link,
            ZIP_LINK=zip_file_link,
            NAME=DUMP_FILES_WATERRANGERS[name],
            DESC="Waterrangers data for site %s" % name,
            SIZE="%.3f MB" % (os.path.getsize(file_in_dist) / BYTES_IN_MB),
            CREATION_DATE=DIST_TIMESTAMP.strftime(MYSQL_DT_FMT)
        )

    waterrangers_section_block = section_block_template.substitute(
        SECTION_BODY=waterrangers_section_body
    )

    ##############################
    dist_html_page = html_template.substitute(
        CNV_FLOWWORKS_BLOCK=cnv_flowworks_section_block,
        DNV_FLOWWORKS_BLOCK=dnv_flowworks_section_block,
        COSMO_BLOCK=cosmo_section_block,
        CONDUCTIVITY_RAINFALL_CORRELATION_BLOCK=conductivity_rainfall_correlation_section_block,
        RAINFALL_EVENTS_BLOCK=rainfall_events_section_block,
        RAINFALL_EVENT_DATA_BLOCK=rainfall_event_data_section_block,
        RAINFALL_EVENT_DATA_AGGREGATE_BLOCK=rainfall_event_data_aggregate_section_block,
        RAINFALL_INTERVAL_DATA_BLOCK=rainfall_interval_data_section_block,
        RAINFALL_INTERVAL_DATA_YEARLY_BLOCK=rainfall_interval_data_yearly_section_block,
        CNV_HYDROMETRIC_BLOCK=cnv_hydrometric_section_block,
        CHLORIDE_ACUITY_BLOCK=chloride_acuity_section_body,
        WATERRANGERS_BLOCK=waterrangers_section_block
    )

    # write everything to file
    with open("%s%s" % (TEMP_DIR, DUMP_HTML_FILE), 'w') as writer:
        writer.write(dist_html_page)

# create the dist from intermediate resources. this file gets deployed on some service medium
def create_dist():
    print("Building distribution from zipped dump files and resources")

    # create a zip file at "./" from:
    # zip files in tmp
    # csv files in tmp
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

    # assemble zip file from tmp dir
    with ZipFile("./%s" % DIST_FILE, 'w', zipfile.ZIP_DEFLATED) as zip_h:
        for root, dirs, files in os.walk(TEMP_DIR):
            dirs.sort()
            # Sort files for cleaner output and debugging
            files.sort()
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

    print(f"Creating distribution with timestamp {DIST_TIMESTAMP.strftime(MYSQL_DT_FMT)}...")

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
