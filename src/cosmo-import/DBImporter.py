from mysql.connector import connect, Error, IntegrityError
from datetime import datetime

import json
import logging

# DatasetName
# MonitoringLocationID
# MonitoringLocationName
# MonitoringLocationLatitude
# MonitoringLocationLongitude
# MonitoringLocationHorizontalCoordinateReferenceSystem
# MonitoringLocationHorizontalAccuracyMeasure
# MonitoringLocationHorizontalAccuracyUnit
# MonitoringLocationVerticalMeasure
# MonitoringLocationVerticalUnit
# MonitoringLocationType
# ActivityType
# ActivityMediaName
# ActivityStartDate
# ActivityStartTime
# ActivityEndDate
# ActivityEndTime
# ActivityDepthHeightMeasure
# ActivityDepthHeightUnit
# SampleCollectionEquipmentName
# CharacteristicName
# MethodSpeciation
# ResultSampleFraction
# ResultValue
# ResultUnit
# ResultValueType
# ResultDetectionCondition
# ResultDetectionQuantitationLimitMeasure
# ResultDetectionQuantitationLimitUnit
# ResultDetectionQuantitationLimitType
# ResultStatusID
# ResultComment
# ResultAnalyticalMethodID
# ResultAnalyticalMethodContext
# ResultAnalyticalMethodName
# AnalysisStartDate
# AnalysisStartTime
# AnalysisStartTimeZone
# LaboratoryName
# LaboratorySampleID

# Order of fields matters
cosmo_schema = [
    "DatasetName",
    "MonitoringLocationID",
    "MonitoringLocationName",
    "MonitoringLocationLatitude",
    "MonitoringLocationLongitude",
    "MonitoringLocationHorizontalCoordinateReferenceSystem",
    "MonitoringLocationHorizontalAccuracyMeasure",
    "MonitoringLocationHorizontalAccuracyUnit",
    "MonitoringLocationVerticalMeasure",
    "MonitoringLocationVerticalUnit",
    "MonitoringLocationType",
    "ActivityType",
    "ActivityMediaName",
    "ActivityStartDate",
    "ActivityStartTime",
    "ActivityEndDate",
    "ActivityEndTime",
    "ActivityDepthHeightMeasure",
    "ActivityDepthHeightUnit",
    "SampleCollectionEquipmentName",
    "CharacteristicName",
    "MethodSpeciation",
    "ResultSampleFraction",
    "ResultValue",
    "ResultUnit",
    "ResultValueType",
    "ResultDetectionCondition",
    "ResultDetectionQuantitationLimitMeasure",
    "ResultDetectionQuantitationLimitUnit",
    "ResultDetectionQuantitationLimitType",
    "ResultStatusID",
    "ResultComment",
    "ResultAnalyticalMethodID",
    "ResultAnalyticalMethodContext",
    "ResultAnalyticalMethodName",
    "AnalysisStartDate",
    "AnalysisStartTime",
    "AnalysisStartTimeZone",
    "LaboratoryName",
    "LaboratorySampleID",
]

COMMIT_SIZE = 20

CONFIG_HOST = "host"
CONFIG_PORT = "port"
CONFIG_USER = "user"
CONFIG_PASS = "pass"
CONFIG_DBASE = "dbname"


class DBImporter:

    def __init__(self, db_config_file):

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        self.db_config_file = db_config_file

        self.inserts = []

    def add(self, entry):

        statement = "INSERT INTO"

        # build fields segment based on db schema

        # INSERT INTO table_name (column1, column2, column3, ...)
        # VALUES (value1, value2, value3, ...);

        # INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country)
        # VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');

        fields_segment = "("
        values_segment = "VALUES ("

        # build values
        for field in cosmo_schema:
            fields_segment += "%s," % field

            # convert py None values to mysql NULL values
            if entry.get(field) is not None:
                values_segment += "'%s'," % entry.get(field)
            else:
                values_segment += "NULL,"

        # chop trailing comma if there is one at the end
        if fields_segment[-1] == ",":
            fields_segment = fields_segment.rstrip(", ")

        # chop trailing comma if there is one at the end
        if values_segment[-1] == ",":
            values_segment = values_segment.rstrip(", ")

        # close segments
        fields_segment += ") "
        values_segment += ");"

        # entry is a DataEntry
        # entry fields

        # if we want to have a different table for each monitoring id
        table = entry.get_monitoring_location_id()

        statement += (" " + table + " " + fields_segment + values_segment)

        self.inserts.append(statement)

    # dump our inserts. for debugging
    def dump(self):

        for insert in self.inserts:
            print("%s" % insert)

    # execute insert statements in bulk
    def execute(self):

        total_inserts = len(self.inserts)

        if total_inserts <= 0:
            raise "no inserts to make"

        # read db info from config file
        # host
        # port
        # user
        # pass
        # database

        json_data = open(self.db_config_file).read()
        config = json.loads(json_data)
        json_data = None

        if config[CONFIG_HOST] is None:
            raise "DB Config missing host"

        if config[CONFIG_PORT] is None:
            raise "DB Config missing port"

        if config[CONFIG_USER] is None:
            raise "DB Config missing user"

        if config[CONFIG_PASS] is None:
            raise "DB Config missing pass"

        if config[CONFIG_DBASE] is None:
            raise "DB Config missing dbase"

        print("Starting import...")

        insert_count = 0
        duplicate_count = 0

        duplicates = []

        # build database connection
        try:
            with connect(
                    host=config[CONFIG_HOST],
                    port=int(config[CONFIG_PORT]),
                    user=config[CONFIG_USER],
                    password=config[CONFIG_PASS],
                    database=config[CONFIG_DBASE],
            ) as connection:

                config[CONFIG_PASS] = None
                config = None

                try:
                    with connection.cursor() as cursor:

                        insert_count = 0

                        # print out an initial count of 0 otherwise it appears to hang
                        print("\r\t%d / %d" % (insert_count, total_inserts), end='', flush=True)

                        # run inserts
                        for insert in self.inserts:

                            try:
                                cursor.execute(insert)

                                # commit every COMMIT_SIZE inserts
                                if insert_count % COMMIT_SIZE == 0:
                                    connection.commit()

                                insert_count += 1

                            except IntegrityError as e:

                                # print("error: %s " % e.args[1])

                                # Arguments: (IntegrityError(1062, "1062 (23000): Duplicate entry '2019-06-12-10:00:00-Temperature, water' for key 'WAGG01.PRIMARY'", '23000'),)
                                if " Duplicate entry " in e.args[1] and " for key " in e.args[1]:

                                    self.logger.warning(
                                        "Attempted to insert duplicate row:\n%s\nContinuing..." % insert)
                                    duplicate_count += 1

                                    duplicates.append(insert)
                                else:
                                    # problem but not a duplicate row
                                    raise e
                            except Error as e:
                                self.logger.warning("Error running an insert:\n%s\nContinuing..." % insert, e)

                            print("\r\t%d / %d (%d duplicates)" % (insert_count, total_inserts, duplicate_count),
                                  end='', flush=True)

                        # commit remaining inserts
                        connection.commit()

                except Error as e:
                    self.logger.error("Error running inserts", e)

        except Error as e:
            self.logger.error("Error connecting to database", e)

        # duplicates report written to file
        if duplicate_count > 0:
            date_time = datetime.now()
            dupe_file = "./duplicates_%s.sql" % date_time.strftime("%Y%m%d-%H%M%S")
            print("\n\nEncountered duplicate entries. Dumping failed inserts to file %s" % dupe_file)
            self.logger.warning("Encountered duplicate entries. Dumping failed inserts to file %s" % dupe_file)

            with open(dupe_file, 'w', encoding='utf-8') as filehandle:
                for insert in duplicates:
                    filehandle.write("%s\n" % insert)

        # report outcome
        self.logger.info("Completed %d inserts. Encountered %d duplicates" % (insert_count, duplicate_count))
        print("\nCompleted %d inserts. Encountered %d duplicates" % (insert_count, duplicate_count))
