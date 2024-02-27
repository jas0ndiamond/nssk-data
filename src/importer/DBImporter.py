from mysql.connector import connect, Error, IntegrityError
from datetime import datetime

import json
import logging

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

        # store the file name, read it when we're ready for the db inserts
        self.db_config_file = db_config_file

        self.importer_name = "DEFAULT"

        self.schema = None
        self.schema_mapping = None

        self.inserts = []

    def set_importer_name(self, new_name):
        self.importer_name = new_name

    # set the schema to expect for the data, and to inform construction of inserts.
    # schema should be an ordered array of fields.
    # must be defined before invocations of add
    def set_schema(self, schema):
        self.schema = schema

    def set_schema_mapping(self, schema_mapping):
        self.schema_mapping = schema_mapping

    # entry is a DataEntry object
    def add(self, entry):

        # TODO: constraints around entry object
        # is entry a subclass of DataEntry?
        # is the entry object value collection the same size as the schema?

        # check if a schema is defined
        if self.schema is None or len(self.schema) <= 0:
            raise "Database schema must be defined"

        statement = "INSERT INTO"

        # build fields segment based on db schema

        # INSERT INTO table_name (column1, column2, column3, ...)
        # VALUES (value1, value2, value3, ...);

        # INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country)
        # VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');

        fields_segment = "("
        values_segment = "VALUES ("

        # build values
        for field in self.schema:

            # remap field if we have a defined mapping
            if self.schema_mapping is not None:
                mapped_field = self.schema_mapping[field]
                if mapped_field is not None:
                    self.logger.debug("Remapping dump field '%s' to db field '%s'" % (field, mapped_field))
                    fields_segment += "%s," % mapped_field
                else:
                    msg = "Could not resolve field mapping for %s: " % field
                    self.logger.error(msg)
                    raise Exception(msg)
            else:
                # no mapping
                fields_segment += "%s," % field

            # convert py None values to mysql NULL values
            if entry.is_defined(field):
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

        # get the table to store the entry
        table = entry.get_db_destination()

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
            dupe_file = "./duplicates_%s_%s.sql" % (self.importer_name, date_time.strftime("%Y%m%d-%H%M%S"))
            print("\n\nEncountered duplicate entries. Dumping failed inserts to file %s" % dupe_file)
            self.logger.warning("Encountered duplicate entries. Dumping failed inserts to file %s" % dupe_file)

            with open(dupe_file, 'w', encoding='utf-8') as filehandle:
                for insert in duplicates:
                    filehandle.write("%s\n" % insert)

        # report outcome
        self.logger.info("Completed %d inserts. Encountered %d duplicates" % (insert_count, duplicate_count))
        print("\nCompleted %d inserts. Encountered %d duplicates" % (insert_count, duplicate_count))
