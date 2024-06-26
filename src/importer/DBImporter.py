from mysql.connector import connect, Error, IntegrityError
from datetime import datetime

import logging

from src.importer.DBConfig import DBConfig
from src.importer.DBConfigFactory import DBConfigFactory

DEFAULT_COMMIT_SIZE = 100
COMMIT_SIZE_MIN = 10
COMMIT_SIZE_MAX = 50000


class DBImporter:

    def __init__(self, db_config_file):

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        self.logger.info("Building DBImporter with config file %s" % db_config_file)

        # store the file name, read it when we're ready for the db inserts
        self.db_config_file = db_config_file

        self.importer_name = "DEFAULT"

        self.commit_size = DEFAULT_COMMIT_SIZE

        self.schema = None
        self.schema_mapping = None

        self.inserts = []

    def set_importer_name(self, new_name):
        self.importer_name = new_name

    def set_commit_size(self, size):
        if COMMIT_SIZE_MIN <= size <= COMMIT_SIZE_MAX:
            self.commit_size = size
        else:
            self.logger.warning("Rejecting invalid commit size")

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
                value = entry.get(field)
                if value is None:
                    values_segment += "NULL,"
                else:
                    values_segment += "'%s'," % value
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

        #TODO call execute if inserts grows to large

    # dump our inserts. for debugging
    def dump(self):

        for insert in self.inserts:
            print("%s" % insert)

    # execute insert statements in bulk
    def execute(self):

        total_inserts = len(self.inserts)

        if total_inserts <= 0:
            raise Exception("no inserts to make")

        config = DBConfigFactory.build(self.db_config_file)

        print("Starting import...")

        insert_count = 0
        duplicate_count = 0
        error_count = 0

        duplicates = []
        errors = []

        # build database connection
        try:
            with connect(
                    host=config[DBConfig.CONFIG_HOST],
                    port=int(config[DBConfig.CONFIG_PORT]),
                    user=config[DBConfig.CONFIG_USER],
                    password=config[DBConfig.CONFIG_PASS],
                    database=config[DBConfig.CONFIG_DBASE],
            ) as connection:

                config[DBConfig.CONFIG_PASS] = None
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
                                if insert_count % self.commit_size == 0:
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
                                self.logger.warning("Error running an insert:\n%s\nContinuing...\n" % insert)
                                self.logger.warning(e)

                                errors.append(insert)

                                error_count += 1

                            print("\r\t%d / %d (%d duplicates, %d errors)" %
                                  (insert_count, total_inserts, duplicate_count, error_count),
                                  end='', flush=True)

                        # commit remaining inserts
                        connection.commit()

                except Error as e:
                    self.logger.error("Error running inserts", e)

        except Error as e:
            self.logger.error("Error connecting to database", e)

        date_time = datetime.now()

        # duplicates report written to file
        if duplicate_count > 0:

            dupe_file = "./duplicates_%s_%s.sql" % (self.importer_name, date_time.strftime("%Y%m%d-%H%M%S"))

            msg = "Encountered duplicate entries. Dumping failed inserts to file %s" % dupe_file

            print("\n\n%s" % msg)
            self.logger.warning(msg)

            with open(dupe_file, 'w', encoding='utf-8') as filehandle:
                for insert in duplicates:
                    filehandle.write("%s\n" % insert)

        if error_count > 0:
            errors_file = "./errors_%s_%s.sql" % (self.importer_name, date_time.strftime("%Y%m%d-%H%M%S"))

            msg = "Encountered errors inserting entries. Dumping failed inserts to file %s" % errors_file

            print("\n\n%s" % msg)
            self.logger.warning(msg)

            with open(errors_file, 'w', encoding='utf-8') as filehandle:
                for insert in errors:
                    filehandle.write("%s\n" % insert)

        # report outcome
        msg = ("Completed %d inserts. Encountered %d duplicates and %d errors" %
               (insert_count, duplicate_count, error_count))
        self.logger.info(msg)
        print("\n%s" % msg)
