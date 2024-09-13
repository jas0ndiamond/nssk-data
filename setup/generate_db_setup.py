import sys
import json
from string import Template

# set up the nssk mysql database
#
# generates sql statements to run locally as root
# adds databases
# adds users
# configures remote access

# config file params
DB_HOST = 'host'
DB_PORT = 'port'
DB_SETUP_USER = 'setup-user'
DB_SETUP_USER_PASS = 'setup-pass'

# TODO: add to config file? Maybe not- each has a set of tables with variable schemas
NSSK_COSMO_DB = "NSSK_COSMO"
NSSK_DNV_WHITEWATER_DB = "NSSK_DNV_WHITEWATER"
NSSK_CNV_RF_DB = "NSSK_CNV_RAINFALL"
NSSK_CONDUCTIVITY_RAINFALL_CORRELATION_DB = "NSSK_CONDUCTIVITY_RAINFALL_CORRELATION"
NSSK_RAINFALL_EVENTS_DB = "NSSK_RAINFALL_EVENTS"
NSSK_RAINFALL_EVENT_DATA_DB = "NSSK_RAINFALL_EVENT_DATA"

DATABASES = [
    NSSK_COSMO_DB,
    NSSK_DNV_WHITEWATER_DB,
    NSSK_CNV_RF_DB,
    NSSK_CONDUCTIVITY_RAINFALL_CORRELATION_DB,
    NSSK_RAINFALL_EVENTS_DB,
    NSSK_RAINFALL_EVENT_DATA_DB
]

# TODO: add to config file?
LOCAL_NETWORK = "192.168.%.%"
CONTAINER_NETWORK = "9.9.1.%"  # configured on container host
WAN_NETWORK = "%"

NSSK_USERS_KEY = "users"
NSSK_USER = "nssk"
NSSK_IMPORT_USER = "nssk_import"
NSSK_BACKUP_USER = "nssk_backup"
NSSK_ADMIN_USER = "nssk_admin"

#####################

cosmo_monitoring_location_ids = [
    "WAGG01",
    "WAGG02",
    "WAGG03",
    "MOSQ01",
    "MOSQ02",
    "MOSQ03",
    "MOSQ04",
    "MOSQ05",
    "MOSQ06",
    "MOSQ07",
    "MISS01",
    "MACK02",
    "MACK03",
    "MACK04",
    "MACK05",
    "HAST01",
    "HAST02",
    "HAST03"
]

cnv_rainfall_sites = [
    "WAGG01",
    "WAGG03"
]

dnv_whitewater_sites = [
    "DNV"
]

conductivity_rainfall_correlation_sites = [
    "WAGG01",
    "WAGG02",
    "WAGG03"
]

#####################

# Dockerfile and start.sh expect this file. do not make configurable
scriptfile_target_dir = "../docker/setup/"
create_db_scriptfile = "%s0_create_dbs.sql" % scriptfile_target_dir
create_users_scriptfile = "%s1_create_users.sql" % scriptfile_target_dir
create_nssk_cosmo_tables_scriptfile = "%s2_create_nssk_cosmo_tables.sql" % scriptfile_target_dir
create_cnv_rainfall_tables_scriptfile = "%s3_create_cnv_rainfall_tables.sql" % scriptfile_target_dir
create_dnv_whitewater_tables_scriptfile = "%s4_create_dnv_whitewater_tables.sql" % scriptfile_target_dir
create_conductivity_rainfall_correlation_tables_scriptfile = "%s5_create_conductivity_rainfall_correlation_tables.sql" % scriptfile_target_dir
create_rainfall_events_tables_scriptfile = "%s6_create_rainfall_events_tables.sql" % scriptfile_target_dir
create_rainfall_event_data_tables_scriptfile = "%s7_create_rainfall_event_data_tables.sql" % scriptfile_target_dir

#####################

config = {}
db_setup_statements = []
user_setup_statements = []
create_nssk_cosmo_tables = []
create_cnv_rainfall_tables = []
create_dnv_whitewater_tables = []
create_conductivity_rainfall_correlation_tables = []
create_rainfall_events_tables = []
create_rainfall_event_data_tables = []


#############################

def write_setup_scripts():
    print("Writing db setup script to %s" % create_db_scriptfile)
    with open(create_db_scriptfile, 'w') as handle:
        handle.writelines("%s\n" % line for line in db_setup_statements)

    print("Writing user setup script to %s" % create_users_scriptfile)
    with open(create_users_scriptfile, 'w') as handle:
        handle.writelines("%s\n" % line for line in user_setup_statements)

    print("Writing NSSK CoSMo table setup script to %s" % create_nssk_cosmo_tables_scriptfile)
    with open(create_nssk_cosmo_tables_scriptfile, 'w') as handle:
        handle.writelines("%s\n" % line for line in create_nssk_cosmo_tables)

    print("Writing CNV Rainfall table setup script to %s" % create_cnv_rainfall_tables_scriptfile)
    with open(create_cnv_rainfall_tables_scriptfile, 'w') as handle:
        handle.writelines("%s\n" % line for line in create_cnv_rainfall_tables)

    print("Writing DNV Whitewater table setup script to %s" % create_dnv_whitewater_tables_scriptfile)
    with open(create_dnv_whitewater_tables_scriptfile, 'w') as handle:
        handle.writelines("%s\n" % line for line in create_dnv_whitewater_tables)

    print("Writing Conductivity Rainfall Correlation table setup script to %s" %
          create_conductivity_rainfall_correlation_tables_scriptfile)
    with open(create_conductivity_rainfall_correlation_tables_scriptfile, 'w') as handle:
        handle.writelines("%s\n" % line for line in create_conductivity_rainfall_correlation_tables)

    print("Writing Rainfall Events table setup script to %s" %
          create_rainfall_events_tables_scriptfile)
    with open(create_rainfall_events_tables_scriptfile, 'w') as handle:
        handle.writelines("%s\n" % line for line in create_rainfall_events_tables)

    print("Writing Rainfall Event Data table setup script to %s" %
          create_rainfall_event_data_tables_scriptfile)
    with open(create_rainfall_event_data_tables_scriptfile, 'w') as handle:
        handle.writelines("%s\n" % line for line in create_rainfall_event_data_tables)

    print("Writing setup script completed")


def check_config():
    ######################
    # check that our required users are defined with passwords

    if NSSK_USER not in config[NSSK_USERS_KEY]:
        raise "NSSK_USER not defined in user config"
    elif config[NSSK_USERS_KEY][NSSK_USER] is None or config[NSSK_USERS_KEY][NSSK_USER] == "":
        raise "NSSK_USER password not defined in user config"

    if NSSK_IMPORT_USER not in config[NSSK_USERS_KEY]:
        raise "NSSK_IMPORT_USER not defined in user config"
    elif config[NSSK_USERS_KEY][NSSK_IMPORT_USER] is None or config[NSSK_USERS_KEY][NSSK_IMPORT_USER] == "":
        raise "NSSK_IMPORT_USER password not defined in user config"

    if NSSK_BACKUP_USER not in config[NSSK_USERS_KEY]:
        raise "NSSK_BACKUP_USER not defined in user config"
    elif config[NSSK_USERS_KEY][NSSK_BACKUP_USER] is None or config[NSSK_USERS_KEY][NSSK_BACKUP_USER] == "":
        raise "NSSK_BACKUP_USER password not defined in user config"

    if NSSK_ADMIN_USER not in config[NSSK_USERS_KEY]:
        raise "NSSK_ADMIN_USER not defined in user config"
    elif config[NSSK_USERS_KEY][NSSK_ADMIN_USER] is None or config[NSSK_USERS_KEY][NSSK_ADMIN_USER] == "":
        raise "NSSK_ADMIN_USER password not defined in user config"


# Create the databases used by the project by running the 'create_databases.sql` script.
# Requires a user that can create databases.
def create_databases():
    for dbname in DATABASES:
        db_setup_statements.append("create database %s;" % dbname)


def configure_users():
    # nssk - standard read-only user for working with data
    # nssk_import - user for importing data from various sources
    # nssk_admin - user/db management
    # nssk_backup - user for executing backups with mysqldump

    ######################
    # create users

    print("Creating database users")

    # create nssk user
    # access from WAN, LAN, container networks
    user_setup_statements.append("CREATE USER '%s'@'%s' IDENTIFIED BY '%s';" %
                                 (NSSK_USER, WAN_NETWORK, config[NSSK_USERS_KEY][NSSK_USER]))

    # create nssk_import
    # access from LAN, container networks
    user_setup_statements.append("CREATE USER '%s'@'%s' IDENTIFIED BY '%s';" %
                                 (NSSK_IMPORT_USER, LOCAL_NETWORK, config[NSSK_USERS_KEY][NSSK_IMPORT_USER]))
    user_setup_statements.append("CREATE USER '%s'@'%s' IDENTIFIED BY '%s';" %
                                 (NSSK_IMPORT_USER, CONTAINER_NETWORK, config[NSSK_USERS_KEY][NSSK_IMPORT_USER]))

    # create nssk_backup
    # access from LAN, container networks
    user_setup_statements.append("CREATE USER '%s'@'%s' IDENTIFIED BY '%s';" %
                                 (NSSK_BACKUP_USER, LOCAL_NETWORK, config[NSSK_USERS_KEY][NSSK_BACKUP_USER]))
    user_setup_statements.append("CREATE USER '%s'@'%s' IDENTIFIED BY '%s';" %
                                 (NSSK_BACKUP_USER, CONTAINER_NETWORK, config[NSSK_USERS_KEY][NSSK_BACKUP_USER]))

    # create nssk_backup
    # access from LAN, container networks
    user_setup_statements.append("CREATE USER '%s'@'%s' IDENTIFIED BY '%s';" %
                                 (NSSK_ADMIN_USER, LOCAL_NETWORK, config[NSSK_USERS_KEY][NSSK_ADMIN_USER]))
    user_setup_statements.append("CREATE USER '%s'@'%s' IDENTIFIED BY '%s';" %
                                 (NSSK_ADMIN_USER, CONTAINER_NETWORK, config[NSSK_USERS_KEY][NSSK_ADMIN_USER]))

    # no longer need passwords
    config[DB_SETUP_USER_PASS] = None
    config[NSSK_USERS_KEY][NSSK_USER] = None
    config[NSSK_USERS_KEY][NSSK_IMPORT_USER] = None
    config[NSSK_USERS_KEY][NSSK_BACKUP_USER] = None
    config[NSSK_USERS_KEY][NSSK_ADMIN_USER] = None

    print("Database user creation completed")

    ######################
    # set user privileges

    print("Configuring user privileges")

    # allow nssk user select access to databases
    for database in DATABASES:
        # WAN_NETWORK is a wildcard so should not need to add LOCAL_NETWORK and CONTAINER_NETWORK
        user_setup_statements.append("GRANT SELECT ON %s.* TO '%s'@'%s';" % (database, NSSK_USER, WAN_NETWORK))

    # allow nssk-import user select access to databases in case inserts need to make decisions
    # local and container networks only
    for database in DATABASES:
        user_setup_statements.append("GRANT SELECT ON %s.* TO '%s'@'%s';" %
                                     (database, NSSK_IMPORT_USER, LOCAL_NETWORK))

        user_setup_statements.append("GRANT SELECT ON %s.* TO '%s'@'%s';" %
                                     (database, NSSK_IMPORT_USER, CONTAINER_NETWORK))

    # add write access to cosmo_data for nssk-import
    # local and container networks only
    for database in DATABASES:
        user_setup_statements.append("GRANT CREATE, INSERT, UPDATE, DELETE ON %s.* TO '%s'@'%s';" %
                                     (database, NSSK_IMPORT_USER, LOCAL_NETWORK))

        user_setup_statements.append("GRANT CREATE, INSERT, UPDATE, DELETE ON %s.* TO '%s'@'%s';" %
                                     (database, NSSK_IMPORT_USER, CONTAINER_NETWORK))

    # allow nssk-admin write access
    # local and container networks only
    for database in DATABASES:
        user_setup_statements.append("GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%s';" %
                                     (database, NSSK_ADMIN_USER, LOCAL_NETWORK))

        user_setup_statements.append("GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%s';" %
                                     (database, NSSK_ADMIN_USER, CONTAINER_NETWORK))

    # backup user permissions
    # local and container networks only
    for database in DATABASES:
        user_setup_statements.append(
            "GRANT SELECT, SHOW VIEW, TRIGGER, LOCK TABLES, EVENT, USAGE ON %s.* TO '%s'@'%s';" %
            (database, NSSK_BACKUP_USER, LOCAL_NETWORK))

        user_setup_statements.append(
            "GRANT SELECT, SHOW VIEW, TRIGGER, LOCK TABLES, EVENT, USAGE ON %s.* TO '%s'@'%s';" %
            (database, NSSK_BACKUP_USER, CONTAINER_NETWORK))

    # backup needs global process privilege
    # local and container networks only
    user_setup_statements.append("GRANT PROCESS ON *.* TO '%s'@'%s';" %
                                 (NSSK_BACKUP_USER, LOCAL_NETWORK))

    user_setup_statements.append("GRANT PROCESS ON *.* TO '%s'@'%s';" %
                                 (NSSK_BACKUP_USER, CONTAINER_NETWORK))

    print("Configuration of user privileges complete")


def limit_remote_root_login():
    # if using a non-root user to set up database, limit that user to only logging in on that host
    if config[DB_SETUP_USER] != 'root':
        user_setup_statements.append("DELETE FROM mysql.user WHERE User='%s' AND Host NOT IN "
                                     "('localhost', '127.0.0.1', '%s');" % (config[DB_SETUP_USER], CONTAINER_NETWORK))

    # specifically limit root
    user_setup_statements.append("DELETE FROM mysql.user WHERE User='root' AND Host NOT IN "
                                 "('localhost', '127.0.0.1', '%s');" % CONTAINER_NETWORK)


def setup_cosmo_tables():
    table_template = Template(open("sql/CoSMo/nssk-cosmo-sensor-table.sql.template").read())

    # set the database to create the tables in
    create_nssk_cosmo_tables.append("use %s;" % NSSK_COSMO_DB)

    for monitoring_location_id in cosmo_monitoring_location_ids:
        create_table_sql = table_template.substitute(MONITORING_LOCATION_ID=monitoring_location_id)
        create_nssk_cosmo_tables.append(create_table_sql)


def setup_cnv_rainfall_tables():
    table_template = Template(open("sql/cnv-rainfall/nssk-cnv-rainfall.sql.template").read())

    # set the database to create the tables in
    create_cnv_rainfall_tables.append("use %s;" % NSSK_CNV_RF_DB)

    for site in cnv_rainfall_sites:
        create_table_sql = table_template.substitute(SITE=site)
        create_cnv_rainfall_tables.append(create_table_sql)


def setup_dnv_whitewater_tables():
    table_template = Template(open("sql/dnv-whitewater/nssk-dnv-whitewater.sql.template").read())

    # set the database to create the tables in
    create_dnv_whitewater_tables.append("use %s;" % NSSK_DNV_WHITEWATER_DB)

    for site in dnv_whitewater_sites:
        create_table_sql = table_template.substitute(SITE=site)
        create_dnv_whitewater_tables.append(create_table_sql)


def setup_conductivity_rainfall_correlation_tables():
    # set the database to create the tables in
    create_conductivity_rainfall_correlation_tables.append("use %s;" % NSSK_CONDUCTIVITY_RAINFALL_CORRELATION_DB)

    table_template = Template(open(
        "sql/conductivity-rainfall-correlation/conductivity-rainfall-correlation.sql.template").read())

    for monitoring_location_id in conductivity_rainfall_correlation_sites:
        create_table_sql = table_template.substitute(MONITORING_LOCATION_ID=monitoring_location_id)
        create_conductivity_rainfall_correlation_tables.append(create_table_sql)


def setup_rainfall_events_tables():
    # set the database to create the tables in
    create_rainfall_events_tables.append("use %s;" % NSSK_RAINFALL_EVENTS_DB)

    table_template = Template(open(
        "sql/rainfall-events/rainfall-events.sql.template").read())

    for monitoring_location_id in cnv_rainfall_sites:
        create_table_sql = table_template.substitute(MONITORING_LOCATION_ID=monitoring_location_id)
        create_rainfall_events_tables.append(create_table_sql)


def setup_rainfall_event_data_tables():
    # set the database to create the tables in
    create_rainfall_event_data_tables.append("use %s;" % NSSK_RAINFALL_EVENT_DATA_DB)

    table_template = Template(open(
        "sql/rainfall-event-data/rainfall-event-data.sql.template").read())

    for monitoring_location_id in cnv_rainfall_sites:
        create_table_sql = table_template.substitute(MONITORING_LOCATION_ID=monitoring_location_id)
        create_rainfall_event_data_tables.append(create_table_sql)


# this may not be necessary any more
# def setup_root_container_login():
#     # let root login from container network. meant to be temporary
#     # UPDATE mysql.user SET Host='9.9.1.%' WHERE Host='localhost' AND User='root';
#     # UPDATE mysql.db SET Host='9.9.1.%' WHERE Host='localhost' AND User='root';
#
#     if config[DB_SETUP_USER] != 'root':
#         setup_statements.append("UPDATE mysql.user SET Host='%s' WHERE Host='localhost' AND User='%s'" %
#                                 (config[DB_SETUP_USER], CONTAINER_NETWORK))
#         setup_statements.append("UPDATE mysql.db SET Host='%s' WHERE Host='localhost' AND User='%s'" %
#                                 (config[DB_SETUP_USER], CONTAINER_NETWORK))
#     # root specifically
#     setup_statements.append("UPDATE mysql.user SET Host='%s' WHERE Host='localhost' AND User='root'" %
#                             CONTAINER_NETWORK)
#     setup_statements.append("UPDATE mysql.db SET Host='%s' WHERE Host='localhost' AND User='root'" %
#                             CONTAINER_NETWORK)
#
#     pass


#####################################
def main(args):
    # find creds file from shell

    usage_msg = "Usage: python3 generate_db_setup.py config.json\n"
    "\tconfig.json: file storing credentials for necessary users "
    "(nssk, nssk-import, nssk-admin, nssk-backup)"

    conf_file = None

    if len(args) == 2:
        if args[1] == "-h" or args[1] == "-help" or args[1] == "--help":
            print(usage_msg)
            exit(1)
        else:
            conf_file = args[1]
    else:
        print(usage_msg)
        exit(1)

    if conf_file is None:
        print("Failed to read conf_file\n%s" % usage_msg)

    ########
    # read config
    json_data = open(conf_file).read()

    global config
    config = json.loads(json_data)
    ########

    # check needed users are defined in conf file and have passwords

    print("Checking config...")
    check_config()
    print("Config check passed")

    # create databases

    print("Creating NSSK databases")
    create_databases()
    print("NSSK databases created")

    # create users and apply permissions for users
    print("Creating NSSK users")
    configure_users()
    limit_remote_root_login()
    print("NSSK users created")

    ###########
    # Core datasets

    # create cosmo sensor tables
    print("Creating CoSMo tables")
    setup_cosmo_tables()
    print("CoSMo tables created")

    # create dnv whitewater tables
    print("Creating DNV Whitewater tables")
    setup_dnv_whitewater_tables()
    print("DNV Whitewater tables completed")

    # create cnv rainfall tables
    print("Creating CNV Rainfall tables")
    setup_cnv_rainfall_tables()
    print("CNV Rainfall tables completed")

    ###########
    # Generative and computed datasets

    # create generative data tables
    print("Creating Conductivity-Rainfall Correlation tables")
    setup_conductivity_rainfall_correlation_tables()
    print("Conductivity-Rainfall Correlation tables completed")

    print("Creating Rainfall Events tables")
    setup_rainfall_events_tables()
    print("Rainfall Events tables completed")

    print("Creating Rainfall Event Data tables")
    setup_rainfall_event_data_tables()
    print("Rainfall Event Data tables completed")

    ###########
    # write our setup script files
    write_setup_scripts()

    print("Setup completed. Move your credentials file to a secure location.")


##############################
if __name__ == "__main__":
    main(sys.argv)
