# prepare and kick off dataset update scripts

import argparse
import sys
import logging
import os
import subprocess
import re

from datetime import datetime
from pathlib import Path

from Config import Config
from ConfigException import ConfigException

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

from src.logger.LoggerFactory import LoggerFactory

###################
# globals

LOGGER: logging.Logger = None
LOG_LEVEL = logging.DEBUG
LOG_DIR_DEFAULT = "log"

######################
SKIP_COSMO_RETRIEVAL = True
SKIP_CNV_RAINFALL_RETRIEVAL = True
SKIP_DNV_WHITEWATER_RETRIEVAL = True
SKIP_WATERRANGERS_RETRIEVAL = True

SKIP_COSMO_IMPORT = False
SKIP_CNV_RAINFALL_IMPORT = True
SKIP_DNV_WHITEWATER_IMPORT = True
SKIP_WATERRANGERS_IMPORT = True

CONFIG_FILE = "conf/config.json"

COSMO_RETRIEVAL_SUCCESS = False
CNV_WHITEWATER_RETRIEVAL_SUCCESS = False
DNV_WHITEWATER_RETRIEVAL_SUCCESS = False
WATERRANGERS_RETRIEVAL_SUCCESS = False

CHECK_CONFIG_ONLY = 0

REQUIRED_BINARIES = ["jq", "xargs", "curl", "ssh", "expect"]
WHICH_BIN = "which"


######################
def precheck():
    # check for required binaries used by update-*.sh scripts and other resources

    for binary in REQUIRED_BINARIES:
        completed_proc = subprocess.run([WHICH_BIN, binary], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

        if completed_proc.returncode != 0:
            precheck_msg = "Could not find %s on PATH. Please install %s." % (binary, binary)
            print(precheck_msg)
            LOGGER.error(precheck_msg)
            return False
        else:
            LOGGER.debug("Found %s" % binary)

    return True


def read_config(config_file):
    LOGGER.info("Reading Config")

    # read - r COSMO_DEST_DIR <<<        $(jq - r '.data_sources.cosmo.dump_destination_dir' "$CONFIG_FILE")
    #
    # read - r CNV_RAINFALL_DEST_DIR <<< $(jq - r '.data_sources.cnv_rainfall.dump_destination_dir' "$CONFIG_FILE")
    # read - r CNV_RAINFALL_USER <<<     $(jq - r '.data_sources.cnv_rainfall.user' "$CONFIG_FILE")
    # read - r CNV_RAINFALL_PASS <<<     $(jq - r '.data_sources.cnv_rainfall.pass' "$CONFIG_FILE")
    #
    # read - r DNV_WHITEWATER_DEST_DIR <<< $(jq - r '.data_sources.dnv_whitewater.dump_destination_dir' "$CONFIG_FILE")
    # read - r DNV_WHITEWATER_API_KEY <<<  $(jq - r '.data_sources.dnv_whitewater.api_key' "$CONFIG_FILE")

    return Config(config_file)


def update_cosmo():
    pass


def update_dnv_whitewater():
    # given known, expected sites, query our site's expected channels
    pass


def main(parsed_args):
    ####################
    # runtime setup
    timestamp = "%s" % datetime.now().strftime("%Y%m%d-%H%M%S")

    ####################
    # logger setup

    script_path = os.path.dirname(os.path.realpath(__file__))

    # default log dir is ./log
    if getattr(parsed_args, "log_dir") is not None:
        log_dir = getattr(parsed_args, "log_dir")[0]
    else:
        log_dir = "%s/%s" % (script_path, LOG_DIR_DEFAULT)

    global LOGGER
    try:
        LOGGER = LoggerFactory.build_logger(
            log_dir,
            "datasource-update_%s.log" % timestamp,
            __name__
        )
    except PermissionError as e:
        print("PermissionError creating logger: %s." % repr(e))
    finally:
        if LOGGER is None:
            print("Failed to create logger. Exiting...")
            sys.exit(1)

    LOGGER.setLevel(LOG_LEVEL)
    LOGGER.info("Logger created successfully")

    ####################
    # remaining arg processing

    if getattr(parsed_args, "cfg_file") is not None:
        config_file = getattr(parsed_args, "cfg_file")[0]
    else:
        msg = "Missing config file. Exiting..."
        LOGGER.error(msg)
        print(msg)
        sys.exit(1)

    ####################
    # precheck
    if not precheck():
        msg = "Precheck failed. Exiting..."
        LOGGER.error(msg)
        print(msg)
        sys.exit(1)
    else:
        msg = "Precheck passed. Continuing..."
        LOGGER.debug(msg)
        print(msg)

    ####################
    # config reading and processing
    config = read_config(config_file)
    if config is None:
        msg = "Reading config failed. Exiting..."
        LOGGER.error(msg)
        print(msg)
        sys.exit(1)
    else:
        msg = "Read config successfully."
        LOGGER.info(msg)
        LOGGER.info(config.to_s())
        print(msg)
        print(config.to_s())

    ####################
    # remaining arg processing

    ####################
    # cosmo dump file retrieval

    cosmo_dump_file = None

    # testing with existing dump file
    cosmo_dump_file = "/home/jason/Pub/nssk-data-dumps/cosmo//cosmo_20241211-192255.csv"

    # p = "./scripts/update-cosmo.sh $DEST_DIR/outfile"
    if not SKIP_COSMO_RETRIEVAL:
        LOGGER.info("Running CoSMo retrieval")
        script_path = "%s/scripts/update-cosmo.sh" % script_path
        cosmo_dump_file = "%s/cosmo_%s.csv" % (config.get_cosmo_retrieval_dest_dir(), timestamp)

        msg = "Running CoSMo retrieval. Dumping to file %s" % cosmo_dump_file
        print("%s..." % msg, end='', flush=True)
        LOGGER.info(msg)

        completed_proc = subprocess.run(
            [script_path, cosmo_dump_file]  # , stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
        )

        # print(completed_proc.stdout)

        if completed_proc.returncode == 0:
            msg = "CoSMo retrieval succeeded"
            LOGGER.info(msg)
            print(msg)
            global COSMO_RETRIEVAL_SUCCESS
            COSMO_RETRIEVAL_SUCCESS = True
        else:
            msg = "CoSMo retrieval failed"
            LOGGER.error(msg)
            print(msg)
    else:
        msg = "Skipping CoSMo dataset retrieval"
        LOGGER.info(msg)
        print(msg)
        COSMO_RETRIEVAL_SUCCESS = True

    ####################
    # cosmo dataset import

    if COSMO_RETRIEVAL_SUCCESS or (SKIP_COSMO_RETRIEVAL and cosmo_dump_file is not None):
        msg = "CoSMo dataset retrieval succeeded, running dataset import"
        LOGGER.info(msg)
        print("%s..." % msg, end='', flush=True)

        python_bin = config.get_python_bin()
        global_opts = config.get_importer_global_options()

        cosmo_config_file = config.get_cosmo_update_config()[Config.IMPORTER_CONFIG_FILE_KEY]
        cosmo_script_file = config.get_cosmo_update_config()[Config.IMPORTER_SCRIPT_FILE_KEY]
        script_options = config.get_cosmo_update_config()[Config.IMPORTER_OPTS_KEY]

        # cd ~/homegit/nssk-data/src/cosmo
        # ../../venv/bin/python3 ./cosmo-import.py -cfg ../../conf/cosmo.json doi.org_10.25976_0gvo-9d12.csv

        cmd = [
            python_bin,
            cosmo_script_file,
        ]

        if global_opts:
            cmd.extend(global_opts)

        if script_options:
            cmd.extend(script_options)

        cmd.append("-cfg")
        cmd.append(cosmo_config_file)
        cmd.append(cosmo_dump_file)

        msg = "CoSMo update command: %s" % " ".join(cmd)
        print(msg)
        print(cmd)
        LOGGER.debug(msg)

        # run the update
        completed_proc = subprocess.run(
            cmd
            # , stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
        )

        print(completed_proc.stdout)

        if completed_proc.returncode == 0:
            msg = "CoSMo dataset import succeeded"
            LOGGER.info(msg)
            print(msg)
            COSMO_RETRIEVAL_SUCCESS = True
        else:
            msg = "CoSMo dataset import failed"
            LOGGER.error(msg)
            print(msg)

    else:
        LOGGER.error("CoSMo dataset retrieval failed, skipping dataset import")

    ####################
    # cnv-rainfall

    ####################
    # dnv-whitewater


##############################
if __name__ == "__main__":
    ############################
    # shell args
    #
    # --dry-run                                              read data dump file and output sql statements.
    # -cfg conductivity-rainfall-correlation.json            database config     not required
    # -c                                                     check the config
    ############################

    msg = "Updating NSSK Streamkeepers data sources"
    print(msg)

    # reads sys.argv
    parser = argparse.ArgumentParser(
        description='Retrieve NSSK data sources from remote hosting, and import them into local storage.')
    parser.add_argument('-cfg', nargs=1, dest='cfg_file',
                        help='Config file in json format. Ex: config.json')
    parser.add_argument('-c', dest='cfg_file_check',
                        help='Run a check of the config and exit.')
    parser.add_argument('--log-dir', nargs=1, dest='log_dir',
                        help='Directory to place logs. Ex: /tmp/log')

    # TODO: implement various --skip* flags

    # call main with parsed args
    main(parser.parse_args())
