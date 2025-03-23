import json
import os

from ConfigException import ConfigException


# a config object contains the datasource update process configuration for use by invoked scripts and processes
# for both data source retrieval and database import
class Config:
    DATASOURCES_KEY = "data_sources"

    COSMO_KEY = "cosmo"
    CNV_FLOWWORKS_KEY = "cnv_flowworks"
    DNV_FLOWWORKS_KEY = "dnv_flowworks"
    WATERRANGERS_KEY = "waterrangers"

    DATA_IMPORT_KEY = "data_import"
    DATA_IMPORT_IMPORTERS_KEY = "importers"
    GLOBAL_OPTS_KEY = "global_options"
    PYTHON_KEY = "python"
    PYTHON_BIN_KEY = "bin"

    IMPORTER_CONFIG_FILE_KEY = "config_file"
    IMPORTER_SCRIPT_FILE_KEY = "importer_script_file"
    LOG_DIR_KEY = "log_dir"
    IMPORTER_OPTS_KEY = "options"
    DEST_DIR_KEY = "destination_dir"
    USER_KEY = "user"
    PASS_KEY = "pass"
    API_KEY_KEY = "api_key"

    def __init__(self, file):
        self.config = None

        self._load(file)
        self._check()

    def _load(self, file):

        # check file exists
        if not os.path.exists(file):
            raise ConfigException("Could not find config file: %s: " % file)

        json_data = open(file).read()

        self.config = json.loads(json_data)

    def _check(self):

        if self.config is None:
            raise ConfigException("Config must be read from file before checking")

        # check each data source and their hardcoded fields

        #######################
        # cosmo

        # cosmo directives
        if self.config[Config.DATASOURCES_KEY][Config.COSMO_KEY] is None:
            raise ConfigException("Missing CoSMo section in config")
        if self.config[Config.DATASOURCES_KEY][Config.COSMO_KEY][Config.DEST_DIR_KEY] is None:
            raise ConfigException("Missing CoSMo destination directory in config")

        # dest dir
        # require it to exist already
        # TODO: try to make directory if it doesn't exist
        if not os.path.exists(self.config[Config.DATASOURCES_KEY][Config.COSMO_KEY][Config.DEST_DIR_KEY]):
            raise ConfigException("CoSMo destination directory does not exist")

        #######################
        # dnv flowworks
        if self.config[Config.DATASOURCES_KEY][Config.DNV_FLOWWORKS_KEY] is None:
            raise ConfigException("Missing DNV Flowworks section in config")
        if self.config[Config.DATASOURCES_KEY][Config.DNV_FLOWWORKS_KEY][Config.DEST_DIR_KEY] is None:
            raise ConfigException("Missing DNV Flowworks destination directory in config")
        if self.config[Config.DATASOURCES_KEY][Config.DNV_FLOWWORKS_KEY][Config.API_KEY_KEY] is None:
            raise ConfigException("Missing DNV Flowworks api key in config")

        if not os.path.exists(self.config[Config.DATASOURCES_KEY][Config.DNV_FLOWWORKS_KEY][Config.DEST_DIR_KEY]):
            raise ConfigException("DNV Flowworks destination directory does not exist")

        #######################
        # cnv rainfall
        if self.config[Config.DATASOURCES_KEY][Config.CNV_FLOWWORKS_KEY] is None:
            raise ConfigException("Missing CNV Flowworks section in config")
        if self.config[Config.DATASOURCES_KEY][Config.CNV_FLOWWORKS_KEY][Config.DEST_DIR_KEY] is None:
            raise ConfigException("Missing CNV Flowworks destination directory in config")
        if self.config[Config.DATASOURCES_KEY][Config.CNV_FLOWWORKS_KEY][Config.USER_KEY] is None:
            raise ConfigException("Missing CNV Flowworks user in config")
        if self.config[Config.DATASOURCES_KEY][Config.CNV_FLOWWORKS_KEY][Config.PASS_KEY] is None:
            raise ConfigException("Missing CNV Flowworks pass in config")

        if not os.path.exists(self.config[Config.DATASOURCES_KEY][Config.CNV_FLOWWORKS_KEY][Config.DEST_DIR_KEY]):
            raise ConfigException("CNV Flowworks destination directory does not exist")

        #######################
        # water rangers
        if self.config[Config.DATASOURCES_KEY][Config.WATERRANGERS_KEY] is None:
            raise ConfigException("Missing Waterrangers section in config")

        if not os.path.exists(self.config[Config.DATASOURCES_KEY][Config.WATERRANGERS_KEY][Config.DEST_DIR_KEY]):
            raise ConfigException("Waterrangers destination directory does not exist")

        #######################
        # importers

    def get_python_bin(self):
        return self.config[Config.DATA_IMPORT_KEY][Config.PYTHON_KEY][Config.PYTHON_BIN_KEY]

    def get_importer_global_options(self):
        return self.config[Config.DATA_IMPORT_KEY][Config.GLOBAL_OPTS_KEY]

    ###########################
    # destination dirs
    def get_cosmo_retrieval_dest_dir(self):
        return self.config[Config.DATASOURCES_KEY][Config.COSMO_KEY][Config.DEST_DIR_KEY]

    def get_cnv_flowworks_dest_dir(self):
        return self.config[Config.DATASOURCES_KEY][Config.CNV_FLOWWORKS_KEY][Config.DEST_DIR_KEY]

    ###########################
    # retrieval configs

    def get_cnv_flowworks_retrieval_config(self):
        return self.config[Config.DATASOURCES_KEY][Config.CNV_FLOWWORKS_KEY]


    ###########################
    # update configs
    def get_cosmo_update_config(self):
        return self.config[Config.DATA_IMPORT_KEY][Config.DATA_IMPORT_IMPORTERS_KEY][Config.COSMO_KEY]

    def get_cnv_flowworks_update_config(self):
        return self.config[Config.DATASOURCES_KEY][Config.DATA_IMPORT_IMPORTERS_KEY][Config.CNV_FLOWWORKS_KEY]

    def get_dnv_whitewater_update_config(self):
        return self.config[Config.DATASOURCES_KEY][Config.DATA_IMPORT_IMPORTERS_KEY][Config.DNV_FLOWWORKS_KEY]

    def get_waterrangers_update_config(self):
        return self.config[Config.DATASOURCES_KEY][Config.DATA_IMPORT_IMPORTERS_KEY][Config.WATERRANGERS_KEY]

    def dump(self):
        print("Config:\n%s" % self.to_s)

    def to_s(self):
        return json.dumps(self.config, indent=4)
