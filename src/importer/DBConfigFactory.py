import json
from src.importer.DBConfig import DBConfig

class DBConfigFactory(object):

    def build(config_file):
        with open(config_file) as json_data:
            config = json.load(json_data)

            if config[DBConfig.CONFIG_HOST] is None:
                raise "DB Config missing host"

            if config[DBConfig.CONFIG_PORT] is None:
                raise "DB Config missing port"

            if config[DBConfig.CONFIG_USER] is None:
                raise "DB Config missing user"

            if config[DBConfig.CONFIG_PASS] is None:
                raise "DB Config missing pass"

            if config[DBConfig.CONFIG_DBASE] is None:
                raise "DB Config missing dbase"

        return config
