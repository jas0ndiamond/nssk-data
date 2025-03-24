import logging
import os
from pathlib import Path


class LoggerFactory:
    @staticmethod
    def build_logger(log_dir, file_name, name):

        # TODO make sure file_name matches [a-zA-Z0-9._-]
        # TODO checks on log_dir

        print("Building logger with log_dir: %s" % log_dir)

        # check if the directory already exists
        if not os.path.exists(log_dir):
            # create the log dir if it doesn't exist
            Path(log_dir).mkdir(parents=True, exist_ok=True)

            # check that we succeeded
            if not os.path.exists(log_dir):
                print("Could not create log dir '%s'." % log_dir)
                return None

        # set log file
        log_file = "%s/%s" % (log_dir, file_name)

        # create log config
        logging.basicConfig(filename=log_file,
                            format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')

        return logging.getLogger(name)
