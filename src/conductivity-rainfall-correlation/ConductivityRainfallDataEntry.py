from pathlib import Path
import sys

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

# depends on adding src to sys.path
from src.data.DataEntry import DataEntry
from src.exception.DataValidationException import DataValidationException


# CosmoTimeStamp
# Conductance
# RainfallAmount


# database is determined externally
# site is determined externally

class ConductivityRainfallDataEntry(DataEntry):

    # row_obj is any structure that can be indexed and is iterable
    # csv, json, raw array
    def __init__(self, entry_obj):

        # raise exception if theres a problem
        super().__init__(entry_obj)

        # monitoring location is the destination table name
        self.monitoringLocationID = None

    def _validate_data(self, fields):
        # no narrowing of dataset done here
        # just check for data validity

        ##################
        # fields comprising the timestamp
        if fields['COSMO_TIMESTAMP'] == '' or fields['COSMO_TIMESTAMP'] is None:
            raise DataValidationException("found invalid COSMO_TIMESTAMP [%s]" % fields['COSMO_TIMESTAMP'])

        if fields['CONDUCTANCE_RESULT'] == '' or fields['CONDUCTANCE_RESULT'] is None:
            raise DataValidationException("Found invalid CONDUCTANCE_RESULT [%s]" % fields['CONDUCTANCE_RESULT'])

        # measurement name/type
        if fields['CNV_RAINFALL'] == '' or fields['CNV_RAINFALL'] is None:
            raise DataValidationException("Found invalid CNV_RAINFALL [%s]" % fields['CNV_RAINFALL'])

        # check that conductance measurements are not negative. stored as string
        if float(fields['CONDUCTANCE_RESULT']) < 0:
            raise DataValidationException("Found invalid CONDUCTANCE_RESULT value [%s]" % fields['CONDUCTANCE_RESULT'])

        if float(fields['CNV_RAINFALL']) < 0:
            raise DataValidationException("Found invalid CNV_RAINFALL value [%s]" % fields['CNV_RAINFALL'])

        # TODO: type constraints. enforce an alphabet on fields where applicable
        # MonitoringLocationID

    def get_db_destination(self):
        return self.monitoringLocationID

    def set_db_destination(self, dest):
        self.monitoringLocationID = dest

    def get_entry_date(self):
        pass

    def get_entry_time(self):
        pass
