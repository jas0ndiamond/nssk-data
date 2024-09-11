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
        if fields['CosmoTimeStamp'] == '' or fields['CosmoTimeStamp'] is None:
            raise DataValidationException("found invalid CosmoTimeStamp [%s]" % fields['CosmoTimeStamp'])

        if fields['Conductance'] == '' or fields['Conductance'] is None:
            raise DataValidationException("Found invalid Conductance [%s]" % fields['Conductance'])

        # measurement name/type
        if fields['RainfallAmount'] == '' or fields['RainfallAmount'] is None:
            raise DataValidationException("Found invalid RainfallAmount [%s]" % fields['RainfallAmount'])

        # check that conductance measurements are not negative. stored as string
        if float(fields['Conductance']) < 0:
            raise DataValidationException("Found invalid Conductance value [%s]" % fields['Conductance'])

        if float(fields['RainfallAmount']) < 0:
            raise DataValidationException("Found invalid RainfallAmount value [%s]" % fields['RainfallAmount'])

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
