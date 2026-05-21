from pathlib import Path
import sys

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

# depends on adding src to sys.path
from src.data.DataEntry import DataEntry
from src.exception.DataValidationException import DataValidationException


# CNV rainfall timestamp as an anchor.
# Rainfall from the CNV rainfall dataset
# Air temperature from the CNV Rainfall -- optional
# CoSMo timestamp
# Conductivity from the CoSMo dataset

# database is determined externally
# site is determined externally

# TODO: better name

class RainfallEventMeasurementsDataEntry(DataEntry):
    # needs to match database table schema
    CNV_FLOWWORKS_TIMESTAMP_FIELD = "CNV_FLOWWORKS_TIMESTAMP"
    CNV_FLOWWORKS_RAINFALL_AMOUNT_FIELD = "CNV_FLOWWORKS_RAINFALL_AMOUNT"
    CNV_AIR_TEMPERATURE_FIELD = "CNV_AIR_TEMPERATURE"
    COSMO_CONDUCTANCE_RESULT_FIELD = "COSMO_CONDUCTANCE_RESULT"
    DNV_FLOWWORKS_FLOW_READING_FIELD = "DNV_FLOWWORKS_FLOW_READING"
    RAINFALL_EVENT_ID_FIELD = "RAINFALL_EVENT_ID"

    # row_obj is any structure that can be indexed and is iterable
    # csv, json, raw array
    def __init__(self, entry_obj):

        # raise exception if there's a problem
        super().__init__(entry_obj)

        # monitoring location is the destination table name
        self.monitoringLocationID = None

    def _validate_data(self, fields):
        # no narrowing of dataset done here
        # just check for data validity

        ##################
        # fields comprising the timestamp

        # cnv rainfall timestamp
        if (fields[RainfallEventMeasurementsDataEntry.CNV_FLOWWORKS_TIMESTAMP_FIELD] == '' or
                fields[RainfallEventMeasurementsDataEntry.CNV_FLOWWORKS_TIMESTAMP_FIELD] is None):
            raise DataValidationException("found invalid CNV_FLOWWORKS_TIMESTAMP [%s]" %
                                          fields[RainfallEventMeasurementsDataEntry.CNV_FLOWWORKS_TIMESTAMP_FIELD])

        # cnv rainfall amount present check. required.
        if (fields[RainfallEventMeasurementsDataEntry.CNV_FLOWWORKS_RAINFALL_AMOUNT_FIELD] == ''
                or fields[RainfallEventMeasurementsDataEntry.CNV_FLOWWORKS_RAINFALL_AMOUNT_FIELD] is None):
            raise DataValidationException("Found None or empty CNV_FLOWWORKS_RAINFALL_AMOUNT [%s]" %
                                          fields[RainfallEventMeasurementsDataEntry.CNV_FLOWWORKS_RAINFALL_AMOUNT_FIELD])

        # check cnv rainfall > 0
        if float(fields[RainfallEventMeasurementsDataEntry.CNV_FLOWWORKS_RAINFALL_AMOUNT_FIELD]) < 0:
            raise DataValidationException("Found invalid CNV_FLOWWORKS_RAINFALL_AMOUNT value [%s]" %
                                          fields[RainfallEventMeasurementsDataEntry.CNV_FLOWWORKS_RAINFALL_AMOUNT_FIELD])

        # cosmo conductance value. Can be none if there's no correlated measurement. Validity check on value if present
        # check that conductance measurement is not negative. stored as string
        if (fields[RainfallEventMeasurementsDataEntry.COSMO_CONDUCTANCE_RESULT_FIELD] != '' and
                fields[RainfallEventMeasurementsDataEntry.COSMO_CONDUCTANCE_RESULT_FIELD] is not None and
                float(fields[RainfallEventMeasurementsDataEntry.COSMO_CONDUCTANCE_RESULT_FIELD]) < 0):
            raise DataValidationException("Found invalid CONDUCTANCE_RESULT [%s]" %
                                          fields[RainfallEventMeasurementsDataEntry.COSMO_CONDUCTANCE_RESULT_FIELD])

        # dnv flowworks flow reading. Can be none if there's no correlated measurement.
        # Validity check on value if present
        # check that flow reading is not negative. stored as string
        if (fields[RainfallEventMeasurementsDataEntry.DNV_FLOWWORKS_FLOW_READING_FIELD] != '' and
                fields[RainfallEventMeasurementsDataEntry.DNV_FLOWWORKS_FLOW_READING_FIELD] is not None and
                float(fields[RainfallEventMeasurementsDataEntry.DNV_FLOWWORKS_FLOW_READING_FIELD]) < 0):
            raise DataValidationException("Found invalid DNV_FLOWWORKS_FLOW_READING_FIELD [%s]" %
                                          fields[RainfallEventMeasurementsDataEntry.DNV_FLOWWORKS_FLOW_READING_FIELD])

        # event id
        if (fields[RainfallEventMeasurementsDataEntry.RAINFALL_EVENT_ID_FIELD] is None
                or fields[RainfallEventMeasurementsDataEntry.RAINFALL_EVENT_ID_FIELD] == ''):
            raise DataValidationException("Found None or empty RAINFALL_EVENT_ID")

        if int(fields[RainfallEventMeasurementsDataEntry.RAINFALL_EVENT_ID_FIELD]) < 0:
            raise DataValidationException("Found invalid EVENT_ID [%s]" %
                                          fields[RainfallEventMeasurementsDataEntry.RAINFALL_EVENT_ID_FIELD])

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
