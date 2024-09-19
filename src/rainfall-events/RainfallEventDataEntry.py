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

class RainfallEventDataEntry(DataEntry):

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

        if fields['RAINFALL_EVENT_START_TIMESTAMP'] == '' or fields['RAINFALL_EVENT_START_TIMESTAMP'] is None:
            raise DataValidationException("found invalid RAINFALL_EVENT_START_TIMESTAMP [%s]" % fields['RAINFALL_EVENT_START_TIMESTAMP'])

        if fields['RAINFALL_EVENT_END_TIMESTAMP'] == '' or fields['RAINFALL_EVENT_END_TIMESTAMP'] is None:
            raise DataValidationException("found invalid RAINFALL_EVENT_END_TIMESTAMP [%s]" % fields['RAINFALL_EVENT_END_TIMESTAMP'])

        # TODO: type constraints. enforce an alphabet on fields where applicable
        # MonitoringLocationID

    def get_db_destination(self):
        return self.monitoringLocationID

    def set_db_destination(self, dest):
        self.monitoringLocationID = dest

    def set_event_id(self, event_id):

        ievent_id = int(event_id)
        # must be set externally, since that's where the event enumeration occurs
        # 7-digit number
        # 2020004 => the 4th event of year 2020
        # first 4 digits must match year of RAINFALL_EVENT_START_TIMESTAMP
        # last digit cannot be 0

        self.set("EVENT_ID", ievent_id)

    def get_event_id(self):
        return self.get("EVENT_ID")

    def get_entry_date(self):
        pass

    def get_entry_time(self):
        pass
