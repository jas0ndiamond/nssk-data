from pathlib import Path
import sys

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

# depends on adding src to sys.path
from src.data.DataEntry import DataEntry
from src.exception.DataValidationException import DataValidationException

class CNVHydrometricDataEntry(DataEntry):

    # row_obj is any structure that can be indexed and is iterable
    # csv, json, raw array
    def __init__(self, entry_obj):

        # raise exception if there's a problem
        super().__init__(entry_obj)

        # for convenience
        self.site = None

        # seeing entries with missing prelim stage but a coherent revised final stage
        if self.get('Preliminary Discharge (m3/s)') == '':
            self.set('Preliminary Discharge (m3/s)', None)

        # convert date in timestamp to YYYY-MM-DD from yyyy/MM/dd
        self.set( 'yyyy/MM/dd HH:mm:ss', self.get('yyyy/MM/dd HH:mm:ss').replace("/", "-") )

    def _validate_data(self, fields):
        # no narrowing of dataset done here
        # just check for data validity

        ##################
        # fields comprising the timestamp
        if fields['yyyy/MM/dd HH:mm:ss'] == '' or fields['yyyy/MM/dd HH:mm:ss'] is None:
            raise DataValidationException("found empty timestamp")

        # likely the most important measurement
        if fields['Revised - Final Stage (m)'] == '' or fields['Revised - Final Stage (m)'] is None:
            raise DataValidationException("found empty revised final stage")

        # TODO: type constraints. enforce an alphabet on fields where applicable

    def get_db_destination(self):
        return self.site

    def set_db_destination(self, db_site):
        self.site = db_site

    def get_entry_date(self):
        pass

    def get_entry_time(self):
        pass

    schema_mapping = {
        "yyyy/MM/dd HH:mm:ss": "MeasurementTimeStamp",
        "Preliminary Discharge (m3/s)": "PrelimaryDischarge",
        "Revised - Final Stage (m)": "RevisedFinalStage"
    }

    schema = [
        "yyyy/MM/dd HH:mm:ss",
        "Preliminary Discharge (m3/s)",
        "Revised - Final Stage (m)"
    ]