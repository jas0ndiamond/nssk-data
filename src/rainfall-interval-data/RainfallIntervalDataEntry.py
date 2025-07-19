from pathlib import Path
import sys

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

# depends on adding src to sys.path
from src.data.DataEntry import DataEntry
from src.exception.DataValidationException import DataValidationException

class RainfallIntervalDataEntry(DataEntry):

    COSMO_TIMESTAMP_FIELD = "CosmoTimestamp"
    COSMO_CONDUCTIVITY_FIELD = "Conductivity"
    COSMO_TEMPERATURE_WATER_FIELD = "TemperatureWater"

    CNV_FLOWWORKS_TIMESTAMP_FIELD = "CNVFlowworksTimestamp"
    CNV_FLOWWORKS_RAINFALL_FIELD = "Rainfall"

    CNV_HYDROMETRIC_TIMESTAMP_FIELD = "CNVHydrometricTimestamp"
    CNV_HYDROMETRIC_REVISED_STAGE_FIELD = "RevisedFinalStage"

    # row_obj is any structure that can be indexed and is iterable
    # csv, json, raw array
    def __init__(self, entry_obj):
        # raise exception if there's a problem
        super().__init__(entry_obj)

        # monitoring location is the destination table name
        self.site_name = None

    def _validate_data(self, fields):
        # at least one timestamp, and associated measurement

        if RainfallIntervalDataEntry.COSMO_TIMESTAMP_FIELD in fields:
            if RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD not in fields and RainfallIntervalDataEntry.COSMO_TEMPERATURE_WATER_FIELD not in fields:
                raise DataValidationException("validationerror")
        elif RainfallIntervalDataEntry.CNV_FLOWWORKS_TIMESTAMP_FIELD in fields:
            if RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_FIELD not in fields:
                raise DataValidationException("validationerror")
        elif RainfallIntervalDataEntry.CNV_HYDROMETRIC_TIMESTAMP_FIELD in fields:
            if RainfallIntervalDataEntry.CNV_HYDROMETRIC_REVISED_STAGE_FIELD in fields:
                raise DataValidationException("validationerror")
        else:
            raise DataValidationException("validationerror missing or unexpected timestamp field")

    def get_db_destination(self):
        return self.site_name

    def set_db_destination(self, dest):
        self.site_name = dest