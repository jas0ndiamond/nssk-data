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

    CNV_HYDROMETRIC_TIMESTAMP_FIELD = "CNVHydrometricTimestamp"
    CNV_HYDROMETRIC_REVISED_STAGE_FIELD = "RevisedFinalStage"

    CNV_FLOWWORKS_TIMESTAMP_FIELD = "CNVFlowworksTimestamp"
    CNV_FLOWWORKS_RAINFALL_START_FIELD = "RainfallStart"
    CNV_FLOWWORKS_RAINFALL_END_FIELD = "RainfallEnd"
    CNV_FLOWWORKS_RAINFALL_AMT_FIELD = "RainfallAmount"



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
                raise DataValidationException("CoSMo measurement missing both conductivity and temperature water measurements")
        elif RainfallIntervalDataEntry.CNV_FLOWWORKS_TIMESTAMP_FIELD in fields:
            if RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_AMT_FIELD not in fields:
                raise DataValidationException("CNV Flowworks measurements missing rainfall amount")
        elif RainfallIntervalDataEntry.CNV_HYDROMETRIC_TIMESTAMP_FIELD in fields:
            if RainfallIntervalDataEntry.CNV_HYDROMETRIC_REVISED_STAGE_FIELD not in fields:
                raise DataValidationException("CNV Hydrometric measurement missing revised stage value")
        else:
            raise DataValidationException("validationerror missing or unexpected timestamp field")

    def get_db_destination(self):
        return self.site_name

    def set_db_destination(self, dest):
        self.site_name = dest

    def get_cosmo_timestamp(self):
        return self.get(self.COSMO_TIMESTAMP_FIELD)

    def get_cosmo_temperature_water(self):
        if self.is_defined(self.COSMO_TEMPERATURE_WATER_FIELD):
            return self.get(self.COSMO_TEMPERATURE_WATER_FIELD)
        return None

    def get_cosmo_conductivity(self):
        if self.is_defined(self.COSMO_CONDUCTIVITY_FIELD):
            return self.get(self.COSMO_CONDUCTIVITY_FIELD)
        return None

    def get_cnv_hydrometric_timestamp(self):
        return self.get(self.CNV_HYDROMETRIC_TIMESTAMP_FIELD)

    def get_cnv_hydrometric_revised_stage(self):
        if self.is_defined(self.CNV_HYDROMETRIC_REVISED_STAGE_FIELD):
            return self.get(self.CNV_HYDROMETRIC_REVISED_STAGE_FIELD)
        return None

    # has a defined timestamp and at least one of Temperature Water or Conductivity- the fields we're looking for
    def has_valid_cosmo_measurement(self):
        pass

    # has a defined timestamp and revised flow
    def has_valid_cnv_hydrometric_measurement(self):
        pass