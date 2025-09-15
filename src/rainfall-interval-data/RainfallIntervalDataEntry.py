from pathlib import Path
import sys

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

# depends on adding src to sys.path
from src.data.DataEntry import DataEntry
from src.exception.DataValidationException import DataValidationException

class RainfallIntervalDataEntry(DataEntry):

    COSMO_TIMESTAMP_FIELD = "COSMO_TIMESTAMP"
    COSMO_CONDUCTIVITY_FIELD = "COSMO_CONDUCTANCE_RESULT"
    COSMO_TEMPERATURE_WATER_FIELD = "COSMO_TEMPERATURE_WATER"

    CNV_HYDROMETRIC_TIMESTAMP_FIELD = "CNV_HYDROMETRIC_TIMESTAMP"
    CNV_HYDROMETRIC_REVISED_STAGE_FIELD = "CNV_HYDROMETRIC_REVISED_FINAL_STAGE"

    CNV_FLOWWORKS_RAINFALL_START_TIMESTAMP_FIELD = "CNV_FLOWWORKS_RAINFALL_START_TIMESTAMP"
    CNV_FLOWWORKS_RAINFALL_END_TIMESTAMP_FIELD = "CNV_FLOWWORKS_RAINFALL_END_TIMESTAMP"
    CNV_FLOWWORKS_RAINFALL_AMT_FIELD = "CNV_FLOWWORKS_RAINFALL_AMOUNT"
    CNV_FLOWWORKS_BARO_PRESSURE_FIELD = "CNV_FLOWWORKS_BARO_PRESSURE"
    CNV_FLOWWORKS_AIR_TEMPERATURE_FIELD = "CNV_FLOWWORKS_AIR_TEMPERATURE"



    # row_obj is any structure that can be indexed and is iterable
    # csv, json, raw array
    def __init__(self, entry_obj):
        # raise exception if there's a problem
        super().__init__(entry_obj)

        # monitoring location is the destination table name
        self.site_name = None

        # value buckets

        # some measurements have None Conductivity
        # if super().is_defined(RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD) and self.get(RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD) is None:
        #     self.set(RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD)
        #     if( super().is_defined(RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD) )


    def _validate_data(self, fields):
        # at least one timestamp, and associated measurement

        # TODO: exception if cosmo ts defined but either temp water or conductivity is None
        # needs to be handled by caller

        if RainfallIntervalDataEntry.COSMO_TIMESTAMP_FIELD in fields:
            if RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD not in fields and RainfallIntervalDataEntry.COSMO_TEMPERATURE_WATER_FIELD not in fields:
                raise DataValidationException("CoSMo measurement missing both conductivity and temperature water measurements")
        elif RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_START_TIMESTAMP_FIELD in fields:
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
        if self.is_defined(self.COSMO_TIMESTAMP_FIELD):
            return self.get(self.COSMO_TIMESTAMP_FIELD)
        return None

    def get_cosmo_temperature_water(self):
        if self.is_defined(self.COSMO_TEMPERATURE_WATER_FIELD):
            return self.get(self.COSMO_TEMPERATURE_WATER_FIELD)
        return None

    def get_cosmo_conductivity(self):
        if self.is_defined(self.COSMO_CONDUCTIVITY_FIELD):
            return self.get(self.COSMO_CONDUCTIVITY_FIELD)
        return None

    def get_cnv_hydrometric_timestamp(self):
        if self.is_defined(self.CNV_HYDROMETRIC_TIMESTAMP_FIELD):
            return self.get(self.CNV_HYDROMETRIC_TIMESTAMP_FIELD)
        return None

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