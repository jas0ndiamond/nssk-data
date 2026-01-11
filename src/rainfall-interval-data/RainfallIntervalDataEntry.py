from datetime import datetime
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

    MYSQL_DATE_FMT = "%Y-%m-%d %H:%M:%S"

    # row_obj is any structure that can be indexed and is iterable
    # csv, json, raw array
    def __init__(self, entry_obj):
        # monitoring location is the destination table name
        self.site_name = None

        # timestamp values
        self.timestamp = None
        self.timestamp_str = None

        super().__init__(entry_obj)



        # value buckets

        # some measurements have None Conductivity
        # if super().is_defined(RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD) and self.get(RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD) is None:
        #     self.set(RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD)
        #     if( super().is_defined(RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD) )

    def _set_timestamp(self, timestamp):
        # internal since we set this from field values in construction
        # timestamp handling - are the timestamps fields actually timestamps?
        # store timestamp datetime for datetime comparisons
        # store timestamp string for sorting
        if isinstance(timestamp, datetime):
            # store datetime and string format
            self.timestamp = timestamp
            self.timestamp_str = timestamp.strftime(RainfallIntervalDataEntry.MYSQL_DATE_FMT)
            return True
        elif isinstance(timestamp, str):
            # store converted datetime and string
            self.timestamp_str = timestamp
            self.timestamp = datetime.strptime(timestamp, RainfallIntervalDataEntry.MYSQL_DATE_FMT)
            return True
        return False

    def _validate_data(self, fields):
        # at least one timestamp, and associated measurement
        # validate all timestamp fields
        # validate existing measurements

        # throw an exception if we don't have at least one valid timestamp
        # TODO: acceptable if some measurement fields are missing?

        # timestamp field exists
        has_timestamp_defined = False

        # timestamp is valid
        has_valid_timestamp = False

        if RainfallIntervalDataEntry.COSMO_TIMESTAMP_FIELD in fields:

            # TODO: robust timestamp precedence

            has_timestamp_defined = True
            if RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD not in fields and RainfallIntervalDataEntry.COSMO_TEMPERATURE_WATER_FIELD not in fields:
                raise DataValidationException("CoSMo measurement missing both conductivity and temperature water measurements")
            if fields[RainfallIntervalDataEntry.COSMO_CONDUCTIVITY_FIELD] is None:
                raise DataValidationException("CoSMo measurement has None conductivity")
            if fields[RainfallIntervalDataEntry.COSMO_TEMPERATURE_WATER_FIELD] is None:
                raise DataValidationException("CoSMo measurement has None temperature")

            if self._set_timestamp(fields[RainfallIntervalDataEntry.COSMO_TIMESTAMP_FIELD]):
                has_valid_timestamp = True
            else:
                raise DataValidationException("CoSMo measurement timestamp must be a string or datetime")

        if RainfallIntervalDataEntry.CNV_HYDROMETRIC_TIMESTAMP_FIELD in fields:
            has_timestamp_defined = True
            if RainfallIntervalDataEntry.CNV_HYDROMETRIC_REVISED_STAGE_FIELD not in fields:
                raise DataValidationException("CNV Hydrometric measurement missing revised stage value")
            if fields[RainfallIntervalDataEntry.CNV_HYDROMETRIC_REVISED_STAGE_FIELD] is None:
                raise DataValidationException("CNV Hydrometric measurement has None revised stage")

            # store as primary timestamp unless we already have done so
            if not has_valid_timestamp:
                if self._set_timestamp(fields[RainfallIntervalDataEntry.CNV_HYDROMETRIC_TIMESTAMP_FIELD]):
                    has_valid_timestamp = True
                else:
                    raise DataValidationException("CNV Hydrometric measurement timestamp must be a string or datetime")

        if RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_START_TIMESTAMP_FIELD in fields:
            has_timestamp_defined = True
            if RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_AMT_FIELD not in fields:
                raise DataValidationException("CNV Flowworks measurements missing rainfall amount")
            if fields[RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_AMT_FIELD] is None:
                raise DataValidationException("CNV Flowworks measurements has None rainfall amount")
            if fields[RainfallIntervalDataEntry.CNV_FLOWWORKS_AIR_TEMPERATURE_FIELD] is None:
                raise DataValidationException("CNV Flowworks measurements has None air temperature")
            if fields[RainfallIntervalDataEntry.CNV_FLOWWORKS_BARO_PRESSURE_FIELD] is None:
                raise DataValidationException("CNV Flowworks measurements has None barometric pressure")

            # store as primary timestamp unless we already have done so
            if not has_valid_timestamp:
                if self._set_timestamp(fields[RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_START_TIMESTAMP_FIELD]):
                    has_valid_timestamp = True
                else:
                    raise DataValidationException("CNV Rainfall measurement timestamp must be a string or datetime")

        # catch if we somehow did not end up with a valid timestamp
        if not has_timestamp_defined or not has_valid_timestamp:
            raise DataValidationException("Validation error: missing or unexpected timestamp field")

        if self.get_timestamp() is None:
            raise DataValidationException("Validation error: found timestamp uninitialized")

        if self.get_timestamp_str() is None:
            raise DataValidationException("Validation error: found timestamp string uninitialized")

    def get_db_destination(self):
        return self.site_name

    def set_db_destination(self, dest):
        self.site_name = dest

    # TODO: look into adding override requirement to DataEntry
    def get_timestamp(self) -> datetime:
        return self.timestamp

    def get_timestamp_str(self) -> str:
        return self.timestamp_str

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

    def set(self, field_name, value) -> None:
        # TODO: if field_name is a timestamp field, need to revalidate and set timestamp/timestamp_str accordingly
        if field_name in [
            RainfallIntervalDataEntry.COSMO_TIMESTAMP_FIELD,
            RainfallIntervalDataEntry.CNV_HYDROMETRIC_TIMESTAMP_FIELD,
            RainfallIntervalDataEntry.CNV_FLOWWORKS_RAINFALL_END_TIMESTAMP_FIELD
        ]:
            # validate and set timestamp/timestamp_str
            # TODO: start here

            pass

            # self._validate_data(self.entry_data)
        else:
            super().set(field_name, value)

    # TODO: add to DataEntry
    def copy(self):
        new_obj = RainfallIntervalDataEntry(self.entry_data)
        new_obj.set_db_destination(self.site_name)

        # TODO: seeing None timestamps at sorting. this should be handled by the constructor call above
        #new_obj._set_timestamp(self.timestamp)

        return new_obj