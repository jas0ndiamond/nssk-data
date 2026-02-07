from pathlib import Path
import re
import sys


path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

# depends on adding src to sys.path
from src.data.DataEntry import DataEntry
from src.exception.DataValidationException import DataValidationException


# yyyy/MM/dd HH:mm:ss
# Air Temperature - 5 min Intervals (°C)
# Barometer 5 min Intervals (mbar)
# Hourly Rainfall (mm)
# Rainfall (mm)

# schema
TIMESTAMP_FIELD = "yyyy/MM/dd HH:mm:ss"
BARO_PRES_FIELD = "Barometer 5 min Intervals (mbar)"
AIR_TEMP_FIELD = "Air Temperature - 5 min Intervals (°C)"
HOURLY_RAINFALL_FIELD = "Hourly Rainfall (mm)"
RAINFALL_FIELD = "Rainfall (mm)"

# in C
AIR_TEMP_MIN = -70
AIR_TEMP_MAX = 70

# in mbar
BARO_PRES_MIN = 400
BARO_PRES_MAX = 10000

# in mm
RAINFALL_MIN = 0
RAINFALL_MAX = 800

# timestamp regex matching "yyyy/MM/dd HH:mm:ss"
TS_REGEX = re.compile(r"\b\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\b")


class CNVFlowworksDataEntry(DataEntry):

    # row_obj is any structure that can be indexed and is iterable
    # csv, json, raw array
    def __init__(self, entry_obj):

        # raise exception if there's a problem
        super().__init__(entry_obj)

        # at this point we have a valid entry, but still want to clean it up
        # TODO: remove alphanumeric+ chars used in sql syntax [ ] { } | " ' ;

        # only one site: set in generate_db_setup.py
        self.site = "CNVRain"

        ##################
        # value buckets - these can be empty in the dump

        if self.get(HOURLY_RAINFALL_FIELD) == '':
            self.set(HOURLY_RAINFALL_FIELD, None)

    def _validate_data(self, fields):
        # no narrowing of dataset done here
        # just check for data validity

        # rainfall/hourly rainfall measurements begin in 1994, other measurements are populated much later
        # require timestamp and valid rainfall data
        # okay if other measurements are missing

        # missing timestamp field
        if fields[TIMESTAMP_FIELD] == '' or fields[TIMESTAMP_FIELD] is None:
            raise DataValidationException(f"Missing measurement timestamp [{TIMESTAMP_FIELD}]")

        # invalid timestamp field
        if not TS_REGEX.search(fields[TIMESTAMP_FIELD]):
            raise DataValidationException(f"Invalid timestamp [{TIMESTAMP_FIELD}]")

        # missing rainfall field
        if fields[RAINFALL_FIELD] == '' or fields[RAINFALL_FIELD] is None:
            raise DataValidationException(f"Missing rainfall amount [{fields[RAINFALL_FIELD]}]")

        ###################
        # hourly rainfall - required to be present and valid
        # however only present at the 1h mark
        if fields[HOURLY_RAINFALL_FIELD] != "":
            try:
                float(fields[HOURLY_RAINFALL_FIELD])
            except Exception as e:
                raise DataValidationException(f"found invalid hourly rainfall [{fields[RAINFALL_FIELD]}]")

            # reject unrealistic hourly rainfall readings
            if float(fields[HOURLY_RAINFALL_FIELD]) <= RAINFALL_MIN or float(fields[HOURLY_RAINFALL_FIELD]) > RAINFALL_MAX:
                raise DataValidationException(f"found out-of-range hourly rainfall [{fields[HOURLY_RAINFALL_FIELD]}]")

        ###################
        # rainfall amount - required to be present and valid
        try:
            float(fields[RAINFALL_FIELD])
        except Exception as e:
            raise DataValidationException(f"found invalid rainfall [{fields[RAINFALL_FIELD]}]")

        # reject unrealistic rainfall readings
        if float(fields[RAINFALL_FIELD]) <= RAINFALL_MIN or float(fields[RAINFALL_FIELD]) >= RAINFALL_MAX:
            raise DataValidationException(f"found out-of-range rainfall [{fields[RAINFALL_FIELD]}]")

        ###################
        # air temperature
        if fields[AIR_TEMP_FIELD] != "":
            try:
                float(fields[AIR_TEMP_FIELD])
            except Exception as e:
                raise DataValidationException(
                    "found invalid air temperature [{fields[AIR_TEMP_FIELD]}]"
                )

            # reject unrealistic temperature readings
            if (float(fields[AIR_TEMP_FIELD]) >= AIR_TEMP_MAX
                    or float(fields[AIR_TEMP_FIELD]) <= AIR_TEMP_MIN):
                raise DataValidationException(
                    f"found out-of-range air temperature [{fields[AIR_TEMP_FIELD]}]"
                )

        ###################
        # barometric pressure
        if fields[BARO_PRES_FIELD] != "":
            try:
                float(fields[BARO_PRES_FIELD])
            except Exception as e:
                raise DataValidationException(f"found invalid barometric pressure [{fields[BARO_PRES_FIELD]}]")

            # reject unrealistic barometric pressure readings
            if float(fields[BARO_PRES_FIELD]) <= BARO_PRES_MIN or float(fields[BARO_PRES_FIELD]) >= BARO_PRES_MAX:
                raise DataValidationException(f"found out-of-range barometric pressure [{fields[BARO_PRES_FIELD]}]")

        return True

    def get_db_destination(self):
        return self.site

    def get_entry_date(self):
        pass

    def get_entry_time(self):
        pass
