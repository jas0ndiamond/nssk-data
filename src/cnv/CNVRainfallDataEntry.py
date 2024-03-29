from pathlib import Path
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


class CNVRainfallDataEntry(DataEntry):

    # row_obj is any structure that can be indexed and is iterable
    # csv, json, raw array
    def __init__(self, entry_obj):

        # raise exception if theres a problem
        super().__init__(entry_obj)

        # at this point we have a valid entry, but still want to clean it up
        # TODO: remove alphanumeric+ chars used in sql syntax [ ] { } | " ' ;

        # only one site: set in generate_db_setup.py
        self.site = "CNV"

        ##################
        # value buckets - these can be empty in the dump

        if self.get('Hourly Rainfall (mm)') == '':
            self.set('Hourly Rainfall (mm)', None)

    def _validate_data(self, fields):
        # no narrowing of dataset done here
        # just check for data validity

        if fields['Rainfall (mm)'] == '' or fields['Rainfall (mm)'] is None:
            raise DataValidationException("Missing Rainfall (mm) [%s]" % fields['Rainfall (mm)'])

        if fields['yyyy/MM/dd HH:mm:ss'] == '' or fields['yyyy/MM/dd HH:mm:ss'] is None:
            raise DataValidationException("Missing yyyy/MM/dd HH:mm:ss [%s]" % fields['yyyy/MM/dd HH:mm:ss'])

        # TODO: type constraints. enforce an alphabet on fields where applicable

        try:
            float(fields["Air Temperature - 5 min Intervals (°C)"])
        except Exception as e:
            raise DataValidationException("found invalid Air Temperature - 5 min Intervals (°C) [%s]" %
                                          fields['Air Temperature - 5 min Intervals (°C)'])

        if (float(fields["Air Temperature - 5 min Intervals (°C)"]) > 80
                or float(fields["Air Temperature - 5 min Intervals (°C)"]) < -80):
            raise DataValidationException("found out-of-range Air Temperature - 5 min Intervals (°C) [%s]" %
                                          fields['Air Temperature - 5 min Intervals (°C)'])

        try:
            float(fields["Barometer 5 min Intervals (mbar)"])
        except Exception as e:
            raise DataValidationException("found invalid Barometer 5 min Intervals (mbar) [%s]" %
                                          fields['Barometer 5 min Intervals (mbar)'])

        try:
            float(fields["Rainfall (mm)"])
        except Exception as e:
            raise DataValidationException("found invalid Rainfall (mm) [%s]" % fields['Rainfall (mm)'])

        return True

    def get_db_destination(self):
        return self.site

    def get_entry_date(self):
        pass

    def get_entry_time(self):
        pass
