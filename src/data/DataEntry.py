import pprint
import re


class DataEntry:

    # row_obj is any structure that can be indexed and is iterable
    # csv, json, raw array
    def __init__(self, row_obj):

        if self._is_valid(row_obj) is False:
            raise "Invalid data. Cannot build data entry"

        # TODO: type constraints
        # is indexable
        # is iterable

        # at this point we have a valid entry, but still want to clean it up
        # remove alphanumeric+ chars used in sql syntax [ ] { } | " ' ;

        ##################
        # scrub invalid characters from values
        # [ #$%[]{},"'| ]

        scrub_pattern = re.compile(r'[\[\]\'\"\$\#\@\!\{\}\,\|]')

        for field in row_obj:
            re.sub(scrub_pattern, '', row_obj[field])

        self.row_data = row_obj

    def _is_valid(self, row_obj):
        raise Exception("DataEntry._is_valid not implemented in subclass")

    def get(self, field_name):
        return self.row_data[field_name]

    def is_defined(self, field_name):
        return self.row_data[field_name] is not None

    def set(self, field_name, value):
        self.row_data[field_name] = value

    def _get_row_data(self):
        return self.row_data

    # def get_entry_date(self):
    #     pass
    #
    # def get_entry_time(self):
    #     pass

    def to_s(self):
        pprint.pprint(self.fields)
