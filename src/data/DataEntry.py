import pprint
import re
from datetime import datetime


class DataEntry:

    # row_obj is any structure that has a key-value mapping and is iterable
    # dict, csv, json
    # TODO: maybe standardize on dict. defer dict creation to the import, who will have context
    def __init__(self, entry_obj):

        # TODO: basic type enforcement on entry_obj
        # is indexable
        # is iterable

        self._validate_data(entry_obj)

        # at this point basic validation has passed, but still want to clean it up
        # remove alphanumeric+ chars used in sql syntax [ ] { } | " ' ;

        ##################
        # scrub invalid characters from values
        # [ #$%[]{},"'| ]

        # TODO: make static
        scrub_pattern = re.compile(r'[\[\]\'\"\$\#\@\!\{\}\,\|]')

        # set internal state from entry_obj
        for field in entry_obj:
            if entry_obj[field] is None:
                # if it's a None, rely on subclass to validate if None values are acceptable for field
                continue
            elif type(entry_obj[field]) == str:
                value = str(entry_obj[field])

                value = re.sub(scrub_pattern, '', value)

                entry_obj[field] = value
            elif type(entry_obj[field]) == float or type(entry_obj[field]) == int:
                # no scrubbing
                pass
            elif type(entry_obj[field] == datetime):
                pass
                # no scrubbing
            else:
                # no scrubbing
                pass

        self.entry_data = entry_obj

    def _validate_data(self, entry_obj):
        # require override, even if the overrider just returns true
        raise RuntimeError("DataEntry._is_valid not implemented in subclass")

    def get_db_destination(self):
        raise RuntimeError("DataEntry.get_db_destination not implemented in subclass")

    def set_db_destination(self, destination):
        raise RuntimeError("DataEntry.set_db_destination not implemented in subclass")

    def get(self, field_name):
        return self.entry_data[field_name]

    def is_defined(self, field_name):
        return field_name in self.entry_data

    def set(self, field_name, value):
        # TODO require field name to be present? need to check how this is used
        #if self.is_defined(field_name):
        self.entry_data[field_name] = value

    def _get_entry_data(self):
        return self.entry_data

    # def get_entry_date(self):
    #     pass
    #
    # def get_entry_time(self):
    #     pass

    def to_s(self):
        return pprint.pformat(self.entry_data)

    def pprint(self):
        pprint.pprint(self.entry_data)