import pprint
import re


class DataEntry:

    # row_obj is any structure that can be indexed and is iterable
    # csv, json, raw array
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

        scrub_pattern = re.compile(r'[\[\]\'\"\$\#\@\!\{\}\,\|]')

        for field in entry_obj:
            value = str(entry_obj[field])

            value = re.sub(scrub_pattern, '', value)

            entry_obj[field] = value

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
        return self.entry_data[field_name] is not None

    def set(self, field_name, value):
        # TODO require field name to be present?
        self.entry_data[field_name] = value

    def _get_entry_data(self):
        return self.entry_data

    # def get_entry_date(self):
    #     pass
    #
    # def get_entry_time(self):
    #     pass

    def to_s(self):
        pprint.pprint(self.entry_data)
