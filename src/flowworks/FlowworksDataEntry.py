from pathlib import Path
import sys
import logging

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

# depends on adding src to sys.path
from src.data.DataEntry import DataEntry


# "date"
# "value"


class FlowworksDataEntry(DataEntry):

    # row_obj is any structure that can be indexed and is iterable
    # csv, json, raw array
    def __init__(self, entry_obj):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        # raise exception if theres a problem
        super().__init__(entry_obj)

        # at this point we have a valid entry, but still want to clean it up
        # TODO: remove alphanumeric+ chars used in sql syntax [ ] { } | " ' ;

        # only one site: set in generate_db_setup.py
        self.site = "DNV"

        ##################
        # value buckets - some entries can be incomplete
        if self.get('value') == 'null':

            print ("Found a null %s" % self.entry_data)

            # self.set('value', None)
            del self.entry_data['value']

    def _is_valid(self, fields):
        # no narrowing of dataset done here
        # just check for data validity

        # max length on field data

        # for field in fields:
        #     if False:
        #         return False

        return True

    def get_db_destination(self):
        return self.site

    def get_entry_date(self):
        pass

    def get_entry_time(self):
        pass
