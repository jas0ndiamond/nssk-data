import unittest
import csv
from pathlib import Path
import sys

path_root = Path(__file__).parents[1]
sys.path.append(str(path_root))

# depends on adding src to sys.path
from src.cosmo.CosmoDataEntry import CosmoDataEntry


# run with:
# ../venv/bin/python3 -m unittest CSVScrubTests.py
# ../venv/bin/python3 -m unittest

class CosmoTests(unittest.TestCase):
    def test_basic_read(self):
        test_file = "res/cosmo_test1.csv"

        with open(test_file, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',', strict=True)

            csv_row = next(reader)

        self.assertIsNotNone(csv_row)

        data_entry = CosmoDataEntry(csv_row)

        self.assertEqual("DFO PSEC Community Stream Monitoring (CoSMo)", data_entry.get("DatasetName"))


if __name__ == '__main__':
    unittest.main()
