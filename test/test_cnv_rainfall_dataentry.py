import unittest
import csv
from pathlib import Path
import sys
from io import StringIO

path_root = Path(__file__).parents[1]
sys.path.append(str(path_root))

# depends on adding src to sys.path
from src.cnv.CNVRainfallDataEntry import CNVRainfallDataEntry


# run with:
# ../venv/bin/python3 -m unittest test_cnv_rainfall_dataentry.py
# ../venv/bin/python3 -m unittest

class CNVRainfallTests(unittest.TestCase):
    def test_basic_read(self):
        test_file = "res/cnv_rainfall_test1.csv"

        with open(test_file, newline='', encoding='utf-8') as csvfile:
            # data dump file has two metadata lines above the schema
            next(csvfile)
            next(csvfile)

            reader = csv.DictReader(csvfile, delimiter=',', strict=True)

            csv_row = next(reader)

        self.assertIsNotNone(csv_row)

        data_entry = CNVRainfallDataEntry(csv_row)

        self.assertEqual("2022-02-27 00:00:00", data_entry.get("yyyy/MM/dd HH:mm:ss"))

    def test_bad_rainfall(self):
        test_file = "res/cnv_rainfall_test_bad_rainfall.csv"

        with open(test_file, newline='', encoding='utf-8') as csvfile:
            # data dump file has two metadata lines above the schema
            next(csvfile)
            next(csvfile)

            reader = csv.DictReader(csvfile, delimiter=',', strict=True)

            csv_row = next(reader)

        self.assertIsNotNone(csv_row)

        data_entry = CNVRainfallDataEntry(csv_row)

        self.assertEqual("2022-02-27 00:00:00", data_entry.get("yyyy/MM/dd HH:mm:ss"))


if __name__ == '__main__':
    unittest.main()
