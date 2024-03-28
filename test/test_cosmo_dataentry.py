import unittest
import csv
from pathlib import Path
import sys
from io import StringIO

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

    def test_validity_conductivity(self):
        # row 5 has bad entry - negative
        test_file = "res/cosmo_test_bad_conductivity1.csv"

        with open(test_file, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',', strict=True)

            # row 1 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

            # row 2 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

            # row 3 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

            # row 4 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

            # row 5 negative conductivity
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)

            try:
                data_entry = None
                data_entry = CosmoDataEntry(csv_row)
                self.fail("Expected exception thrown parsing negative conductivity")
            except Exception as e:
                # assert the assignment failed
                self.assertIsNone(data_entry)

            # row 6 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

    def test_validity_conductivity2(self):
        # row 5 has bad entry - missing
        test_file = "res/cosmo_test_bad_conductivity2.csv"

        with open(test_file, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',', strict=True)

            # row 1 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

            # row 2 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

            # row 3 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

            # row 4 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

            # row 5 negative conductivity
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)

            try:
                data_entry = None
                data_entry = CosmoDataEntry(csv_row)
                self.fail("Expected exception thrown parsing negative conductivity")
            except Exception as e:
                # assert the assignment failed
                self.assertIsNone(data_entry)

            # row 6 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

    def test_validity_conductivity3(self):
        # row 5 has bad entry - not a float
        test_file = "res/cosmo_test_bad_conductivity3.csv"

        with open(test_file, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',', strict=True)

            # row 1 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

            # row 2 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

            # row 3 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

            # row 4 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

            # row 5 negative conductivity
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)

            try:
                data_entry = None
                data_entry = CosmoDataEntry(csv_row)
                self.fail("Expected exception thrown parsing negative conductivity")
            except Exception as e:
                # assert the assignment failed
                self.assertIsNone(data_entry)

            # row 6 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

    def test_validity_spec_conductance(self):
        # row 4 has bad entry
        test_file = "res/cosmo_test_bad_spec_conductance.csv"

        with open(test_file, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',', strict=True)

            # row 1 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

            # row 2 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

            # row 3 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)

            # row 4 negative spec conductance
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)

            try:
                data_entry = None
                data_entry = CosmoDataEntry(csv_row)
                self.fail("Expected exception thrown parsing negative specific conductance")
            except Exception as e:
                # assert the assignment failed
                self.assertIsNone(data_entry)

            # row 5 okay
            csv_row = next(reader)

            self.assertIsNotNone(csv_row)
            data_entry = None
            data_entry = CosmoDataEntry(csv_row)

            # value stored as string
            self.assertTrue(float(data_entry.get("ResultValue")) > 0)


if __name__ == '__main__':
    unittest.main()
