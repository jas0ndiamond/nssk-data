import unittest
from pathlib import Path
import sys

path_root = Path(__file__).parents[1]
sys.path.append(str(path_root))

# depends on adding src to sys.path
from src.importer.DBConfigFactory import DBConfigFactory
from src.importer.DBConfig import DBConfig


# run with:
# ../venv/bin/python3 -m unittest test_dbconfigfactory.py
# ../venv/bin/python3 -m unittest

class DBConfigFactoryTests(unittest.TestCase):
    def test_read(self):
        test_file = "res/db_test_config.json"

        config = DBConfigFactory.build(test_file)

        self.assertEqual(config[DBConfig.CONFIG_HOST], "myhost")
        self.assertEqual(config[DBConfig.CONFIG_PORT], int("10101"))
        self.assertEqual(config[DBConfig.CONFIG_USER], "myuser")
        self.assertEqual(config[DBConfig.CONFIG_PASS], "mypass")
        self.assertEqual(config[DBConfig.CONFIG_DBASE], "my_database")

    def test_clear_password(self):

        # more of a sanity check than a test
        test_file = "res/db_test_config.json"

        config = DBConfigFactory.build(test_file)

        config[DBConfig.CONFIG_PASS] = None

        self.assertEqual(config[DBConfig.CONFIG_PASS], None)


if __name__ == '__main__':
    unittest.main()
