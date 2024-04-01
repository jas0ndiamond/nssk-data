# nssk-data

Data Analysis Apps and Tools for North Shore Streamkeepers

---
## Requirements
* Python 3.9 or greater for datastream.py

---
## Setup
* Build, compile, install, and setup Python 3.11.7 with pip

```
./configure --enable-optimizations --with-ensurepip=install --prefix /home/$(whoami)/Python-3.11.7`
make -j6
make install
python3 -m pip install --upgrade pip
python3 -m pip install wheel
```

* Create venv for project

`python3 -m venv /path/to/new/virtual/environment`

*  Install required pip modules:

`python3 -m pip install requests mysql-connector-python ijson argparse`

* Install datastream-py

`venv/bin/python3 -m pip install git+https://github.com/datastreamapp/datastream-py`

---
## Obtain DataSets and Resources 

* Obtain DOI for your datasets
* Obtain API Key from the datastreamapp project (https://github.com/datastreamapp/api-docs)
* Follow examples from the datastreamapp project.
---
## Setup & Run imports

Create databases to manage imported data from NSSK sources.

[Setup](docker/README.md)

Sources:
* [CoSMo](src/cosmo/README.md)
* Flowworks
* CNV

---
## Unit Tests

```
cd ./test/

# Run all tests
../venv/bin/python3 -m unittest

# Run test suite by name
../venv/bin/python3 -m unittest test_cnv_rainfall_dataentry.py
../venv/bin/python3 -m unittest test_cosmo_dataentry.py
```

---
## Links
* https://nssk.ca
* https://datastream.org/
* https://github.com/datastreamapp/
* https://doi.org