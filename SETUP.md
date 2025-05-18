## Setup

1. Ensure python3 is installed and available on the PATH
2. Run the environment setup script `./setup-env.sh`

## Datastream API

### Environment setup
__SKIP THIS SECTION unless you're specifically planning to use the Datastream API__ 

* API requires Python 3.9 or greater for datastream.py
* Install python3.11 or later through your package manager
* Alternatively build, compile, install, and setup Python 3.11.7 with pip:
    ```
    git clone -b [version-tag] https://github.com/python/cpython.git
    cd cpython
    ./configure --enable-optimizations --with-ensurepip=install --prefix /home/$(whoami)/Python-3.11.7`
    make -j6
    make install
    /home/$(whoami)/Python-3.11.7/bin/python3 -m pip install --upgrade pip
    /home/$(whoami)/Python-3.11.7/bin/python3 -m pip install wheel
    ```

* Install datastream-py

    `venv/bin/python3 -m pip install git+https://github.com/datastreamapp/datastream-py`

### Obtain DataSets and Resources 

* Obtain DOI for your datasets
* Obtain API Key from the datastreamapp project (https://github.com/datastreamapp/api-docs)
* Follow examples from the datastreamapp project.
---
## Create and Configure Database

Create database container to manage data from NSSK sources.

[Database Setup](https://github.com/jas0ndiamond/nssk-database)

---
## Run imports
* [CoSMo](src/cosmo/README.md)
* [CoSMo Solist](src/cosmo-solinst/README.md)
* [DNV Flowworks](src/dnv_flowworks/README.md)
* [CNV Flowworks](src/cnv_flowworks/README.md)
* [CNV Hydrometric](src/cnv_hydrometric/README.md)
* [Rainfall Events](src/rainfall-events/README.md)
* [Rainfall Event Data](src/rainfall-event-data/README.md)
* [Waterrangers](src/waterrangers/README.md)
* [Conductivity Rainfall Correlation](src/conductivity-rainfall-correlation/README.md)

---
## Unit Tests

```
cd ./test/

# Run test suite by name
venv/bin/python3 -m unittest test_cnv_flowworks_dataentry.py
venv/bin/python3 -m unittest test_cosmo_dataentry.py
```
