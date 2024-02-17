# nssk-data

Data Analysis Apps and Tools for North Shore Streamkeepers

---
# Requirements
* Python 3.9 or greater for datastream.py

---
# Setup
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

`python3 -m pip install requests mysql-connector-python`

* Install datastream-py

`venv/bin/python -m pip install git+https://github.com/datastreamapp/datastream-py`

# Obtain DataSets and Resources 

* Obtain DOI for your datasets
* Obtain API Key from the datastreamapp project (https://github.com/datastreamapp/api-docs)
* Follow examples from the datastreamapp project.
---
# Run imports

CoSMo

Flowworks

---
# Links
* https://nssk.ca
* https://datastream.org/
* https://github.com/datastreamapp/
* https://doi.org