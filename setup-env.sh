#!/bin/bash

# cleanly installs a venv for the project

# check for python3 on the path
which python3 > /dev/null
RESULT=$?

echo -n "Checking for system python3..."
if [ $RESULT -eq 0 ]; then
  echo "Found python3"
else
  echo "Could not find python3 on PATH. Please install python3. Exiting..."
  exit 1
fi

# install in project root- same as this script
TARGET_DIR="$(dirname "$(readlink -f "$0")")"

# hardcoded target dir. manually wipe to reset
VENV_DIR="$TARGET_DIR/venv"
PYTHON_BIN="$VENV_DIR/bin/python3"

if [ -d "$VENV_DIR" ]; then
  echo "Target venv directory already exists at $VENV_DIR. Remove manually if necessary."
  exit 1
fi

# install the venv using system python3
echo "Installing venv"
python3 -m venv "$VENV_DIR"
INSTALL_RESULT=$?

# check if the venv install was successful
if [ $INSTALL_RESULT -ne 0 ]; then
  echo "VENV installation failed. Unexpected return value $INSTALL_RESULT"
  exit 1
elif [ ! -f "$PYTHON_BIN" ]; then
  echo "VENV installation failed. Could not find python binary at $PYTHON_BIN"
  exit 1
else
  echo "Initial venv setup successful"
fi

# upgrade pip to latest
echo "=========================================="
echo "Upgrading pip to latest"
eval "$PYTHON_BIN -m pip install --upgrade pip"

# install latest wheel
echo "=========================================="
echo "Installing latest wheel"
eval "$PYTHON_BIN -m pip install wheel"

# install project dependencies
echo "=========================================="
echo "Installing python dependencies"
eval "$PYTHON_BIN -m pip install -r requirements.txt"

echo "=========================================="
echo "Python venv setup complete. Exiting."
