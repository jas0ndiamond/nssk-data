#!/bin/bash

if [ -z $1 ]; then
  echo "Usage: run.sh confFile"
  exit 1
fi

CONF_FILE=$1

if [ ! -f "$CONF_FILE" ]; then
	echo "Could not resolve conf file $CONF_FILE"
	exit 1
fi

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"

PYTHON_BIN="$SCRIPT_DIR/../../venv/bin/python"

if [ ! -f "$PYTHON_BIN" ]; then
	echo "Could not locate python bin $PYTHON_BIN. Check that the venv was installed with the setup-env.sh script"
	exit 1
else
	eval "$PYTHON_BIN $SCRIPT_DIR/generate_dist.py -cfg $CONF_FILE"
fi
