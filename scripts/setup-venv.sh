#!/bin/bash -u
set -e

python3 -m venv catenv
script_dir=$(dirname "$0")
source $script_dir/../catenv/bin/activate
pip install --upgrade pip
pip install -r "$script_dir/requirements.txt"

echo "Run 'source catenv/bin/activate' to activate the virtual environment"
