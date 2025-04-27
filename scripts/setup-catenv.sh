#!/bin/bash -u
set -e # capture errors and exit immediately, until we get to the end

root_dir=$(git rev-parse --show-toplevel 2>/dev/null)
cd "$root_dir"
python3 -m venv catenv

source "$root_dir/catenv/bin/activate"

pip install --upgrade pip
pip install -r "$root_dir/scripts/requirements.txt"

if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
    # Script is being sourced
    echo "Env should be activated"
else
    echo "Run 'source ${BASH_SOURCE[0]}' to activate the virtual environment"
fi

set +e # because if we source this script, we don't want to exit on error
