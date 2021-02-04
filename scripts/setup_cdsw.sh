#!/usr/bin/env bash
set -eux -o pipefail

python3 --version

pip3 install pip==20.0.2
pip3 --version

pip3 install pypandoc==1.4  # required due to resolution error
pip3 install -r src/requirements.txt
pip3 install -r src/requirements.txt  # run again in case of resolution issues
pip3 list

kedro install

echo done
