#!/bin/bash

## logic would be to check for lib folder if not create first then run zip of mllib
curr_dir=${PWD}
echo "Starting building mllib...."

if [ ! -d "${curr_dir}/projlib" ]
then
  echo "Making directory ${curr_dir}/projlib"
  mkdir -p "${curr_dir}/projlib"
fi

zip -r ${curr_dir}/projlib/mllib.zip ./mllib
echo "Build success"
echo ".........................."