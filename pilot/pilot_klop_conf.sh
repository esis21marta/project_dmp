#!/bin/bash

newpath=$(cat pilot/pilot_ext.conf | grep hdfs_base_path | awk '{print $3}' | sed 's/\//\\\//g')
search1="hdfs_base_path:\s(.*)(#.*)"
replace1="hdfs_base_path: ${newpath} \2"

## Change basepath for klop scoring parameters
sed -i -E "s/${search1}/${replace1}/g" conf/external/scoring/klop/default_probability/parameters.yml
sed -i -E "s/${search1}/${replace1}/g" conf/external/scoring/klop/income_prediction/parameters.yml
sed -i -E "s/${search1}/${replace1}/g" conf/external/scoring/klop/loan_propensity/parameters.yml

## Change hardcoded filepath catalog with_score 
search3="with_score:\n(.*)parquet(.*)\n(.*)filepath:\shdfs:\/\/\/data\/landing\/gx_pnt\/(.*)"
replace3="with_score:\n\1parquet\2\n\3filepath: \4"

perl -0777 -i -pe "s/${search3}/${replace3}/smg" conf/external/scoring/klop/default_probability/catalog.yml
perl -0777 -i -pe "s/${search3}/${replace3}/smg" conf/external/scoring/klop/income_prediction/catalog.yml
perl -0777 -i -pe "s/${search3}/${replace3}/smg" conf/external/scoring/klop/loan_propensity/catalog.yml


## Change output_feature from file 
sed -i '/output_features/,/^$/d' conf/dmp/scoring/parameters_feature.yml
cat pilot/klop_feature_list.txt >> conf/dmp/scoring/parameters_feature.yml