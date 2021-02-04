#!/bin/bash

tgl=`date '+%Y%m%d'`

branch_name=${1}
[ -z "$branch_name" ] && branch_name=$(cat pilot/pilot_ext.conf | grep branch_name | awk '{print $3}')


## Git command is enabled when the script is in project_dir
git stash
git checkout develop
git pull origin develop
git checkout -b ${branch_name}

## changed mck_dmp_pipeline/01_agregation into full path hdfs:///data/landing/gx_pnt/mck_dmp_pipeline/01_agregation
agg_base_path=$(cat pilot/pilot_ext.conf | grep agg_base_path | awk '{print $3}' | sed 's/\//\\\//g')
sed -i -E "s/filepath:\s(mck_dmp_pipeline)\/01_aggregation/filepath: ${agg_base_path}\1\/01_aggregation/g" conf/dmp/scoring/catalog.yml

## start modify catalog.yml
## changed l6_master_hive database into dmp_staging and tables into master_dmp_pilot_ext
db=$(cat pilot/pilot_ext.conf | grep master_hive_db | awk '{print $3}')
table=$(cat pilot/pilot_ext.conf | grep master_hive_table | awk '{print $3}')

search0="l6_master_hive:\n(.*)hive\n(.*)database:\s(.*)\n(.*)table:\s(.*)"
replace0="l6_master_hive:\n\1hive\n\2database: ${db}\n\4table: ${table}"

sed -i -E "/./{H;$!d} ; x ; s/${search0}/${replace0}/g" conf/dmp/scoring/catalog.yml

## change hdfs_base_path in parameters.yml
newpath=$(cat pilot/pilot_ext.conf | grep hdfs_base_path | awk '{print $3}' | sed 's/\//\\\//g')
search1="hdfs_base_path:\s(.*)(#.*)"
replace1="hdfs_base_path: ${newpath} \2"
sed -i -E "s/${search1}/${replace1}/g" conf/dmp/scoring/parameters.yml

## Copy parameters packing unpacking from training
cp conf/dmp/training/parameters_packing_unpacking.yml conf/dmp/scoring/

## Change partner_msisdns_old:
search2="partner_msisdns_old:\n(.*)parquet\n(.*)filepath:\s(.*)\n(.*)partitions:\s(.*)"
replace2="partner_msisdns_old:\n\1parquet\n\2filepath: hdfs:\/\/\/data\/landing\/gx_pnt\/\3\n\4partitions: \5"
sed -i -E "/./{H;$!d} ; x ; s/${search2}/${replace2}/g" conf/dmp/scoring/catalog.yml

## Change feature_mode to feature_list in parameters_feature
sed -i -E "s/^features_mode:\s(.*)(#.*)/features_mode: feature_list \2/g" conf/dmp/scoring/parameters_feature.yml


## Change spark config to dmp_prod and up memory used
queuename=$(cat pilot/pilot_ext.conf | grep spark_queue | awk '{print $3}')
sed -i -E "s/spark.driver.maxResultSize:\s(.*)$/spark.driver.maxResultSize: 32g/g" conf/dmp/scoring/spark.yml
sed -i -E "s/spark.yarn.queue:\s(.*)$/spark.yarn.queue: ${queuename}/g" conf/dmp/scoring/spark.yml
sed -i -E "s/spark.executor.memory:\s(.*)$/spark.executor.memory: 32g\nspark.executor.memoryOverhead: 10g/g" conf/dmp/scoring/spark.yml
sed -i -E "s/spark.yarn.executor.memoryOverhead:\s(.*)$/spark.yarn.executor.memoryOverhead: 10g/g" conf/dmp/scoring/spark.yml