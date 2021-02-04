# Copyright 2018-present QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

. /home/dmpprod01/.bashrc

BRANCH=master
EDGE_NODE_PATH=/data/gx_pnt/dmp_project/dmp/prod_tests

./deploy_vm.sh $EDGE_NODE_PATH $BRANCH

cd $EDGE_NODE_PATH/project_dmp

. docker/files/docker_ver.conf
DOCKER_VERSION=$latest_tag

sudo docker run --rm -v $EDGE_NODE_PATH/project_dmp:/usr/src/app -v /etc/hosts:/etc/hosts -v /opt:/opt -v /etc/hadoop:/etc/hadoop -v /etc/spark2/conf.cloudera.spark2_on_yarn:/etc/spark2/conf.cloudera.spark2_on_yarn -v /opt/cloudera/parcels:/opt/cloudera/parcels -v /home/dmpprod01/keytab_dmpprod01:/root/keytab_dmpprod01 --env SPARK_HOME=/opt/cloudera/parcels/SPARK2-2.4.0.cloudera2-1.cdh5.13.3.p0.1041012/lib/spark2 --env SPARK_CONF_DIR=/etc/spark2/conf.cloudera.spark2_on_yarn --env PYSPARK_PYTHON=/opt/cloudera/parcels/Anaconda/bin/python3 --env PYSPARK_PYTHON3=/opt/cloudera/parcels/Anaconda/bin/python3 --env PAI_RUNS=hdfs:///data/landing/gx_pnt/mck_dmp_int/data_science/pai/test --env LD_LIBRARY_PATH=/root/lib/instantclient_19_6:/opt/cloudera/parcels/CDH-5.13.3-1.cdh5.13.3.p3182.3440/lib64/: --env HADOOP_CONF_DIR=/etc/spark2/conf.cloudera.spark2_on_yarn/yarn-conf --env HADOOP_HOME="/opt/cloudera/parcels/CDH/lib/hadoop" --net=host docker.cicd-jfrog.telkomsel.co.id/kedro:$DOCKER_VERSION /bin/bash -c 'kinit dmpprod01@TELKOMSEL.CO.ID -R -k -t /root/keytab_dmpprod01; kedro integration-test'
