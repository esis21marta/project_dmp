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

. /home/dmpstage01/.bashrc

EDGE_NODE_PATH=${1:-/data/gx_pnt/dmp_project/dmp/stage}
BRANCH_NAME=${2:-no_branch}

mkdir -p $EDGE_NODE_PATH/project_dmp
cd $EDGE_NODE_PATH

if [ ! -d "project_dmp/.git" ]
then
    git clone https://cicd-gitlab.telkomsel.co.id/19334796/project_dmp.git project_dmp
fi

cd project_dmp
git clean -fd
git checkout develop

# Fetch Release Branch from release.conf in case release branch name was not passed
if [ ${BRANCH_NAME} == "no_branch" ]
then
    . releases/release.conf
    BRANCH_NAME=$release_branch
fi

git fetch origin $BRANCH_NAME:$BRANCH_NAME
git checkout $BRANCH_NAME
git pull origin $BRANCH_NAME
