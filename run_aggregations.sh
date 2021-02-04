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

#!/bin/bash bash
set -eux

display_help() {
    echo "Usage: $0 [option...]" >&2
    echo
    echo "   -p, --pipeline          [ARRAY] Name of the aggregation pipeline options [ALL | CUSTOMER_LOS | CUSTOMER_PROFILE | HANDSET  | HANDSET_INTERNAL | INTERNET_APP_USAGE | INTERNET_USAGE | NETWORK | PAYU_USAGE | PRODUCT | RECHARGE | REVENUE | SMS | VOICE_CALLS ]"
    echo "                           To run all the pipelines pass ALL"
    echo "   -h, --help              Show Help"
    echo "Examples:"
    echo "$0 -p ALL"
    echo "$0 -p HANDSET"
    echo "$0 -p HANDSET,REVENUE,SMS"
    echo
    exit 0
}


case "$1"
in
    -p|--pipeline)
        PIPELINE=$2
        PIPELINES=( "${PIPELINE//,/ }" )
        shift
        ;;
    -h|--help)
        display_help
        exit 0
        ;;
    *)
        echo "Error: Unknown option: $1" >&2
        exit 1
        ;;
esac


echo "###################################################################################"
echo "############################# Weekly Aggregation Data #############################"
echo "###################################################################################"


if [[ ${PIPELINES[*]} == "ALL" ]]
then
    # Run Kedro for the Aggregation of all
    echo "Running Kedro Pipeline for the Aggregation of All Domains"
    # kedro run --env=dmp/aggregation --tag=de_aggregation_pipeline
    kedro run --env=dmp/aggregation --tag=agg_customer_profile,agg_handset,agg_handset_internal,agg_internet_app_usage,agg_internet_usage,agg_network,agg_payu_usage,agg_product,agg_recharge,agg_revenue,agg_text_messaging,agg_voice_calls
elif [[ (${PIPELINES[*]} == *"ALL "*) || (${PIPELINES[*]} == *" ALL"*) ]]; then
    echo "Conflicting Inputs, Pipelines to run ${PIPELINES[*]}"
    exit 1
else
    if [[ ${PIPELINES[*]} == *"CUSTOMER_LOS"* ]]
    then
        # Run Kedro for the Aggregation of Customer Profile
        echo "Running Kedro Pipeline for the Aggregation of Customer LOS Domain"
        kedro run --env=dmp/aggregation --tag=agg_customer_los
    fi
    if [[ ${PIPELINES[*]} == *"CUSTOMER_PROFILE"* ]]
    then
        # Run Kedro for the Aggregation of Customer Profile
        echo "Running Kedro Pipeline for the Aggregation of Customer Profile Domain"
        kedro run --env=dmp/aggregation --tag=agg_customer_profile
    fi
    if [[ ${PIPELINES[*]} == *"HANDSET"* ]]
    then
        # Run Kedro for the Aggregation of Handset
        echo "Running Kedro Pipeline for the Aggregation of Handset Domain"
        kedro run --env=dmp/aggregation --tag=agg_handset
    fi
    if [[ ${PIPELINES[*]} == *"HANDSET_INTERNAL"* ]]
    then
        # Run Kedro for the Aggregation of Handset
        echo "Running Kedro Pipeline for the Aggregation of Handset Internal Domain"
        kedro run --env=dmp/aggregation --tag=agg_handset_internal
    fi
    if [[ ${PIPELINES[*]} == *"INTERNET_APP_USAGE"* ]]
    then
        # Run Kedro for the Aggregation of Internet App Usage
        echo "Running Kedro Pipeline for the Aggregation of Internet App Usage Domain"
        kedro run --env=dmp/aggregation --tag=agg_internet_app_usage
    fi
    if [[ ${PIPELINES[*]} == *"INTERNET_USAGE"* ]]
    then
        # Run Kedro for the Aggregation of Internet Usage
        echo "Running Kedro Pipeline for the Aggregation of Internet Usage Domain"
        kedro run --env=dmp/aggregation --tag=agg_internet_usage
    fi
    if [[ ${PIPELINES[*]} == *"NETWORK"* ]]
    then
        # Run Kedro for the Aggregation of Internet Usage
        echo "Running Kedro Pipeline for the Aggregation of Network Domain"
        kedro run --env=dmp/aggregation --tag=agg_network
    fi
    if [[ ${PIPELINES[*]} == *"PAYU_USAGE"* ]]
    then
        # Run Kedro for the Aggregation of PAYU Usage
        echo "Running Kedro Pipeline for the Aggregation of PayU Usage Domain"
        kedro run --env=dmp/aggregation --tag=agg_payu_usage
    fi
    if [[ ${PIPELINES[*]} == *"PRODUCT"* ]]
    then
        # Run Kedro for the Aggregation of Internet Usage
        echo "Running Kedro Pipeline for the Aggregation of Product Domain"
        kedro run --env=dmp/aggregation --tag=agg_product
    fi
    if [[ ${PIPELINES[*]} == *"RECHARGE"* ]]
    then
        # Run Kedro for the Aggregation of Recharge
        echo "Running Kedro Pipeline for the Aggregation of Recharge Domain"
        kedro run --env=dmp/aggregation --tag=agg_recharge
    fi
    if [[ ${PIPELINES[*]} == *"REVENUE"* ]]
    then
        # Run Kedro for the Aggregation of Revenue
        echo "Running Kedro Pipeline for the Aggregation of Revenue Domain"
        kedro run --env=dmp/aggregation --tag=agg_revenue
    fi
    if [[ ${PIPELINES[*]} == *"SMS"* ]]
    then
        # Run Kedro for the Aggregation of SMS
        echo "Running Kedro Pipeline for the Aggregation of SMS Domain"
        kedro run --env=dmp/aggregation --tag=agg_text_messaging
    fi
    if [[ ${PIPELINES[*]} == *"VOICE_CALLS"* ]]
    then
        # Run Kedro for the Aggregation of Voice Calls
        echo "Running Kedro Pipeline for the Aggregation of Voice Calls Domain"
        kedro run --env=dmp/aggregation --tag=agg_voice_calls
    fi
fi


echo "###################################################################################"
echo "########################## Weekly Aggregation COMPLETED ###########################"
echo "###################################################################################"
