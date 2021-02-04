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
set -eu

display_help() {
    echo "Usage: $0 [option...]" >&2
    echo
    echo "   -p, --pipeline          [ARRAY] Name of the scoring pipeline options [ALL | CREATE_SCAFFOLD | CUSTOMER_LOS | CUSTOMER_PROFILE | HANDSET  | HANDSET_INTERNAL | INTERNET_APP_USAGE | INTERNET_USAGE | NETWORK | PAYU_USAGE | PRODUCT | RECHARGE | REVENUE | SMS | VOICE_CALLS]"
    echo "                           To run all the pipelines pass ALL"
    echo "   -s, --step              Step to run options [ALL | PRIMARY | FEATURE | MASTER]"
    echo "                           To run all the steps pass ALL"
    echo "   -h, --help              Show Help"
    echo "Examples:"
    echo "$0 -p ALL -s ALL"
    echo "$0 -p HANDSET -s ALL"
    echo "$0 -p HANDSET,REVENUE,SMS -s FEATURES,MASTER"
    echo
    exit 0
}

while getopts ":p:s:h" opt
do
    case ${opt} in
        p)
            PIPELINE=$2
            PIPELINES=( "${PIPELINE//,/ }" )
            ;;
        s)
            STEP=$2
            STEPS=( "${STEP//,/ }" )
            ;;
        h)
            display_help
            exit 0
            ;;
        *)
            echo "Error: Unknown option: $1" >&2
            exit 1
            ;;
    esac
done


if [[ (${STEPS[*]} == "ALL") || (${STEPS[*]} == *"PRIMARY"*) ]]
then

    echo "###################################################################################"
    echo "################################ PRIMARY PIPELINE #################################"
    echo "###################################################################################"

    if [[ ${PIPELINES[*]} == "ALL" ]]
    then
        # Run Kedro for the Primary of all
        echo "Running Kedro Pipeline for the primary of All Domains"
        kedro run --env=dmp/scoring --tag=de_primary_pipeline
    elif [[ (${PIPELINES[*]} == *"ALL "*) || (${PIPELINES[*]} == *" ALL"*) ]]; then
        echo "Conflicting Inputs, Pipelines to run ${PIPELINES[*]}"
        exit 1
    else
        if [[ ${PIPELINES[*]} == *"CREATE_SCAFFOLD"* ]]
        then
            # Run Kedro for the Primary of Scaffold
            echo "Running Kedro Pipeline for the Primary of Scaffold"
            kedro run --env=dmp/scoring --tag=prm_scaffold
        fi
        if [[ ${PIPELINES[*]} == *"CUSTOMER_LOS"* ]]
        then
            # Run Kedro for the Primary of Handset
            echo "Running Kedro Pipeline for the Primary of Customer LOS Domain"
            kedro run --env=dmp/scoring --tag=prm_customer_los
        fi
        if [[ ${PIPELINES[*]} == *"CUSTOMER_PROFILE"* ]]
        then
            # Run Kedro for the Primary of Customer Profile
            echo "Running Kedro Pipeline for the Primary of Customer Profile Domain"
            kedro run --env=dmp/scoring --tag=prm_customer_profile
        fi
        if [[ ${PIPELINES[*]} == *"HANDSET"* ]]
        then
            # Run Kedro for the Primary of Handset
            echo "Running Kedro Pipeline for the Primary of Handset Domain"
            kedro run --env=dmp/scoring --tag=prm_handset
        fi
        if [[ ${PIPELINES[*]} == *"HANDSET_INTERNAL"* ]]
        then
            # Run Kedro for the Primary of Handset
            echo "Running Kedro Pipeline for the Primary of Handset Internal Domain"
            kedro run --env=dmp/scoring --tag=prm_handset_internal
        fi
        if [[ ${PIPELINES[*]} == *"INTERNET_APP_USAGE"* ]]
        then
            # Run Kedro for the Primary of Internet App Usage
            echo "Running Kedro Pipeline for the Primary of Internet App Usage Domain"
            kedro run --env=dmp/scoring --tag=prm_internet_app_usage
        fi
        if [[ ${PIPELINES[*]} == *"INTERNET_USAGE"* ]]
        then
            # Run Kedro for the Primary of Internet Usage
            echo "Running Kedro Pipeline for the Primary of Internet Usage Domain"
            kedro run --env=dmp/scoring --tag=prm_internet_usage
        fi
        if [[ ${PIPELINES[*]} == *"NETWORK"* ]]
        then
            # Run Kedro for the Primary of PAYU Usage
            echo "Running Kedro Pipeline for the Primary of Network Domain"
            kedro run --env=dmp/scoring --tag=prm_network
        fi
        if [[ ${PIPELINES[*]} == *"PAYU_USAGE"* ]]
        then
            # Run Kedro for the Primary of PAYU Usage
            echo "Running Kedro Pipeline for the Primary of PayU Usage Domain"
            kedro run --env=dmp/scoring --tag=prm_payu_usage
        fi
        if [[ ${PIPELINES[*]} == *"PRODUCT"* ]]
        then
            # Run Kedro for the Primary of PAYU Usage
            echo "Running Kedro Pipeline for the Primary of Product Domain"
            kedro run --env=dmp/scoring --tag=prm_product
        fi
        if [[ ${PIPELINES[*]} == *"RECHARGE"* ]]
        then
            # Run Kedro for the Primary of Recharge
            echo "Running Kedro Pipeline for the Primary of Recharge Domain"
            kedro run --env=dmp/scoring --tag=prm_recharge
        fi
        if [[ ${PIPELINES[*]} == *"REVENUE"* ]]
        then
            # Run Kedro for the Primary of Revenue
            echo "Running Kedro Pipeline for the Primary of Revenue Domain"
            kedro run --env=dmp/scoring --tag=prm_revenue
        fi
        if [[ ${PIPELINES[*]} == *"SMS"* ]]
        then
            # Run Kedro for the Primary of SMS
            echo "Running Kedro Pipeline for the Primary of SMS Domain"
            kedro run --env=dmp/scoring --tag=prm_text_messaging
        fi
        if [[ ${PIPELINES[*]} == *"VOICE_CALLS"* ]]
        then
            # Run Kedro for the Primary of Voice Calls
            echo "Running Kedro Pipeline for the Primary of Voice Calls Domain"
            kedro run --env=dmp/scoring --tag=prm_voice_calls
        fi
    fi
    echo "###################################################################################"
    echo "########################### PRIMARY PIPELINE COMPLETED ############################"
    echo "###################################################################################"

fi


if [[ (${STEPS[*]} == "ALL") || (${STEPS[*]} == *"FEATURE"*) ]]
then

    echo "###################################################################################"
    echo "################################ FEATURES PIPELINE ################################"
    echo "###################################################################################"

    if [[ ${PIPELINES[*]} == "ALL" ]]
    then
        # Run Kedro for the features of all
        echo "Running Kedro Pipeline for the features of All Domains"
        kedro run --env=dmp/scoring --tag=de_feature_pipeline
    elif [[ (${PIPELINES[*]} == *"ALL "*) || (${PIPELINES[*]} == *" ALL"*) ]]; then
        echo "Conflicting Inputs, Pipelines to run ${PIPELINES[*]}"
        exit 1
    else
        if [[ ${PIPELINES[*]} == *"CUSTOMER_LOS"* ]]
        then
            # Run Kedro for the features of Customer Profile
            echo "Running Kedro Pipeline for the features of Customer LOS Domain"
            kedro run --env=dmp/scoring --tag=fea_customer_los
        fi
        if [[ ${PIPELINES[*]} == *"CUSTOMER_PROFILE"* ]]
        then
            # Run Kedro for the features of Customer Profile
            echo "Running Kedro Pipeline for the features of Customer Profile Domain"
            kedro run --env=dmp/scoring --tag=fea_customer_profile
        fi
        if [[ ${PIPELINES[*]} == *"HANDSET"* ]]
        then
            # Run Kedro for the features of Handset
            echo "Running Kedro Pipeline for the features of Handset Domain"
            kedro run --env=dmp/scoring --tag=fea_handset
        fi
        if [[ ${PIPELINES[*]} == *"HANDSET_INTERNAL"* ]]
        then
            # Run Kedro for the features of Handset Internal
            echo "Running Kedro Pipeline for the features of Handset Internal Domain"
            kedro run --env=dmp/scoring --tag=fea_handset_internal
        fi
        if [[ ${PIPELINES[*]} == *"INTERNET_APP_USAGE"* ]]
        then
            # Run Kedro for the features of Internet App Usage
            echo "Running Kedro Pipeline for the features of Internet App Usage Domain"
            kedro run --env=dmp/scoring --tag=fea_internet_app_usage
        fi
        if [[ ${PIPELINES[*]} == *"INTERNET_USAGE"* ]]
        then
            # Run Kedro for the features of Internet Usage
            echo "Running Kedro Pipeline for the features of Internet Usage Domain"
            kedro run --env=dmp/scoring --tag=fea_internet_usage
        fi
        if [[ ${PIPELINES[*]} == *"RECHARGE"* ]]
        then
            # Run Kedro for the features of Recharge
            echo "Running Kedro Pipeline for the features of Recharge Domain"
            kedro run --env=dmp/scoring --tag=fea_recharge
        fi
        if [[ ${PIPELINES[*]} == *"NETWORK"* ]]
        then
            # Run Kedro for the features of PAYU Usage
            echo "Running Kedro Pipeline for the features of Network Domain"
            kedro run --env=dmp/scoring --tag=fea_network
        fi
        if [[ ${PIPELINES[*]} == *"PAYU_USAGE"* ]]
        then
            # Run Kedro for the features of PAYU Usage
            echo "Running Kedro Pipeline for the features of PayU Usage Domain"
            kedro run --env=dmp/scoring --tag=fea_payu_usage
        fi
        if [[ ${PIPELINES[*]} == *"PRODUCT"* ]]
        then
            # Run Kedro for the features of PAYU Usage
            echo "Running Kedro Pipeline for the features of Product Domain"
            kedro run --env=dmp/scoring --tag=fea_product
        fi
        if [[ ${PIPELINES[*]} == *"REVENUE"* ]]
        then
            # Run Kedro for the features of Revenue
            echo "Running Kedro Pipeline for the features of Revenue Domain"
            kedro run --env=dmp/scoring --tag=fea_revenue
        fi
        if [[ ${PIPELINES[*]} == *"SMS"* ]]
        then
            # Run Kedro for the features of SMS
            echo "Running Kedro Pipeline for the features of SMS Domain"
            kedro run --env=dmp/scoring --tag=fea_text_messaging
        fi
        if [[ ${PIPELINES[*]} == *"VOICE_CALLS"* ]]
        then
            # Run Kedro for the features of Voice Calls
            echo "Running Kedro Pipeline for the features of Voice Calls Domain"
            kedro run --env=dmp/scoring --tag=fea_voice_calls
        fi
    fi
    echo "###################################################################################"
    echo "########################### FEATURE PIPELINE COMPLETED ############################"
    echo "###################################################################################"
fi


if [[ (${STEPS[*]} == "ALL") || (${STEPS[*]} == *"MASTER"*) ]]
then

    echo "###################################################################################"
    echo "################################# MASTER PIPELINE #################################"
    echo "###################################################################################"

    # Run Kedro for the master of all domain
    echo "Running Kedro Pipeline for the master of All Domains"
    kedro run --env=dmp/scoring --tag=de_master_pipeline

    echo "###################################################################################"
    echo "############################ MASTER PIPELINE COMPLETED ############################"
    echo "###################################################################################"
fi
