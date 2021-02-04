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

from typing import Dict

from kedro.pipeline import Pipeline

# Model factory
from src.dmp.pipelines.dmp import _06_qa as qa
from src.dmp.pipelines.dmp._00_preprocessing import availability as prep_dq_availability
from src.dmp.pipelines.dmp._00_preprocessing import (
    data_set_schema_checks as schema_checks,
)
from src.dmp.pipelines.dmp._00_preprocessing import (
    storage_checks as prep_storage_checks,
)
from src.dmp.pipelines.dmp._01_aggregation import airtime_loan as agg_airtime_loan
from src.dmp.pipelines.dmp._01_aggregation import customer_los as agg_customer_los
from src.dmp.pipelines.dmp._01_aggregation import (
    customer_profile as agg_customer_profile,
)
from src.dmp.pipelines.dmp._01_aggregation import handset as agg_handset
from src.dmp.pipelines.dmp._01_aggregation import (
    internet_app_usage as agg_internet_app_usage,
)
from src.dmp.pipelines.dmp._01_aggregation import internet_usage as agg_internet_usage
from src.dmp.pipelines.dmp._01_aggregation import mobility as agg_mobility
from src.dmp.pipelines.dmp._01_aggregation import mytsel as agg_mytsel
from src.dmp.pipelines.dmp._01_aggregation import network as agg_network
from src.dmp.pipelines.dmp._01_aggregation import outlets as agg_outlets
from src.dmp.pipelines.dmp._01_aggregation import payu_usage as agg_payu_usage
from src.dmp.pipelines.dmp._01_aggregation import product as agg_product
from src.dmp.pipelines.dmp._01_aggregation import recharge as agg_recharge
from src.dmp.pipelines.dmp._01_aggregation import revenue as agg_revenue
from src.dmp.pipelines.dmp._01_aggregation import text_messaging as agg_text_messaging
from src.dmp.pipelines.dmp._01_aggregation import voice_calling as agg_voice_calling
from src.dmp.pipelines.dmp._02_primary import airtime_loan as prm_airtime_loan
from src.dmp.pipelines.dmp._02_primary import customer_los as prm_customer_los
from src.dmp.pipelines.dmp._02_primary import customer_profile as prm_customer_profile
from src.dmp.pipelines.dmp._02_primary import handset as prm_handset
from src.dmp.pipelines.dmp._02_primary import (
    internet_app_usage as prm_internet_app_usage,
)
from src.dmp.pipelines.dmp._02_primary import internet_usage as prm_internet_usage
from src.dmp.pipelines.dmp._02_primary import mobility as prm_mobility
from src.dmp.pipelines.dmp._02_primary import msisdn_list
from src.dmp.pipelines.dmp._02_primary import mytsel as prm_mytsel
from src.dmp.pipelines.dmp._02_primary import network as prm_network
from src.dmp.pipelines.dmp._02_primary import payu_usage as prm_payu_usage
from src.dmp.pipelines.dmp._02_primary import product as prm_product
from src.dmp.pipelines.dmp._02_primary import recharge as prm_recharge
from src.dmp.pipelines.dmp._02_primary import revenue as prm_revenue
from src.dmp.pipelines.dmp._02_primary import scaffold
from src.dmp.pipelines.dmp._02_primary import text_messaging as prm_text_messaging
from src.dmp.pipelines.dmp._02_primary import voice_calling as prm_voice_calling
from src.dmp.pipelines.dmp._04_features import airtime_loan as fea_airtime_loan
from src.dmp.pipelines.dmp._04_features import customer_los as fea_customer_los
from src.dmp.pipelines.dmp._04_features import customer_profile as fea_customer_profile
from src.dmp.pipelines.dmp._04_features import handset as fea_handset
from src.dmp.pipelines.dmp._04_features import (
    internet_app_usage as fea_internet_app_usage,
)
from src.dmp.pipelines.dmp._04_features import internet_usage as fea_internet_usage
from src.dmp.pipelines.dmp._04_features import mobility as fea_mobility
from src.dmp.pipelines.dmp._04_features import mytsel as fea_mytsel
from src.dmp.pipelines.dmp._04_features import network as fea_network
from src.dmp.pipelines.dmp._04_features import outlets as fea_outlets
from src.dmp.pipelines.dmp._04_features import payu_usage as fea_payu_usage
from src.dmp.pipelines.dmp._04_features import product as fea_product
from src.dmp.pipelines.dmp._04_features import recharge as fea_recharge
from src.dmp.pipelines.dmp._04_features import revenue as fea_revenue
from src.dmp.pipelines.dmp._04_features import text_messaging as fea_text_messaging
from src.dmp.pipelines.dmp._04_features import voice_calling as fea_voice_calling
from src.dmp.pipelines.dmp._05_model_input import pipeline as model_input
from src.dmp.pipelines.dmp._06_qa import reseller as reseller_custom_qa
from src.dmp.pipelines.dmp._06_qa import timeliness as qa_timeliness
from src.dmp.pipelines.dmp._07_data_dictionary import pipeline as data_dictionary

# Clustering
from src.dmp.pipelines.internal_esis._4g_stimulation import (
    _0_preprocessing,
    _1_psi_check,
    _2_dim_reduction_and_clustering,
)
from src.dmp.pipelines.internal_esis._ses_stimulation import (
    _1_rfm_segmentation,
    _2_sub_segmentation_clustering,
)


def create_pipelines(**kwargs) -> Dict[str, Pipeline]:
    """Create the project's pipeline.

    Args:
        kwargs: Ignore any additional arguments added in the future.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.

    """
    ####################### Clustering ####################################
    ### 4G ###
    fg_sementing_pipeline = _0_preprocessing.create_pipeline()
    fg_psi_pipeline = _1_psi_check.create_pipeline()
    fg_clustering_pipeline = _2_dim_reduction_and_clustering.create_pipeline()
    fg_clustering_pipeline_scoring = (
        _2_dim_reduction_and_clustering.create_pipeline_scoring()
    )
    # fg_pipeline_all = fg_sementing_pipeline + fg_psi_pipeline + fg_clustering_pipeline + fg_clustering_pipeline_scoring
    ### SES ###
    ses_sementing_pipeline = _1_rfm_segmentation.create_pipeline()
    # ses_psi_pipeline = _1_psi_check.create_pipeline()
    ses_clustering_pipeline = _2_sub_segmentation_clustering.create_pipeline_training()
    ses_clustering_pipeline_scoring = (
        _2_sub_segmentation_clustering.create_pipeline_scoring()
    )
    # fg_pipeline_all = fg_sementing_pipeline + fg_psi_pipeline + fg_clustering_pipeline + fg_clustering_pipeline_scoring
    #######################################################################

    # Scaffold setup (returns msisdn + weekstart) for domains we provide
    scaffold_pipeline = scaffold.create_pipeline()

    # Preprocessing layer
    prep_schema_checks_pipeline = schema_checks.create_pipeline()
    prep_storage_checks_pipeline = prep_storage_checks.create_pipeline()
    prep_dq_availability_pipeline = prep_dq_availability.create_pipeline()

    # Aggregation layer - weekly/monthly aggregations
    agg_airtime_loan_pipeline = agg_airtime_loan.create_pipeline()
    agg_internet_app_usage_pipeline = agg_internet_app_usage.create_pipeline()
    agg_handset_pipeline = agg_handset.create_pipeline()
    agg_internet_usage_pipeline = agg_internet_usage.create_pipeline()
    agg_product_pipeline = agg_product.create_pipeline()
    agg_recharge_pipeline = agg_recharge.create_pipeline()
    agg_revenue_pipeline = agg_revenue.create_pipeline()
    agg_text_messaging_pipeline = agg_text_messaging.create_pipeline()
    agg_voice_calling_pipeline = agg_voice_calling.create_pipeline()
    agg_customer_profile_pipeline = agg_customer_profile.create_pipeline()
    agg_network_pipeline = agg_network.create_pipeline()
    agg_outlets_pipeline = agg_outlets.create_pipeline()
    # agg_handset_internal_pipeline = agg_handset_internal.create_pipeline()
    agg_payu_usage_pipeline = agg_payu_usage.create_pipeline()
    agg_customer_los_pipeline = agg_customer_los.create_pipeline()
    agg_mobility_pipeline = agg_mobility.create_pipeline()
    agg_mytsel_pipeline = agg_mytsel.create_pipeline()

    # Primary layer - filtering partner data & filling time series
    msisdn_list_pipeline = msisdn_list.create_pipeline()
    prm_airtime_loan_pipeline = prm_airtime_loan.create_pipeline()
    prm_internet_app_usage_pipeline = prm_internet_app_usage.create_pipeline()
    prm_handset_pipeline = prm_handset.create_pipeline()
    # prm_handset_internal_pipeline = prm_handset_internal.create_pipeline()
    prm_internet_usage_pipeline = prm_internet_usage.create_pipeline()
    prm_product_pipeline = prm_product.create_pipeline()
    prm_recharge_pipeline = prm_recharge.create_pipeline()
    prm_revenue_pipeline = prm_revenue.create_pipeline()
    prm_text_messaging_pipeline = prm_text_messaging.create_pipeline()
    prm_voice_calling_pipeline = prm_voice_calling.create_pipeline()
    prm_network_pipeline = prm_network.create_pipeline()
    prm_payu_usage_pipeline = prm_payu_usage.create_pipeline()
    prm_customer_los_pipeline = prm_customer_los.create_pipeline()
    prm_customer_profile_pipeline = prm_customer_profile.create_pipeline()
    prm_mobility_pipeline = prm_mobility.create_pipeline()
    # Covid-19 Primary Layer
    prm_internet_usage_covid_pipeline = prm_internet_usage.create_covid_pipeline()
    prm_internet_app_usage_covid_pipeline = (
        prm_internet_app_usage.create_covid_pipeline()
    )
    prm_recharge_covid_pipeline = prm_recharge.create_covid_pipeline()
    prm_text_messaging_covid_pipeline = prm_text_messaging.create_covid_pipeline()
    prm_voice_calling_covid_pipeline = prm_voice_calling.create_covid_pipeline()
    prm_mytsel_pipeline = prm_mytsel.create_pipeline()

    # Feature layer - Calculating all the features
    fea_airtime_loan_pipeline = fea_airtime_loan.create_pipeline()
    fea_customer_los_pipeline = fea_customer_los.create_pipeline()
    fea_handset_pipeline = fea_handset.create_pipeline()
    # fea_handset_internal_pipeline = fea_handset_internal.create_pipeline()
    fea_internet_app_usage_pipeline = fea_internet_app_usage.create_pipeline()
    fea_internet_usage_pipeline = fea_internet_usage.create_pipeline()
    fea_product_pipeline = fea_product.create_pipeline()
    fea_recharge_pipeline = fea_recharge.create_pipeline()
    fea_revenue_pipeline = fea_revenue.create_pipeline()
    fea_text_messaging_pipeline = fea_text_messaging.create_pipeline()
    fea_voice_calling_pipeline = fea_voice_calling.create_pipeline()
    fea_customer_profile_pipeline = fea_customer_profile.create_pipeline()
    fea_network_pipeline = fea_network.create_pipeline()
    fea_payu_usage_pipeline = fea_payu_usage.create_pipeline()
    fea_outlets_pipeline = fea_outlets.create_pipeline()
    fea_mobility_pipeline = fea_mobility.create_pipeline()
    # Covid-19 Feature Layer
    fea_internet_usage_covid_pipeline = fea_internet_usage.create_covid_pipeline()
    fea_internet_app_usage_covid_pipeline = (
        fea_internet_app_usage.create_covid_pipeline()
    )
    fea_recharge_covid_pipeline = fea_recharge.create_covid_pipeline()
    fea_text_messaging_covid_pipeline = fea_text_messaging.create_covid_pipeline()
    fea_voice_calling_covid_pipeline = fea_voice_calling.create_covid_pipeline()
    fea_mytsel_pipeline = fea_mytsel.create_pipeline()

    # Model input layer - merge all features
    de_pipeline_master = model_input.create_pipeline()

    qa_pipeline = qa.create_pipeline(project_context=kwargs["project_context"])
    qa_timeliness_pipeline = qa_timeliness.create_pipeline()
    qa_setup_pipeline = qa.create_setup_pipeline()
    qa_reseller_custom_pipeline = reseller_custom_qa.create_pipeline()

    data_dictionary_pipeline = data_dictionary.create_pipeline()

    de_pipeline_pre_processing = (
        prep_schema_checks_pipeline
        + prep_storage_checks_pipeline
        + prep_dq_availability_pipeline
    )

    de_pipeline_aggregation = (
        agg_airtime_loan_pipeline
        + agg_customer_los_pipeline
        + agg_customer_profile_pipeline
        # + agg_handset_internal_pipeline
        + agg_handset_pipeline
        + agg_internet_app_usage_pipeline
        + agg_internet_usage_pipeline
        + agg_network_pipeline
        + agg_outlets_pipeline
        + agg_payu_usage_pipeline
        + agg_product_pipeline
        + agg_recharge_pipeline
        + agg_revenue_pipeline
        + agg_text_messaging_pipeline
        + agg_voice_calling_pipeline
        + agg_mobility_pipeline
        + agg_mytsel_pipeline
    )

    de_pipeline_primary = (
        msisdn_list_pipeline
        + scaffold_pipeline
        + prm_airtime_loan_pipeline
        + prm_customer_los_pipeline
        + prm_customer_profile_pipeline
        # + prm_handset_internal_pipeline
        + prm_handset_pipeline
        + prm_internet_app_usage_pipeline
        + prm_internet_usage_pipeline
        + prm_network_pipeline
        + prm_payu_usage_pipeline
        + prm_product_pipeline
        + prm_recharge_pipeline
        + prm_revenue_pipeline
        + prm_text_messaging_pipeline
        + prm_voice_calling_pipeline
        + prm_mobility_pipeline
        + prm_mytsel_pipeline
    )

    de_pipeline_feature = (
        fea_airtime_loan_pipeline
        + fea_customer_los_pipeline
        + fea_customer_profile_pipeline
        # + fea_handset_internal_pipeline
        + fea_handset_pipeline
        + fea_internet_app_usage_pipeline
        + fea_internet_usage_pipeline
        + fea_network_pipeline
        + fea_outlets_pipeline
        + fea_payu_usage_pipeline
        + fea_product_pipeline
        + fea_recharge_pipeline
        + fea_revenue_pipeline
        + fea_text_messaging_pipeline
        + fea_voice_calling_pipeline
        + fea_mobility_pipeline
        + fea_mytsel_pipeline
    )

    de_pipeline_qa = (
        qa_setup_pipeline
        + qa_pipeline
        + qa_reseller_custom_pipeline
        + qa_timeliness_pipeline
    )

    de_pipeline_data_dictionary = data_dictionary_pipeline

    de_primary_covid = (
        prm_internet_usage_covid_pipeline
        + prm_internet_app_usage_covid_pipeline
        + prm_recharge_covid_pipeline
        + prm_text_messaging_covid_pipeline
        + prm_voice_calling_covid_pipeline
    )
    de_feature_covid = (
        fea_internet_usage_covid_pipeline
        + fea_internet_app_usage_covid_pipeline
        + fea_recharge_covid_pipeline
        + fea_text_messaging_covid_pipeline
        + fea_voice_calling_covid_pipeline
    )
    de_pipeline_covid = de_primary_covid + de_feature_covid

    de_pipeline_all = (
        de_pipeline_pre_processing
        + de_pipeline_aggregation
        + de_pipeline_primary
        + de_pipeline_feature
        + de_pipeline_master
        + de_pipeline_qa
        + de_pipeline_data_dictionary
        + de_pipeline_covid
    )

    pipeline_all = de_pipeline_all

    # Domain Pipelines:
    customer_los_pipeline = (
        agg_customer_los_pipeline
        + prm_customer_los_pipeline
        + fea_customer_los_pipeline
    )

    customer_profile_pipeline = (
        agg_customer_profile_pipeline
        + prm_customer_profile_pipeline
        + fea_customer_profile_pipeline
    )

    handset_pipeline = (
        agg_handset_pipeline + prm_handset_pipeline + fea_handset_pipeline
    )

    internet_app_usage_pipeline = (
        agg_internet_app_usage_pipeline
        + prm_internet_app_usage_pipeline
        + fea_internet_app_usage_pipeline
    )

    internet_usage_pipeline = (
        agg_internet_usage_pipeline
        + prm_internet_usage_pipeline
        + fea_internet_usage_pipeline
    )

    mytsel_pipeline = agg_mytsel_pipeline + prm_mytsel_pipeline + fea_mytsel_pipeline

    mobility_pipeline = (
        agg_mobility_pipeline + prm_mobility_pipeline + fea_mobility_pipeline
    )

    network_pipeline = (
        agg_network_pipeline + prm_network_pipeline + fea_network_pipeline
    )

    outlets_pipeline = agg_outlets_pipeline + fea_outlets_pipeline

    payu_usage_pipeline = (
        agg_payu_usage_pipeline + prm_payu_usage_pipeline + fea_payu_usage_pipeline
    )

    product_pipeline = (
        agg_product_pipeline + prm_product_pipeline + fea_product_pipeline
    )

    recharge_pipeline = (
        agg_recharge_pipeline + prm_recharge_pipeline + fea_recharge_pipeline
    )

    revenue_pipeline = (
        agg_revenue_pipeline + prm_revenue_pipeline + fea_revenue_pipeline
    )

    text_messaging_pipeline = (
        agg_text_messaging_pipeline
        + prm_text_messaging_pipeline
        + fea_text_messaging_pipeline
    )

    voice_calling_pipeline = (
        agg_voice_calling_pipeline
        + prm_voice_calling_pipeline
        + fea_voice_calling_pipeline
    )

    return {
        # 4g clsutering
        "fg_segmentation": fg_sementing_pipeline,
        "fg_psi": fg_psi_pipeline,
        "fg_clustering": fg_clustering_pipeline,
        "fg_clustering_scoring": fg_clustering_pipeline_scoring,
        # "fg": fg_pipeline_all,
        # SES clsutering
        "ses_segmentation": ses_sementing_pipeline,
        # "ses_psi": ses_psi_pipeline,
        "ses_clustering": ses_clustering_pipeline,
        "ses_clustering_scoring": ses_clustering_pipeline_scoring,
        # "ses": ses_pipeline_all,
        # De
        "de_pipeline_all": de_pipeline_all,
        "de_pipeline_preprocessing": de_pipeline_pre_processing,
        "de_pipeline_aggregation": de_pipeline_aggregation,
        "de_pipeline_primary": de_pipeline_primary,
        "de_pipeline_feature": de_pipeline_feature,
        "de_pipeline_master": de_pipeline_master,
        "de_pipeline_qa": de_pipeline_qa,
        "de_pipeline_data_dictionary": de_pipeline_data_dictionary,
        # Covid Pipeline
        "de_primary_covid": de_primary_covid,
        "de_feature_covid": de_feature_covid,
        "de_pipeline_covid": de_pipeline_covid,
        # Common Pipelines
        "scaffold_pipeline": scaffold_pipeline,
        "msisdn_list_pipeline": msisdn_list_pipeline,
        # Domain Pipelines
        "customer_los_pipeline": customer_los_pipeline,
        "customer_profile_pipeline": customer_profile_pipeline,
        "handset_pipeline": handset_pipeline,
        "internet_app_usage_pipeline": internet_app_usage_pipeline,
        "internet_usage_pipeline": internet_usage_pipeline,
        "mytsel_pipeline": mytsel_pipeline,
        "mobility_pipeline": mobility_pipeline,
        "network_pipeline": network_pipeline,
        "outlets_pipeline": outlets_pipeline,
        "payu_usage_pipeline": payu_usage_pipeline,
        "product_pipeline": product_pipeline,
        "recharge_pipeline": recharge_pipeline,
        "revenue_pipeline": revenue_pipeline,
        "text_messaging_pipeline": text_messaging_pipeline,
        "voice_calling_pipeline": voice_calling_pipeline,
        # Default Pipeline
        "__default__": pipeline_all,
    }
