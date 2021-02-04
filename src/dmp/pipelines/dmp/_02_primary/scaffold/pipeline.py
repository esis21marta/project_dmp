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

from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.dmp._02_primary.scaffold.create_scaffold import (
    create_msisdn_monthly_scaffold,
    create_msisdn_scaffold,
    create_msisdn_scaffold_fixed_weekstart,
    create_scaffold,
    create_scaffold_3m_4m,
)


def create_pipeline() -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to create the `scaffolding` parquet for the primary layer.
    Given a list of dataframe inputs, it will create a scaffold parquet file which can be used in the `fill_time_series` function
    defined here: `dmp.pipelines.dmp._02_primary.scaffold.fill_time_series.fill_time_series`.

    Returns:
        A Pipeline object containing all the Scaffold Nodes.
    """

    return Pipeline(
        [
            # node(
            #     func=create_scaffold(),
            #     inputs=[
            #         "l2_customer_los_weekly_data_final",
            #         "l2_handset_weekly_aggregated_filtered",
            #         "l3_internet_app_usage_weekly_filtered",
            #         "l1_internet_usage_weekly_filtered",
            #         "l1_rech_weekly_filtered",
            #         "l1_acc_bal_weekly_filtered",
            #         "l1_chg_pkg_prchse_weekly_filtered",
            #         "l2_revenue_weekly_data_filtered",
            #         "l2_text_messaging_weekly_filtered",
            #         "l2_commercial_text_messaging_weekly_filtered",
            #         "l2_voice_calling_weekly_aggregated_filtered",
            #     ],
            #     outputs="l1_weekly_scaffold_5m",
            #     name="create_scaffold",
            #     tags=[
            #         "prm_handset",
            #         "handset_pipeline",
            #         "prm_handset_internal",
            #         "handset_internal_pipeline",
            #         "prm_internet_app_usage",
            #         "internet_app_usage_pipeline",
            #         "prm_internet_usage",
            #         "internet_usage_pipeline",
            #         "prm_network",
            #         "network_pipeline",
            #         "prm_network_msisdn",
            #         "network_msisdn_pipeline",
            #         "prm_payu_usage",
            #         "payu_usage_pipeline",
            #         "prm_product",
            #         "product_pipeline",
            #         "prm_recharge",
            #         "recharge_pipeline",
            #         "prm_revenue",
            #         "revenue_pipeline",
            #         "prm_text_messaging",
            #         "text_messaging_pipeline",
            #         "prm_voice_calls",
            #         "voice_calls_pipeline",
            #         "prm_scoring_pipeline",
            #         "scoring_pipeline",
            #     ],
            # ),
            node(
                func=create_msisdn_scaffold,
                inputs=[
                    "l1_prepaid_customers_data_6m",
                    "l1_postpaid_customers_data_6m",
                    "partner_msisdns",
                ],
                outputs="l1_weekly_scaffold_6m",
                name="create_scaffold",
                tags=[
                    "prm_airtime_loan",
                    "airtime_loan_pipeline",
                    "prm_handset",
                    "handset_pipeline",
                    "prm_handset_internal",
                    "handset_internal_pipeline",
                    "prm_internet_app_usage",
                    "internet_app_usage_pipeline",
                    "prm_internet_usage",
                    "internet_usage_pipeline",
                    "prm_network",
                    "network_pipeline",
                    "prm_network_msisdn",
                    "network_msisdn_pipeline",
                    "prm_payu_usage",
                    "payu_usage_pipeline",
                    "prm_product",
                    "product_pipeline",
                    "prm_recharge",
                    "recharge_pipeline",
                    "prm_revenue",
                    "revenue_pipeline",
                    "prm_text_messaging",
                    "text_messaging_pipeline",
                    "prm_voice_calls",
                    "voice_calls_pipeline",
                    "prm_scoring_pipeline",
                    "scoring_pipeline",
                ],
            ),
            node(
                func=create_msisdn_scaffold_fixed_weekstart(
                    "pre_covid_first_weekstart", "pre_covid_last_weekstart"
                ),
                inputs=[
                    "l1_prepaid_customers_data_all",
                    "l1_postpaid_customers_data_all",
                    "partner_msisdns",
                ],
                outputs="l1_weekly_scaffold_pre_covid",
                name="create_scaffold_pre_covid",
                tags=[
                    "prm_recharge_covid",
                    "recharge_covid_pipeline",
                    "prm_internet_app_usage_covid",
                    "internet_app_usage_covid_pipeline",
                    "prm_text_messaging_covid",
                    "text_messaging_covid_pipeline",
                    "prm_voice_calls_covid",
                    "voice_calls_covid_pipeline",
                    "prm_internet_usage_covid",
                    "internet_usage_covid_pipeline",
                ],
            ),
            node(
                func=create_msisdn_scaffold_fixed_weekstart(
                    "covid_first_weekstart", "covid_last_weekstart"
                ),
                inputs=[
                    "l1_prepaid_customers_data_all",
                    "l1_postpaid_customers_data_all",
                    "partner_msisdns",
                ],
                outputs="l1_weekly_scaffold_covid",
                name="create_scaffold_covid",
                tags=[
                    "prm_recharge_covid",
                    "recharge_covid_pipeline",
                    "prm_internet_app_usage_covid",
                    "internet_app_usage_covid_pipeline",
                    "prm_text_messaging_covid",
                    "text_messaging_covid_pipeline",
                    "prm_voice_calls_covid",
                    "voice_calls_covid_pipeline",
                    "prm_internet_usage_covid",
                    "internet_usage_covid_pipeline",
                ],
            ),
            node(
                func=create_msisdn_scaffold_fixed_weekstart(
                    "post_covid_first_weekstart", "post_covid_last_weekstart"
                ),
                inputs=[
                    "l1_prepaid_customers_data_all",
                    "l1_postpaid_customers_data_all",
                    "partner_msisdns",
                ],
                outputs="l1_weekly_scaffold_post_covid",
                name="create_scaffold_post_covid",
                tags=[
                    "prm_recharge_covid",
                    "recharge_covid_pipeline",
                    "prm_internet_app_usage_covid",
                    "internet_app_usage_covid_pipeline",
                    "prm_text_messaging_covid",
                    "text_messaging_covid_pipeline",
                    "prm_voice_calls_covid",
                    "voice_calls_covid_pipeline",
                    "prm_internet_usage_covid",
                    "internet_usage_covid_pipeline",
                ],
            ),
            node(
                func=create_scaffold_3m_4m,
                inputs="l1_weekly_scaffold_6m",
                outputs=dict(
                    df_scaffold_3m="l1_weekly_scaffold",
                    df_scaffold_4m="l1_weekly_scaffold_4m",
                ),
                name="create_scaffold_3m_4m",
                tags=[
                    "prm_airtime_loan",
                    "airtime_loan_pipeline",
                    "prm_handset",
                    "handset_pipeline",
                    "prm_mytsel",
                    "mytsel_pipeline",
                    "prm_handset_internal",
                    "handset_internal_pipeline",
                    "prm_internet_app_usage",
                    "internet_app_usage_pipeline",
                    "prm_internet_usage",
                    "internet_usage_pipeline",
                    "prm_network",
                    "network_pipeline",
                    "prm_network_msisdn",
                    "network_msisdn_pipeline",
                    "prm_payu_usage",
                    "payu_usage_pipeline",
                    "prm_product",
                    "product_pipeline",
                    "prm_recharge",
                    "recharge_pipeline",
                    "prm_revenue",
                    "revenue_pipeline",
                    "prm_text_messaging",
                    "text_messaging_pipeline",
                    "prm_voice_calls",
                    "voice_calls_pipeline",
                    "prm_scoring_pipeline",
                    "scoring_pipeline",
                ],
            ),
            node(
                func=create_scaffold("lac_ci"),
                inputs="l1_network_lacci_weekly_aggregated",
                outputs="l1_weekly_scaffold_lacci_6m",
                name="create_scaffold_lacci",
                tags=[
                    "prm_network_lacci",
                    "network_lacci_pipeline",
                    "prm_network",
                    "network_pipeline",
                ],
            ),
            node(
                func=create_msisdn_monthly_scaffold,
                inputs=[
                    "l1_prepaid_customers_data_6m",
                    "l1_postpaid_customers_data_6m",
                    "partner_msisdns",
                ],
                outputs="l1_monthly_scaffold",
                name="create_scaffold_monthly",
                tags=["prm_mobility", "de_mobility_pipeline",],
            ),
            node(
                func=create_scaffold("lac"),
                inputs="l1_network_lac_weekly_aggregated",
                outputs="l1_weekly_scaffold_lac_6m",
                name="create_scaffold_lac",
                tags=[
                    "prm_network_lacci",
                    "network_lacci_pipeline",
                    "prm_network",
                    "network_pipeline",
                ],
            ),
        ],
        tags=["prm_scaffold", "de_primary_pipeline", "de_pipeline"],
    )
