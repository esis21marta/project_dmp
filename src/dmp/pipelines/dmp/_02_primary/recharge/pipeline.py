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

from src.dmp.pipelines.dmp._02_primary.filter_partner_data.filter_partner_data import (
    filter_partner_data,
    filter_partner_data_fixed_weekstart,
)
from src.dmp.pipelines.dmp._02_primary.recharge.recharge_fill_time_series import (
    fill_time_series_recharge,
)
from src.dmp.pipelines.dmp._02_primary.scaffold.fill_time_series import fill_time_series


def create_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to create the primary layer for `Recharge` after the data
    has been aggregated to weekly-level. This function will be executed as part of the `primary` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `Recharge` nodes to execute the `primary` layer.
    """
    return Pipeline(
        [
            node(
                func=filter_partner_data,
                inputs=[
                    "partner_msisdns",
                    "l1_rech_weekly",
                    "params:filter_partner_data",
                ],
                outputs="l1_rech_weekly_filtered",
                name="filter_partner_data_recharge",
                tags=["prm_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fill_time_series(),
                inputs=["l1_weekly_scaffold", "l1_rech_weekly_filtered"],
                outputs="l1_rech_weekly_final",
                name="fill_time_series_recharge",
                tags=["prm_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=filter_partner_data,
                inputs=[
                    "partner_msisdns",
                    "l1_digi_rech_weekly",
                    "params:filter_partner_data",
                ],
                outputs="l1_digi_rech_weekly_filtered",
                name="filter_partner_data_digi_recharge",
                tags=["prm_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fill_time_series(),
                inputs=["l1_weekly_scaffold", "l1_digi_rech_weekly_filtered"],
                outputs="l1_digi_rech_weekly_final",
                name="fill_time_series_digi_recharge",
                tags=["prm_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=filter_partner_data,
                inputs=[
                    "partner_msisdns",
                    "l2_account_balance_weekly",
                    "params:filter_partner_data",
                ],
                outputs="l1_acc_bal_weekly_filtered",
                name="filter_partner_data_recharge_account_balance",
                tags=["prm_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fill_time_series_recharge,
                inputs=["l1_weekly_scaffold", "l1_acc_bal_weekly_filtered"],
                outputs="l1_acc_bal_weekly_final",
                name="fill_time_series_recharge_account_balance",
                tags=["prm_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=filter_partner_data,
                inputs=[
                    "partner_msisdns",
                    "l1_chg_pkg_prchse_weekly",
                    "params:filter_partner_data",
                ],
                outputs="l1_chg_pkg_prchse_weekly_filtered",
                name="filter_partner_data_recharge_package_purchase",
                tags=["prm_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fill_time_series(),
                inputs=["l1_weekly_scaffold", "l1_chg_pkg_prchse_weekly_filtered"],
                outputs="l1_chg_pkg_prchse_weekly_final",
                name="fill_time_series_recharge_package_purchase",
                tags=["prm_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=filter_partner_data,
                inputs=[
                    "partner_msisdns",
                    "l1_mytsel_rech_weekly",
                    "params:filter_partner_data",
                ],
                outputs="l1_mytsel_rech_weekly_filtered",
                name="filter_partner_data_recharge_mytsel",
                tags="prm_recharge_mytsel",
            ),
            node(
                func=fill_time_series(),
                inputs=["l1_weekly_scaffold", "l1_mytsel_rech_weekly_filtered"],
                outputs="l1_mytsel_rech_weekly_final",
                name="fill_time_series_recharge_mytsel",
                tags="prm_recharge_mytsel",
            ),
        ],
        tags=[
            "prm_recharge",
            "recharge_pipeline",
            "de_primary_pipeline",
            "de_pipeline",
        ],
    )


def create_covid_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to create the primary layer for Covid-19 related features after the data
    has been aggregated to weekly-level. This function will be executed as part of the `primary` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the Covid-19 nodes to execute the `primary` layer.
    """

    return Pipeline(
        [
            ## PRE-COVID PRIMARY
            node(
                func=filter_partner_data_fixed_weekstart,
                inputs=[
                    "partner_msisdns",
                    "l2_rech_weekly_all",
                    "params:filter_partner_data",
                    "params:pre_covid_first_weekstart",
                    "params:pre_covid_last_weekstart",
                ],
                outputs="l3_rech_weekly_filtered_pre_covid",
                name="filter_partner_data_recharge_pre_covid",
            ),
            node(
                func=fill_time_series(),
                inputs=[
                    "l1_weekly_scaffold_pre_covid",
                    "l3_rech_weekly_filtered_pre_covid",
                ],
                outputs="l4_rech_weekly_final_pre_covid",
                name="fill_time_series_recharge_pre_covid",
            ),
            node(
                func=filter_partner_data_fixed_weekstart,
                inputs=[
                    "partner_msisdns",
                    "l2_digi_rech_weekly_all",
                    "params:filter_partner_data",
                    "params:pre_covid_first_weekstart",
                    "params:pre_covid_last_weekstart",
                ],
                outputs="l3_digi_rech_weekly_filtered_pre_covid",
                name="filter_partner_data_digi_recharge_pre_covid",
            ),
            node(
                func=fill_time_series(),
                inputs=[
                    "l1_weekly_scaffold_pre_covid",
                    "l3_digi_rech_weekly_filtered_pre_covid",
                ],
                outputs="l4_digi_rech_weekly_final_pre_covid",
                name="fill_time_series_digi_recharge_pre_covid",
            ),
            node(
                func=filter_partner_data_fixed_weekstart,
                inputs=[
                    "partner_msisdns",
                    "l2_account_balance_weekly_all",
                    "params:filter_partner_data",
                    "params:pre_covid_first_weekstart",
                    "params:pre_covid_last_weekstart",
                ],
                outputs="l3_acc_bal_weekly_filtered_pre_covid",
                name="filter_partner_data_recharge_account_balance_pre_covid",
            ),
            node(
                func=fill_time_series_recharge,
                inputs=[
                    "l1_weekly_scaffold_pre_covid",
                    "l3_acc_bal_weekly_filtered_pre_covid",
                ],
                outputs="l4_acc_bal_weekly_final_pre_covid",
                name="fill_time_series_recharge_account_balance_pre_covid",
            ),
            node(
                func=filter_partner_data_fixed_weekstart,
                inputs=[
                    "partner_msisdns",
                    "l2_chg_pkg_prchse_weekly_all",
                    "params:filter_partner_data",
                    "params:pre_covid_first_weekstart",
                    "params:pre_covid_last_weekstart",
                ],
                outputs="l3_chg_pkg_prchse_weekly_filtered_pre_covid",
                name="filter_partner_data_recharge_package_purchase_pre_covid",
            ),
            node(
                func=fill_time_series(),
                inputs=[
                    "l1_weekly_scaffold_pre_covid",
                    "l3_chg_pkg_prchse_weekly_filtered_pre_covid",
                ],
                outputs="l4_chg_pkg_prchse_weekly_final_pre_covid",
                name="fill_time_series_recharge_package_purchase_pre_covid",
            ),
            ## COVID PRIMARY
            node(
                func=filter_partner_data_fixed_weekstart,
                inputs=[
                    "partner_msisdns",
                    "l2_rech_weekly_all",
                    "params:filter_partner_data",
                    "params:covid_first_weekstart",
                    "params:covid_last_weekstart",
                ],
                outputs="l3_rech_weekly_filtered_covid",
                name="filter_partner_data_recharge_covid",
            ),
            node(
                func=fill_time_series(),
                inputs=["l1_weekly_scaffold_covid", "l3_rech_weekly_filtered_covid"],
                outputs="l4_rech_weekly_final_covid",
                name="fill_time_series_recharge_covid",
            ),
            node(
                func=filter_partner_data_fixed_weekstart,
                inputs=[
                    "partner_msisdns",
                    "l2_digi_rech_weekly_all",
                    "params:filter_partner_data",
                    "params:covid_first_weekstart",
                    "params:covid_last_weekstart",
                ],
                outputs="l3_digi_rech_weekly_filtered_covid",
                name="filter_partner_data_digi_recharge_covid",
            ),
            node(
                func=fill_time_series(),
                inputs=[
                    "l1_weekly_scaffold_covid",
                    "l3_digi_rech_weekly_filtered_covid",
                ],
                outputs="l4_digi_rech_weekly_final_covid",
                name="fill_time_series_digi_recharge_covid",
            ),
            node(
                func=filter_partner_data_fixed_weekstart,
                inputs=[
                    "partner_msisdns",
                    "l2_account_balance_weekly_all",
                    "params:filter_partner_data",
                    "params:covid_first_weekstart",
                    "params:covid_last_weekstart",
                ],
                outputs="l3_acc_bal_weekly_filtered_covid",
                name="filter_partner_data_recharge_account_balance_covid",
            ),
            node(
                func=fill_time_series_recharge,
                inputs=[
                    "l1_weekly_scaffold_covid",
                    "l3_acc_bal_weekly_filtered_covid",
                ],
                outputs="l4_acc_bal_weekly_final_covid",
                name="fill_time_series_recharge_account_balance_covid",
            ),
            node(
                func=filter_partner_data_fixed_weekstart,
                inputs=[
                    "partner_msisdns",
                    "l2_chg_pkg_prchse_weekly_all",
                    "params:filter_partner_data",
                    "params:covid_first_weekstart",
                    "params:covid_last_weekstart",
                ],
                outputs="l3_chg_pkg_prchse_weekly_filtered_covid",
                name="filter_partner_data_recharge_package_purchase_covid",
            ),
            node(
                func=fill_time_series(),
                inputs=[
                    "l1_weekly_scaffold_covid",
                    "l3_chg_pkg_prchse_weekly_filtered_covid",
                ],
                outputs="l4_chg_pkg_prchse_weekly_final_covid",
                name="fill_time_series_recharge_package_purchase_covid",
            ),
            # POST-COVID PRIMARY
            node(
                func=filter_partner_data_fixed_weekstart,
                inputs=[
                    "partner_msisdns",
                    "l2_rech_weekly_all",
                    "params:filter_partner_data",
                    "params:post_covid_first_weekstart",
                    "params:post_covid_last_weekstart",
                ],
                outputs="l3_rech_weekly_filtered_post_covid",
                name="filter_partner_data_recharge_post_covid",
            ),
            node(
                func=fill_time_series(),
                inputs=[
                    "l1_weekly_scaffold_post_covid",
                    "l3_rech_weekly_filtered_post_covid",
                ],
                outputs="l4_rech_weekly_final_post_covid",
                name="fill_time_series_recharge_post_covid",
            ),
            node(
                func=filter_partner_data_fixed_weekstart,
                inputs=[
                    "partner_msisdns",
                    "l2_digi_rech_weekly_all",
                    "params:filter_partner_data",
                    "params:post_covid_first_weekstart",
                    "params:post_covid_last_weekstart",
                ],
                outputs="l3_digi_rech_weekly_filtered_post_covid",
                name="filter_partner_data_digi_recharge_post_covid",
            ),
            node(
                func=fill_time_series(),
                inputs=[
                    "l1_weekly_scaffold_post_covid",
                    "l3_digi_rech_weekly_filtered_post_covid",
                ],
                outputs="l4_digi_rech_weekly_final_post_covid",
                name="fill_time_series_digi_recharge_post_covid",
            ),
            node(
                func=filter_partner_data_fixed_weekstart,
                inputs=[
                    "partner_msisdns",
                    "l2_account_balance_weekly_all",
                    "params:filter_partner_data",
                    "params:post_covid_first_weekstart",
                    "params:post_covid_last_weekstart",
                ],
                outputs="l3_acc_bal_weekly_filtered_post_covid",
                name="filter_partner_data_recharge_account_balance_post_covid",
            ),
            node(
                func=fill_time_series_recharge,
                inputs=[
                    "l1_weekly_scaffold_post_covid",
                    "l3_acc_bal_weekly_filtered_post_covid",
                ],
                outputs="l4_acc_bal_weekly_final_post_covid",
                name="fill_time_series_recharge_account_balance_post_covid",
            ),
            node(
                func=filter_partner_data_fixed_weekstart,
                inputs=[
                    "partner_msisdns",
                    "l2_chg_pkg_prchse_weekly_all",
                    "params:filter_partner_data",
                    "params:post_covid_first_weekstart",
                    "params:post_covid_last_weekstart",
                ],
                outputs="l3_chg_pkg_prchse_weekly_filtered_post_covid",
                name="filter_partner_data_recharge_package_purchase_post_covid",
            ),
            node(
                func=fill_time_series(),
                inputs=[
                    "l1_weekly_scaffold_post_covid",
                    "l3_chg_pkg_prchse_weekly_filtered_post_covid",
                ],
                outputs="l4_chg_pkg_prchse_weekly_final_post_covid",
                name="fill_time_series_recharge_package_purchase_post_covid",
            ),
        ],
        tags=["prm_covid", "prm_recharge_covid", "recharge_covid_pipeline"],
    )
