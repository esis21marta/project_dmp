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

from typing import Any, Dict

import pyspark


def pack(
    # External
    kredivo_default_probability_input: pyspark.sql.DataFrame,
    kredivo_default_probability_params: Dict[str, Any],
    campaign_eval_multisim_stratified_input: pyspark.sql.DataFrame,
    campaign_eval_multisim_stratified_params: Dict[str, Any],
    campaign_eval_tnl_grabngo_stratified_input: pyspark.sql.DataFrame,
    campaign_eval_tnl_grabngo_stratified_params: Dict[str, Any],
    campaign_eval_tnl_pathtochurn_stratified_input: pyspark.sql.DataFrame,
    campaign_eval_tnl_pathtochurn_stratified_params: Dict[str, Any],
    kredivo_default_probability_validation_input: pyspark.sql.DataFrame,
    kredivo_default_probability_validation_params: Dict[str, Any],
    kredivo_income_input: pyspark.sql.DataFrame,
    kredivo_income_params: Dict[str, Any],
    kredivo_precampaign_input: pyspark.sql.DataFrame,
    kredivo_precampaign_params: Dict[str, Any],
    kredivo_poc_input: pyspark.sql.DataFrame,
    kredivo_poc_params: Dict[str, Any],
    home_credit_default_probability_input: pyspark.sql.DataFrame,
    home_credit_default_probability_params: Dict[str, Any],
    home_credit_default_probability_validation_input: pyspark.sql.DataFrame,
    home_credit_default_probability_validation_params: Dict[str, Any],
    # Internal
    # internal_random_sample_input: pyspark.sql.DataFrame,
    # internal_random_sample_params: Dict[str, Any],
    internal_hvc_input: pyspark.sql.DataFrame,
    internal_hvc_params: Dict[str, Any],
    internal_hvc_may_input: pyspark.sql.DataFrame,
    internal_hvc_may_params: Dict[str, Any],
    internal_ucg_201910_input: pyspark.sql.DataFrame,
    internal_ucg_201910_params: Dict[str, Any],
    # internal_4g_campaigns_input: pyspark.sql.DataFrame,
    # internal_4g_campaigns_params: Dict[str, Any],
    # Inlife and 4G
    fg_scale_up_samples_input: pyspark.sql.DataFrame,
    fg_scale_up_samples_params: Dict[str, Any],
    gamers_samples_input: pyspark.sql.DataFrame,
    gamers_samples_params: Dict[str, Any],
    drgb_input: pyspark.sql.DataFrame,
    drgb_params: Dict[str, Any],
    rgb_a_samples_input: pyspark.sql.DataFrame,
    rgb_a_samples_params: Dict[str, Any],
    hvc_lapser_a_input: pyspark.sql.DataFrame,
    hvc_lapser_a_params: Dict[str, Any],
    hvc_lapser_b_input: pyspark.sql.DataFrame,
    hvc_lapser_b_params: Dict[str, Any],
    multisim_4g_input: pyspark.sql.DataFrame,
    multisim_4g_params: Dict[str, Any],
    multisim_non_4g_samples_input: pyspark.sql.DataFrame,
    multisim_non_4g_samples_params: Dict[str, Any],
    hvc_prop_input: pyspark.sql.DataFrame,
    hvc_prop_params: Dict[str, Any],
    _4g_lapser_input: pyspark.sql.DataFrame,
    _4g_lapser_params: Dict[str, Any],
    acq_input: pyspark.sql.DataFrame,
    acq_params: Dict[str, Any],
    # Interface with DE
    de_msisdn_column: str,
) -> pyspark.sql.DataFrame:
    """
    Selects distinct msisdns from tables, unions them and renames the column to match DE input.

    # External
    :param kredivo_default_probability_input: table to pack
    :param kredivo_default_probability_params: dictionary containing information about the msisdn column
    :param kredivo_default_probability_validation_input: table to pack
    :param kredivo_default_probability_validation_params: dictionary containing information about the msisdn column
    :param kredivo_income_input: table to pack
    :param kredivo_income_params: dictionary containing information about the msisdn column
    :param kredivo_precampaign: table to pack
    :param kredivo_precampaign_params: dictionary containing information about the msisdn column
    :param kredivo_poc_input: table to pack
    :param kredivo_poc_params: dictionary containing information about the msisdn column
    :param home_credit_default_probability_input: table to pack
    :param home_credit_default_probability_params: dictionary containing information about the msisdn column
    :param home_credit_default_probability_validation_input: table to pack
    :param home_credit_default_probability_validation_params: dictionary containing information about the msisdn column
    # Internal
    :param internal_random_sample_input: table to pack
    :param internal_random_sample_params: dictionary containing information about the msisdn column
    :param internal_hvc_input: table to pack
    :param internal_hvc_params: dictionary containing information about the msisdn column
    :param internal_hvc_may_input: table to pack
    :param internal_hvc_may_params: dictionary containing information about the msisdn column
    :param internal_ucg_201910_input: table to pack
    :param internal_ucg_201910_params: dictionary containing information about the msisdn column
    :param internal_4g_campaigns_input: table to pack
    :param internal_4g_campaigns_params: dictionary containing information about the msisdn column
    # Inlife and 4G
    :param fg_scale_up_samples_input: table to pack
    :param fg_scale_up_samples_params: dictionary containing information about the msisdn column
    :param gamers_samples_input: table to pack
    :param gamers_samples_params: dictionary containing information about the msisdn column
    :param drgb_input: table to pack
    :param drgb_params: dictionary containing information about the msisdn column
    :param rgb_a_samples_input: table to pack
    :param rgb_a_samples_params: dictionary containing information about the msisdn column
    :param hvc_lapser_a_input: table to pack
    :param hvc_lapser_a_params: dictionary containing information about the msisdn column
    :param hvc_lapser_b_input: table to pack
    :param hvc_lapser_b_params: dictionary containing information about the msisdn column
    :param multisim_4g_input: table to pack
    :param multisim_4g_params: dictionary containing information about the msisdn column
    :param multisim_4g_input: table to pack
    :param multisim_4g_params: dictionary containing information about the msisdn column
    :param multisim_non4g_samples_input: table to pack
    :param multisim_non4g_samples_params: dictionary containing information about the msisdn column
    :param hvc_prop_input: table to pack
    :param hvc_prop_params: dictionary containing information about the msisdn column
    :param 4g_lapser_input: table to pack
    :param 4g_lapser_params: dictionary containing information about the msisdn column
    :param acq_input: table to pack
    :param acq_params: dictionary containing information about the msisdn column
    # Interface with DE
    :param de_msisdn_column: msisdn column name expected by DE
    :return: union of distinct msisdns
    """

    # External
    kredivo_default_probability_input = kredivo_default_probability_input.withColumnRenamed(
        kredivo_default_probability_params["msisdn_column"], de_msisdn_column
    )
    campaign_eval_multisim_stratified_input = campaign_eval_multisim_stratified_input.withColumnRenamed(
        kredivo_default_probability_params["msisdn_column"], de_msisdn_column
    )
    campaign_eval_tnl_grabngo_stratified_input = campaign_eval_tnl_grabngo_stratified_input.withColumnRenamed(
        kredivo_default_probability_params["msisdn_column"], de_msisdn_column
    )
    campaign_eval_tnl_pathtochurn_stratified_input = campaign_eval_tnl_pathtochurn_stratified_input.withColumnRenamed(
        kredivo_default_probability_params["msisdn_column"], de_msisdn_column
    )
    kredivo_default_probability_validation_input = kredivo_default_probability_validation_input.withColumnRenamed(
        kredivo_default_probability_validation_params["msisdn_column"], de_msisdn_column
    )
    kredivo_income_input = kredivo_income_input.withColumnRenamed(
        kredivo_income_params["msisdn_column"], de_msisdn_column
    )
    kredivo_precampaign_input = kredivo_precampaign_input.withColumnRenamed(
        kredivo_precampaign_params["msisdn_column"], de_msisdn_column
    )
    kredivo_poc_input = kredivo_poc_input.withColumnRenamed(
        kredivo_poc_params["msisdn_column"], de_msisdn_column
    )
    home_credit_default_probability_input = home_credit_default_probability_input.withColumnRenamed(
        home_credit_default_probability_params["msisdn_column"], de_msisdn_column
    )
    home_credit_default_probability_validation_input = home_credit_default_probability_validation_input.withColumnRenamed(
        home_credit_default_probability_validation_params["msisdn_column"],
        de_msisdn_column,
    )
    # Internal
    # internal_random_sample_input = internal_random_sample_input.withColumnRenamed(
    #     internal_random_sample_params["msisdn_column"], de_msisdn_column
    # )
    internal_hvc_input = internal_hvc_input.withColumnRenamed(
        internal_hvc_params["msisdn_column"], de_msisdn_column
    )
    internal_hvc_may_input = internal_hvc_may_input.withColumnRenamed(
        internal_hvc_may_params["msisdn_column"], de_msisdn_column
    )
    internal_ucg_201910_input = internal_ucg_201910_input.withColumnRenamed(
        internal_ucg_201910_params["msisdn_column"], de_msisdn_column
    )
    # internal_4g_campaigns_input = internal_4g_campaigns_input.withColumnRenamed(
    #     internal_4g_campaigns_params["msisdn_column"], de_msisdn_column
    # )
    # Inlife and 4G
    fg_scale_up_samples_input = fg_scale_up_samples_input.withColumnRenamed(
        fg_scale_up_samples_params["msisdn_column"], de_msisdn_column
    )
    gamers_samples_input = gamers_samples_input.withColumnRenamed(
        gamers_samples_params["msisdn_column"], de_msisdn_column
    )
    drgb_input = drgb_input.withColumnRenamed(
        drgb_params["msisdn_column"], de_msisdn_column
    )
    rgb_a_samples_input = rgb_a_samples_input.withColumnRenamed(
        rgb_a_samples_params["msisdn_column"], de_msisdn_column
    )
    hvc_lapser_a_input = hvc_lapser_a_input.withColumnRenamed(
        hvc_lapser_a_params["msisdn_column"], de_msisdn_column
    )
    hvc_lapser_b_input = hvc_lapser_b_input.withColumnRenamed(
        hvc_lapser_b_params["msisdn_column"], de_msisdn_column
    )
    multisim_4g_input = multisim_4g_input.withColumnRenamed(
        multisim_4g_params["msisdn_column"], de_msisdn_column
    )
    multisim_non4g_samples_input = multisim_non_4g_samples_input.withColumnRenamed(
        multisim_non_4g_samples_params["msisdn_column"], de_msisdn_column
    )
    hvc_prop_input = hvc_prop_input.withColumnRenamed(
        hvc_prop_params["msisdn_column"], de_msisdn_column
    )
    _4g_lapser_input = _4g_lapser_input.withColumnRenamed(
        _4g_lapser_params["msisdn_column"], de_msisdn_column
    )
    acq_input = acq_input.withColumnRenamed(
        acq_params["msisdn_column"], de_msisdn_column
    )
    return (
        kredivo_default_probability_input.select(de_msisdn_column)
        .union(campaign_eval_multisim_stratified_input.select(de_msisdn_column))
        .union(campaign_eval_tnl_grabngo_stratified_input.select(de_msisdn_column))
        .union(campaign_eval_tnl_pathtochurn_stratified_input.select(de_msisdn_column))
        .union(kredivo_default_probability_validation_input.select(de_msisdn_column))
        .union(kredivo_income_input.select(de_msisdn_column))
        .union(kredivo_precampaign_input.select(de_msisdn_column))
        .union(kredivo_poc_input.select(de_msisdn_column))
        .union(home_credit_default_probability_input.select(de_msisdn_column))
        .union(
            home_credit_default_probability_validation_input.select(de_msisdn_column)
        )
        # .union(internal_random_sample_input.select(de_msisdn_column))
        .union(internal_hvc_input.select(de_msisdn_column))
        .union(internal_hvc_may_input.select(de_msisdn_column))
        .union(internal_ucg_201910_input.select(de_msisdn_column))
        # .union(internal_4g_campaigns_input.select(de_msisdn_column))
        .union(fg_scale_up_samples_input.select(de_msisdn_column))
        .union(gamers_samples_input.select(de_msisdn_column))
        .union(drgb_input.select(de_msisdn_column))
        .union(rgb_a_samples_input.select(de_msisdn_column))
        .union(hvc_lapser_a_input.select(de_msisdn_column))
        .union(hvc_lapser_b_input.select(de_msisdn_column))
        .union(multisim_4g_input.select(de_msisdn_column))
        .union(multisim_non4g_samples_input.select(de_msisdn_column))
        .union(hvc_prop_input.select(de_msisdn_column))
        .union(_4g_lapser_input.select(de_msisdn_column))
        .union(acq_input.select(de_msisdn_column))
        .distinct()
        .repartition(10)
    )
