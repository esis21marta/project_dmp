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

OUTPUT_COLUMNS_SPEND_PATTERN = [
    "msisdn",
    "weekstart",
    "fea_days_since_last_rev_payu",
    "fea_days_since_last_rev_pkg",
    "fea_days_since_last_rev_tot",
]

OUTPUT_COLUMNS_MONTHLY_SPEND = [
    "msisdn",
    "weekstart",
    "fea_monthly_spend_voice_sum_01m_by_02m",
    "fea_monthly_spend_data_sum_01m_by_02m",
    "fea_monthly_spend_sms_sum_01m_by_02m",
    "fea_monthly_spend_roam_sum_01m_by_02m",
    "fea_monthly_spend_voice_min_03m",
    "fea_monthly_spend_data_min_03m",
    "fea_monthly_spend_sms_min_03m",
    "fea_monthly_spend_roam_min_03m",
    "fea_monthly_spend_voice_max_03m",
    "fea_monthly_spend_data_max_03m",
    "fea_monthly_spend_sms_max_03m",
    "fea_monthly_spend_roam_max_03m",
    "fea_monthly_spend_voice_pattern_01w_by_01m_over_03m",
    "fea_monthly_spend_voice_pattern_02w_by_01m_over_03m",
    "fea_monthly_spend_voice_pattern_03w_by_01m_over_03m",
    "fea_monthly_spend_voice_pattern_04w_by_01m_over_03m",
    "fea_monthly_spend_data_pattern_01w_by_01m_over_03m",
    "fea_monthly_spend_data_pattern_02w_by_01m_over_03m",
    "fea_monthly_spend_data_pattern_03w_by_01m_over_03m",
    "fea_monthly_spend_data_pattern_04w_by_01m_over_03m",
    "fea_monthly_spend_sms_pattern_01w_by_01m_over_03m",
    "fea_monthly_spend_sms_pattern_02w_by_01m_over_03m",
    "fea_monthly_spend_sms_pattern_03w_by_01m_over_03m",
    "fea_monthly_spend_sms_pattern_04w_by_01m_over_03m",
    "fea_monthly_spend_roam_pattern_01w_by_01m_over_03m",
    "fea_monthly_spend_roam_pattern_02w_by_01m_over_03m",
    "fea_monthly_spend_roam_pattern_03w_by_01m_over_03m",
    "fea_monthly_spend_roam_pattern_04w_by_01m_over_03m",
    "fea_rev_alt_monthly_spend_voice_sum_01m_by_02m",
    "fea_rev_alt_monthly_spend_data_sum_01m_by_02m",
    "fea_rev_alt_monthly_spend_sms_sum_01m_by_02m",
    "fea_rev_alt_monthly_spend_roam_sum_01m_by_02m",
]

OUTPUT_COLUMNS_SALARY = [
    "msisdn",
    "weekstart",
    "fea_monthly_spend_voice_pattern_salary_1_5_over_03m",
    "fea_monthly_spend_voice_pattern_salary_6_10_over_03m",
    "fea_monthly_spend_voice_pattern_salary_11_15_over_03m",
    "fea_monthly_spend_voice_pattern_salary_16_20_over_03m",
    "fea_monthly_spend_voice_pattern_salary_21_25_over_03m",
    "fea_monthly_spend_voice_pattern_salary_26_31_over_03m",
    "fea_monthly_spend_data_pattern_salary_1_5_over_03m",
    "fea_monthly_spend_data_pattern_salary_6_10_over_03m",
    "fea_monthly_spend_data_pattern_salary_11_15_over_03m",
    "fea_monthly_spend_data_pattern_salary_16_20_over_03m",
    "fea_monthly_spend_data_pattern_salary_21_25_over_03m",
    "fea_monthly_spend_data_pattern_salary_26_31_over_03m",
    "fea_monthly_spend_sms_pattern_salary_1_5_over_03m",
    "fea_monthly_spend_sms_pattern_salary_6_10_over_03m",
    "fea_monthly_spend_sms_pattern_salary_11_15_over_03m",
    "fea_monthly_spend_sms_pattern_salary_16_20_over_03m",
    "fea_monthly_spend_sms_pattern_salary_21_25_over_03m",
    "fea_monthly_spend_sms_pattern_salary_26_31_over_03m",
    "fea_monthly_spend_roam_pattern_salary_1_5_over_03m",
    "fea_monthly_spend_roam_pattern_salary_6_10_over_03m",
    "fea_monthly_spend_roam_pattern_salary_11_15_over_03m",
    "fea_monthly_spend_roam_pattern_salary_16_20_over_03m",
    "fea_monthly_spend_roam_pattern_salary_21_25_over_03m",
    "fea_monthly_spend_roam_pattern_salary_26_31_over_03m",
]
