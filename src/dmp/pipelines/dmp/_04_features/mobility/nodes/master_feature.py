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

import pyspark

from utils import get_end_date, get_month_id_bw_sd_ed, get_start_date


def mobility_master_features(
    # l5_home_covid_stay_cnt,
    # l5_covid_work_home_distance,
    # l5_covid_work_home_not_same,
    # l5_work_covid_stay_cnt,
    # l5_home_stay_covid_distance,
    l5_home_stay_customer_profile,
    l5_home_stay,
    l5_home_stay_freq,
    l5_home_stay_unique_count,
    l5_work_home_same,
    l5_poi_freq,
    # l5_staypoint_covid_distance,
    # l5_staypoint_covid_monthly_stats,
    l5_staypoint_customer_profile,
    l5_staypoint_freq,
    l5_staypoint,
    l5_staypoint_unique_count,
    # l5_work_stay_covid_distance,
    l5_work_stay_customer_profile,
    l5_work_stay,
    l5_work_stay_freq,
    l5_work_stay_unique_count,
    sla_date_parameter,
) -> pyspark.sql.DataFrame:

    df_msisdn_mo = (
        l5_home_stay.join(
            l5_home_stay_customer_profile, on=["msisdn", "mo_id"], how="left"
        )
        .join(l5_home_stay_freq, on=["msisdn", "mo_id"], how="left")
        .join(l5_home_stay_unique_count, on=["msisdn", "mo_id"], how="left")
        .join(l5_work_home_same, on=["msisdn", "mo_id"], how="left")
        .join(l5_poi_freq, on=["msisdn", "mo_id"], how="left")
        .join(l5_staypoint_customer_profile, on=["msisdn", "mo_id"], how="left")
        .join(l5_staypoint_freq, on=["msisdn", "mo_id"], how="left")
        .join(l5_staypoint, on=["msisdn", "mo_id"], how="left")
        .join(l5_staypoint_unique_count, on=["msisdn", "mo_id"], how="left")
        .join(l5_work_stay, on=["msisdn", "mo_id"], how="left")
        .join(l5_work_stay_freq, on=["msisdn", "mo_id"], how="left")
        .join(l5_work_stay_unique_count, on=["msisdn", "mo_id"], how="left")
        .join(l5_work_stay_customer_profile, on=["msisdn", "mo_id"], how="left")
    )

    # df_msisdn = (
    #     l5_home_covid_stay_cnt.join(
    #         l5_covid_work_home_distance, on="msisdn", how="left"
    #     )
    #     .join(l5_covid_work_home_not_same, on="msisdn", how="left")
    #     .join(l5_work_covid_stay_cnt, on="msisdn", how="left")
    #     .join(l5_home_stay_covid_distance, on="msisdn", how="left")
    #     .join(l5_staypoint_covid_distance, on="msisdn", how="left")
    #     .join(l5_work_stay_covid_distance, on="msisdn", how="left")
    # )
    #
    # df_msisdn_join = df_msisdn_mo.join(df_msisdn, on=["msisdn"], how="left")

    start_date = get_start_date()
    end_date = get_end_date()

    df_month = get_month_id_bw_sd_ed(start_date, end_date, sla_date_parameter)

    df_master = df_msisdn_mo.join(df_month, on=["mo_id"])

    return df_master
