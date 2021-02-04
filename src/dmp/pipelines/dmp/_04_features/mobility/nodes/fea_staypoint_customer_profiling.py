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
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.window import Window


def mobility_staypoint_customer_profiling_features(
    stay_point: pyspark.sql.DataFrame,
    gdp_agg: pyspark.sql.DataFrame,
    gdp_per_capita_agg: pyspark.sql.DataFrame,
    hdi_agg: pyspark.sql.DataFrame,
    fb_pop_agg: pyspark.sql.DataFrame,
    urbancity_agg: pyspark.sql.DataFrame,
    gar_demographics_agg: pyspark.sql.DataFrame,
    external_bps: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """

    """

    stay_point_agg_window = Window.partitionBy(f.col("msisdn"), f.col("mo_id")).orderBy(
        f.col("agg_duration").desc()
    )

    stay_point_agg = stay_point.withColumn(
        "stay_rank", f.rank().over(stay_point_agg_window)
    ).where(f.col("stay_rank") <= 1)

    df_join = (
        stay_point_agg.selectExpr(
            "msisdn", "mo_id", "geohash", "province_name", "stay_rank"
        )
        .distinct()
        .join(gdp_agg, on="geohash", how="left")
        .join(gdp_per_capita_agg, on="geohash", how="left")
        .join(hdi_agg, on="geohash", how="left")
        .join(fb_pop_agg, on="geohash", how="left")
        .join(urbancity_agg, on="geohash", how="left")
        .join(gar_demographics_agg, on="geohash", how="left")
        .join(
            external_bps,
            f.lower(f.trim(f.col("province_name")))
            == f.lower(f.trim(f.col("province"))),
            how="left",
        )
        .withColumn("prefix", f.lit("fea_mobility_staypoint").cast(t.StringType()))
        .withColumn(
            "stay_rank_renamed",
            f.concat_ws("_", f.col("prefix"), f.col("stay_rank").cast(t.StringType())),
        )
        .groupBy("msisdn", "mo_id")
        # .pivot("stay_rank_renamed")
        .agg(
            f.first(f.col("gdp_ppp_2015_usd")).alias(
                "fea_mobility_staypoint_gdp_ppp_2015_usd"
            ),
            f.first(f.col("gdp_per_capita_ppp_2015_usd")).alias(
                "fea_mobility_staypoint_gdp_per_capita_ppp_2015_usd"
            ),
            f.first(f.col("hdi_2015")).alias("fea_mobility_staypoint_hdi_2015"),
            f.first(f.col("2015_pop_density")).alias(
                "fea_mobility_staypoint_2015_pop_density"
            ),
            f.first(f.col("2020_pop_density")).alias(
                "fea_mobility_staypoint_2020_pop_density"
            ),
            f.first(f.col("men_pop_density")).alias(
                "fea_mobility_staypoint_men_pop_density"
            ),
            f.first(f.col("women_pop_density")).alias(
                "fea_mobility_staypoint_women_pop_density"
            ),
            f.first(f.col("women_age_15_49_pop_density")).alias(
                "fea_mobility_staypoint_women_age_15_49_pop_density"
            ),
            f.first(f.col("children_under_5_pop_density")).alias(
                "fea_mobility_staypoint_children_under_5_pop_density"
            ),
            f.first(f.col("youth_15_24_pop_density")).alias(
                "fea_mobility_staypoint_youth_15_24_pop_density"
            ),
            f.first(f.col("elderly_60_plus_pop_density")).alias(
                "fea_mobility_staypoint_elderly_60_plus_pop_density"
            ),
            f.first(f.col("urbanicity")).alias("fea_mobility_staypoint_urbanicity"),
            f.first(f.col("emp_agr")).alias("fea_mobility_staypoint_emp_agr"),
            f.first(f.col("emp_gov")).alias("fea_mobility_staypoint_emp_gov"),
            f.first(f.col("emp_ind")).alias("fea_mobility_staypoint_emp_ind"),
            f.first(f.col("emp_ser")).alias("fea_mobility_staypoint_emp_ser"),
            f.first(f.col("ic_high")).alias("fea_mobility_staypoint_ic_high"),
            f.first(f.col("ic_mhg")).alias("fea_mobility_staypoint_ic_mhg"),
            f.first(f.col("ic_mlw")).alias("fea_mobility_staypoint_ic_mlw"),
            f.first(f.col("ic_low")).alias("fea_mobility_staypoint_ic_low"),
            f.first(f.col("tot_val")).alias("fea_mobility_staypoint_tot_val"),
            f.first(f.col("extreme_poverty_rate_urban_2018")).alias(
                "fea_mobility_staypoint_extreme_poverty_rate_urban_2018"
            ),
            f.first(f.col("extreme_poverty_rate_rural_2018")).alias(
                "fea_mobility_staypoint_extreme_poverty_rate_rural_2018"
            ),
            f.first(f.col("extreme_poverty_rate_total_2018")).alias(
                "fea_mobility_staypoint_extreme_poverty_rate_total_2018"
            ),
        )
    )

    return df_join
