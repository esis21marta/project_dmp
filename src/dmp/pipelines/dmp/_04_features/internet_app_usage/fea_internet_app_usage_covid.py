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
import datetime
import re
from functools import partial

import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from utils import (
    get_config_based_features,
    get_config_based_features_column_names,
    get_end_date,
    get_required_output_columns,
)
from utils import get_rolling_window as rolling_window
from utils import get_start_date, join_all, ratio_of_columns, sum_of_columns


def fea_int_app_usage_covid(
    df_fea_int_app_usage: pyspark.sql.DataFrame,
    df_fea_int_app_usage_pre_covid_fixed: pyspark.sql.DataFrame,
    df_fea_int_app_usage_covid_fixed: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: list,
) -> pyspark.sql.DataFrame:
    """
    Generate Internet App Usage for Covid-19 related features

    Args:
        df_fea_int_app_usage: Regular internet app usage features
        df_fea_int_app_usage_pre_covid_fixed: Internet App Usage pre-covid fixed/static features
        df_fea_int_app_usage_covid_fixed: Internet App Usage covid fixed/static features

    Returns:
        Dataframe which contain Internet App Usage for Covid-19 related features
    """
    first_weekstart = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    covid_first_weekstart = get_start_date(
        partition_column="weekstart", first_weekstart_params="covid_first_weekstart"
    ).strftime("%Y-%m-%d")
    covid_last_weekstart = get_end_date(
        partition_column="weekstart", last_weekstart_params="covid_last_weekstart"
    ).strftime("%Y-%m-%d")
    post_covid_first_weekstart = get_start_date(
        partition_column="weekstart",
        first_weekstart_params="post_covid_first_weekstart",
    ).strftime("%Y-%m-%d")
    post_covid_last_weekstart = get_end_date(
        partition_column="weekstart", last_weekstart_params="post_covid_last_weekstart"
    ).strftime("%Y-%m-%d")

    ### FEATURE SELECTION
    pre_with_covid_columns = [
        "fea_int_app_usage_ecommerce_accessed_apps_pre_with_covid_ratio",
        "fea_int_app_usage_ecommerce_data_vol_pre_with_covid_ratio",
        "fea_int_app_usage_ecommerce_accessed_num_weeks_pre_with_covid_ratio",
        "fea_int_app_usage_ecommerce_to_non_data_vol_pre_with_covid_ratio",
        "fea_int_app_usage_ecommerce_duration_pre_with_covid_ratio",
        "fea_int_app_usage_transportation_and_logistics_accessed_num_weeks_pre_with_covid_ratio",
        "fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_pre_with_covid_ratio",
        "fea_int_app_usage_fitness_data_vol_weekday_pre_with_covid_ratio",
        "fea_int_app_usage_fitness_accessed_apps_pre_with_covid_ratio",
        "fea_int_app_usage_fitness_data_vol_pre_with_covid_ratio",
        "fea_int_app_usage_fitness_accessed_num_weeks_pre_with_covid_ratio",
        "fea_int_app_usage_productivity_data_vol_pre_with_covid_ratio",
        "fea_int_app_usage_entertainment_data_vol_pre_with_covid_ratio",
    ]
    feature_columns = [
        *pre_with_covid_columns,
        "fea_int_app_usage_ecommerce_accessed_apps_pre_post_covid_ratio",
        "fea_int_app_usage_ecommerce_data_vol_pre_post_covid_ratio",
        "fea_int_app_usage_ecommerce_accessed_num_weeks_pre_post_covid_ratio",
        "fea_int_app_usage_ecommerce_to_non_data_vol_pre_post_covid_ratio",
        "fea_int_app_usage_ecommerce_duration_pre_post_covid_ratio",
        "fea_int_app_usage_transportation_and_logistics_accessed_num_weeks_pre_post_covid_ratio",
        "fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_pre_post_covid_ratio",
        "fea_int_app_usage_fitness_data_vol_weekday_pre_post_covid_ratio",
        "fea_int_app_usage_fitness_accessed_apps_pre_post_covid_ratio",
        "fea_int_app_usage_fitness_data_vol_pre_post_covid_ratio",
        "fea_int_app_usage_fitness_accessed_num_weeks_pre_post_covid_ratio",
        "fea_int_app_usage_productivity_data_vol_pre_post_covid_ratio",
        "fea_int_app_usage_entertainment_data_vol_pre_post_covid_ratio",
    ]

    required_feature_columns = get_required_output_columns(
        output_features=feature_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    if first_weekstart >= post_covid_last_weekstart:
        fea_df = df_fea_int_app_usage.select("msisdn", "weekstart").join(
            df_fea_int_app_usage_covid_fixed, ["msisdn"], how="left"
        )
        fea_df = fea_df.select(required_feature_columns)

        return fea_df
    else:
        config_feature_int_app = config_feature["internet_apps_usage"]

        ## Split dataframe into 3: covid, post-covid, after post-covid
        df_fea_int_app_usage_covid = df_fea_int_app_usage.filter(
            f.col("weekstart").between(covid_first_weekstart, covid_last_weekstart)
        )
        df_fea_int_app_usage_post_covid = df_fea_int_app_usage.filter(
            f.col("weekstart").between(
                post_covid_first_weekstart, post_covid_last_weekstart
            )
        )
        df_fea_int_app_usage = join_all(
            [
                df_fea_int_app_usage.filter(
                    f.col("weekstart") > post_covid_last_weekstart
                ),
                df_fea_int_app_usage_covid_fixed,
            ],
            on=["msisdn"],
            how="left",
        )

        ## Generate covid features
        df_fea_int_app_usage_covid = prepare_fea_int_app_usage_covid(
            df_fea_int_app_usage_covid, config_feature_int_app
        )
        df_fea_int_app_usage_post_covid = prepare_fea_int_app_usage_covid(
            df_fea_int_app_usage_post_covid, config_feature_int_app
        )
        df_fea_int_app_usage_covid = generate_fea_int_app_usage_covid_fixed(
            df_fea_int_app_usage_covid, "covid"
        )
        df_fea_int_app_usage_post_covid = generate_fea_int_app_usage_covid_fixed(
            df_fea_int_app_usage_post_covid, "post_covid"
        )

        df_covid = join_all(
            [df_fea_int_app_usage_covid, df_fea_int_app_usage_post_covid,],
            on=["msisdn", "weekstart"],
            how="outer",
        )
        df_covid = df_covid.join(
            df_fea_int_app_usage_pre_covid_fixed, ["msisdn"], how="left"
        )

        df_covid = fea_int_app_usage_covid_fixed(df_covid)

        df_fea_int_app_usage = df_fea_int_app_usage.select(required_feature_columns)
        df_covid = df_covid.select(required_feature_columns)

        fea_df = df_fea_int_app_usage.union(df_covid)

        # fillna for pre & covid features
        pre_with_covid_columns = [
            i for i in fea_df.columns if i in pre_with_covid_columns
        ]
        for column in pre_with_covid_columns:
            fea_df = fea_df.withColumn(
                column,
                f.when(
                    f.col(column).isNull(),
                    f.last(f.col(column), ignorenulls=True).over(
                        rolling_window(-1, oby="weekstart")
                    ),
                ).otherwise(f.col(column)),
            )

        return fea_df


def generate_fea_int_app_usage_covid_fixed(
    df: pyspark.sql.DataFrame, fea_suffix: str, filter_weekstart: str = None,
) -> pyspark.sql.DataFrame:
    """
    Generate Fixed/Static Internet App Usage Covid-19 features

    Args:
        df: Regular internet app usage features
        fea_suffix: Suffix of feature name, can be `pre_covid`, `covid`, or `post_covid`
        filter_weekstart: Specific weekstart to be used by dataframe

    Returns:
        Dataframe which contain static Internet App Usage Covid-19 features
    """
    if filter_weekstart:
        df = df.filter(f.col("weekstart") == filter_weekstart)

    df_fea_int_app_usage_covid = (
        df.withColumnRenamed(
            "fea_int_app_usage_ecommerce_accessed_apps_03m",
            f"fea_int_app_usage_ecommerce_accessed_apps_{fea_suffix}",
        )
        .withColumnRenamed(
            "fea_int_app_usage_covid_ratio_ecommerce_to_non_data_vol_03m",
            f"fea_int_app_usage_{fea_suffix}_ratio_ecommerce_to_non_data_vol",
        )
        .withColumnRenamed(
            "fea_int_app_usage_ecommerce_data_vol_03m",
            f"fea_int_app_usage_ecommerce_data_vol_{fea_suffix}",
        )
        .withColumnRenamed(
            "fea_int_app_usage_ecommerce_duration_03m",
            f"fea_int_app_usage_ecommerce_duration_{fea_suffix}",
        )
        .withColumnRenamed(
            "fea_int_app_usage_ecommerce_accessed_apps_num_weeks_03m",
            f"fea_int_app_usage_ecommerce_accessed_num_weeks_{fea_suffix}",
        )
        .withColumnRenamed(
            "fea_int_app_usage_transportation_and_logistics_accessed_apps_num_weeks_03m",
            f"fea_int_app_usage_transportation_and_logistics_accessed_num_weeks_{fea_suffix}",
        )
        .withColumnRenamed(
            "fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_ratio_03m",
            f"fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_ratio_{fea_suffix}",
        )
        .withColumnRenamed(
            "fea_int_app_usage_fitness_data_vol_weekday_03m",
            f"fea_int_app_usage_fitness_data_vol_weekday_{fea_suffix}",
        )
        .withColumnRenamed(
            "fea_int_app_usage_fitness_accessed_apps_03m",
            f"fea_int_app_usage_fitness_accessed_apps_{fea_suffix}",
        )
        .withColumnRenamed(
            "fea_int_app_usage_fitness_data_vol_03m",
            f"fea_int_app_usage_fitness_data_vol_{fea_suffix}",
        )
        .withColumnRenamed(
            "fea_int_app_usage_fitness_accessed_apps_num_weeks_03m",
            f"fea_int_app_usage_fitness_accessed_num_weeks_{fea_suffix}",
        )
        .withColumnRenamed(
            "fea_int_app_usage_covid_productivity_data_vol_03m",
            f"fea_int_app_usage_productivity_data_vol_{fea_suffix}",
        )
        .withColumnRenamed(
            "fea_int_app_usage_covid_entertainment_data_vol_03m",
            f"fea_int_app_usage_entertainment_data_vol_{fea_suffix}",
        )
    )
    df_fea_int_app_usage_covid = df_fea_int_app_usage_covid.select(
        "msisdn",
        "weekstart",
        f"fea_int_app_usage_ecommerce_accessed_apps_{fea_suffix}",
        f"fea_int_app_usage_{fea_suffix}_ratio_ecommerce_to_non_data_vol",
        f"fea_int_app_usage_ecommerce_data_vol_{fea_suffix}",
        f"fea_int_app_usage_ecommerce_duration_{fea_suffix}",
        f"fea_int_app_usage_ecommerce_accessed_num_weeks_{fea_suffix}",
        f"fea_int_app_usage_transportation_and_logistics_accessed_num_weeks_{fea_suffix}",
        f"fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_ratio_{fea_suffix}",
        f"fea_int_app_usage_fitness_data_vol_weekday_{fea_suffix}",
        f"fea_int_app_usage_fitness_accessed_apps_{fea_suffix}",
        f"fea_int_app_usage_fitness_data_vol_{fea_suffix}",
        f"fea_int_app_usage_fitness_accessed_num_weeks_{fea_suffix}",
        f"fea_int_app_usage_productivity_data_vol_{fea_suffix}",
        f"fea_int_app_usage_entertainment_data_vol_{fea_suffix}",
    )

    return df_fea_int_app_usage_covid


def fea_int_app_usage_covid_fixed(
    df_covid: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Calculates features on Internet App Usage Covid-19

    Args:
        df_covid: Internet App Usage features during pre and post covid period

    Returns:
        df_covid: Dataframe with following features
            - msisdn: Unique Id
            - fea_int_app_usage_ecommerce_accessed_apps_pre_covid: Pre-covid ecommerce accessed apps
            - fea_int_app_usage_ecommerce_data_vol_pre_covid: Pre-covid ecommerce data volume
            - fea_int_app_usage_ecommerce_accessed_apps_pre_with_covid_ratio: Ratio between pre and covid for ecommerce accessed apps
            - fea_int_app_usage_ecommerce_accessed_apps_pre_post_covid_ratio: Ratio between pre and post covid for ecommerce accessed apps
            - fea_int_app_usage_ecommerce_data_vol_pre_with_covid_ratio: Ratio between pre and covid for ecommerce data volume
            - fea_int_app_usage_ecommerce_data_vol_pre_post_covid_ratio: Ratio between pre and post covid for ecommerce data volume
            - fea_int_app_usage_ecommerce_accessed_num_weeks_pre_with_covid_ratio: Ratio between pre and covid for number of weeks the customer accessed ecommerce apps
            - fea_int_app_usage_ecommerce_accessed_num_weeks_pre_post_covid_ratio: Ratio between pre and post covid for number of weeks the customer accessed ecommerce apps
            - fea_int_app_usage_ecommerce_to_non_data_vol_pre_with_covid_ratio: Ratio between pre and covid for ecommerce vs non-ecommerce data volume
            - fea_int_app_usage_ecommerce_to_non_data_vol_pre_post_covid_ratio: Ratio between pre and post covid for ecommerce vs non-ecommerce data volume
            - fea_int_app_usage_ecommerce_duration_pre_with_covid_ratio: Ratio between pre and covid for duration on ecommerce access
            - fea_int_app_usage_ecommerce_duration_pre_post_covid_ratio: Ratio between pre and post covid for duration on ecommerce access
            - fea_int_app_usage_transportation_and_logistics_accessed_num_weeks_pre_with_covid_ratio: Ratio between pre and covid for number of weeks the customer accessed transportation and logistics apps
            - fea_int_app_usage_transportation_and_logistics_accessed_num_weeks_pre_post_covid_ratio: Ratio between pre and post covid for number of weeks the customer accessed transportation and logistics apps
            - fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_pre_with_covid_ratio: Ratio between pre and covid for transportation and logistics data volume weekday vs weekend
            - fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_pre_post_covid_ratio: Ratio between pre and post covid for transportation and logistics data volume weekday vs weekend
            - fea_int_app_usage_fitness_data_vol_weekday_pre_with_covid_ratio: Ratio between pre and covid for fitness data volume on weekday
            - fea_int_app_usage_fitness_data_vol_weekday_pre_post_covid_ratio: Ratio between pre and post covid for fitness data volume on weekday
            - fea_int_app_usage_fitness_accessed_apps_pre_with_covid_ratio: Ratio between pre and covid for fitness accessed apps
            - fea_int_app_usage_fitness_accessed_apps_pre_post_covid_ratio: Ratio between pre and post covid for fitness accessed apps
            - fea_int_app_usage_fitness_data_vol_pre_with_covid_ratio: Ratio between pre and covid for fitness data volume
            - fea_int_app_usage_fitness_data_vol_pre_post_covid_ratio: Ratio between pre and post covid for fitness data volume
            - fea_int_app_usage_fitness_accessed_num_weeks_pre_with_covid_ratio: Ratio between pre and covid for number of weeks the customer accessed fitness apps
            - fea_int_app_usage_fitness_accessed_num_weeks_pre_post_covid_ratio: Ratio between pre and post covid for number of weeks the customer accessed fitness apps
            - fea_int_app_usage_productivity_data_vol_pre_with_covid_ratio: Ratio between pre and covid for productivity apps data volume
            - fea_int_app_usage_productivity_data_vol_pre_post_covid_ratio: Ratio between pre and post covid for productivity apps data volume
            - fea_int_app_usage_entertainment_data_vol_pre_post_covid_ratio: Ratio between pre and covid for entertainment apps data volume
            - fea_int_app_usage_entertainment_data_vol_pre_with_covid_ratio: Ratio between pre and post covid for entertainment apps data volume

    """
    df_covid = (
        df_covid.withColumn(
            "fea_int_app_usage_ecommerce_accessed_apps_pre_with_covid_ratio",  # FID 1
            f.col("fea_int_app_usage_ecommerce_accessed_apps_covid")
            / (
                f.col("fea_int_app_usage_ecommerce_accessed_apps_covid")
                + f.col("fea_int_app_usage_ecommerce_accessed_apps_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_ecommerce_accessed_apps_pre_post_covid_ratio",  # FID 1
            f.col("fea_int_app_usage_ecommerce_accessed_apps_post_covid")
            / (
                f.col("fea_int_app_usage_ecommerce_accessed_apps_post_covid")
                + f.col("fea_int_app_usage_ecommerce_accessed_apps_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_ecommerce_data_vol_pre_with_covid_ratio",  # FID 2
            f.col("fea_int_app_usage_ecommerce_data_vol_covid")
            / (
                f.col("fea_int_app_usage_ecommerce_data_vol_covid")
                + f.col("fea_int_app_usage_ecommerce_data_vol_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_ecommerce_data_vol_pre_post_covid_ratio",  # FID 2
            f.col("fea_int_app_usage_ecommerce_data_vol_post_covid")
            / (
                f.col("fea_int_app_usage_ecommerce_data_vol_post_covid")
                + f.col("fea_int_app_usage_ecommerce_data_vol_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_ecommerce_accessed_num_weeks_pre_with_covid_ratio",  # FID 3
            f.col("fea_int_app_usage_ecommerce_accessed_num_weeks_covid")
            / (
                f.col("fea_int_app_usage_ecommerce_accessed_num_weeks_covid")
                + f.col("fea_int_app_usage_ecommerce_accessed_num_weeks_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_ecommerce_accessed_num_weeks_pre_post_covid_ratio",  # FID 3
            f.col("fea_int_app_usage_ecommerce_accessed_num_weeks_post_covid")
            / (
                f.col("fea_int_app_usage_ecommerce_accessed_num_weeks_post_covid")
                + f.col("fea_int_app_usage_ecommerce_accessed_num_weeks_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_ecommerce_to_non_data_vol_pre_with_covid_ratio",  # FID 7
            f.col("fea_int_app_usage_covid_ratio_ecommerce_to_non_data_vol")
            / (
                f.col("fea_int_app_usage_covid_ratio_ecommerce_to_non_data_vol")
                + f.col("fea_int_app_usage_pre_covid_ratio_ecommerce_to_non_data_vol")
            ),
        )
        .withColumn(
            "fea_int_app_usage_ecommerce_to_non_data_vol_pre_post_covid_ratio",  # FID 7
            f.col("fea_int_app_usage_post_covid_ratio_ecommerce_to_non_data_vol")
            / (
                f.col("fea_int_app_usage_post_covid_ratio_ecommerce_to_non_data_vol")
                + f.col("fea_int_app_usage_pre_covid_ratio_ecommerce_to_non_data_vol")
            ),
        )
        .withColumn(
            "fea_int_app_usage_ecommerce_duration_pre_with_covid_ratio",  # FID 10
            f.col("fea_int_app_usage_ecommerce_duration_covid")
            / (
                f.col("fea_int_app_usage_ecommerce_duration_covid")
                + f.col("fea_int_app_usage_ecommerce_duration_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_ecommerce_duration_pre_post_covid_ratio",  # FID 10
            f.col("fea_int_app_usage_ecommerce_duration_post_covid")
            / (
                f.col("fea_int_app_usage_ecommerce_duration_post_covid")
                + f.col("fea_int_app_usage_ecommerce_duration_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_transportation_and_logistics_accessed_num_weeks_pre_with_covid_ratio",  # FID 23
            f.col(
                "fea_int_app_usage_transportation_and_logistics_accessed_num_weeks_covid"
            )
            / (
                f.col(
                    "fea_int_app_usage_transportation_and_logistics_accessed_num_weeks_covid"
                )
                + f.col(
                    "fea_int_app_usage_transportation_and_logistics_accessed_num_weeks_pre_covid"
                )
            ),
        )
        .withColumn(
            "fea_int_app_usage_transportation_and_logistics_accessed_num_weeks_pre_post_covid_ratio",  # FID 23
            f.col(
                "fea_int_app_usage_transportation_and_logistics_accessed_num_weeks_post_covid"
            )
            / (
                f.col(
                    "fea_int_app_usage_transportation_and_logistics_accessed_num_weeks_post_covid"
                )
                + f.col(
                    "fea_int_app_usage_transportation_and_logistics_accessed_num_weeks_pre_covid"
                )
            ),
        )
        .withColumn(
            "fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_pre_with_covid_ratio",  # FID 30
            f.col(
                "fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_ratio_covid"
            )
            / (
                f.col(
                    "fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_ratio_covid"
                )
                + f.col(
                    "fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_ratio_pre_covid"
                )
            ),
        )
        .withColumn(
            "fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_pre_post_covid_ratio",  # FID 30
            f.col(
                "fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_ratio_post_covid"
            )
            / (
                f.col(
                    "fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_ratio_post_covid"
                )
                + f.col(
                    "fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_ratio_pre_covid"
                )
            ),
        )
        .withColumn(
            "fea_int_app_usage_fitness_data_vol_weekday_pre_with_covid_ratio",  # FID 35
            f.col("fea_int_app_usage_fitness_data_vol_weekday_covid")
            / (
                f.col("fea_int_app_usage_fitness_data_vol_weekday_covid")
                + f.col("fea_int_app_usage_fitness_data_vol_weekday_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_fitness_data_vol_weekday_pre_post_covid_ratio",  # FID 35
            f.col("fea_int_app_usage_fitness_data_vol_weekday_post_covid")
            / (
                f.col("fea_int_app_usage_fitness_data_vol_weekday_post_covid")
                + f.col("fea_int_app_usage_fitness_data_vol_weekday_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_fitness_accessed_apps_pre_with_covid_ratio",  # FID 36
            f.col("fea_int_app_usage_fitness_accessed_apps_covid")
            / (
                f.col("fea_int_app_usage_fitness_accessed_apps_covid")
                + f.col("fea_int_app_usage_fitness_accessed_apps_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_fitness_accessed_apps_pre_post_covid_ratio",  # FID 36
            f.col("fea_int_app_usage_fitness_accessed_apps_post_covid")
            / (
                f.col("fea_int_app_usage_fitness_accessed_apps_post_covid")
                + f.col("fea_int_app_usage_fitness_accessed_apps_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_fitness_data_vol_pre_with_covid_ratio",  # FID 37
            f.col("fea_int_app_usage_fitness_data_vol_covid")
            / (
                f.col("fea_int_app_usage_fitness_data_vol_covid")
                + f.col("fea_int_app_usage_fitness_data_vol_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_fitness_data_vol_pre_post_covid_ratio",  # FID 37
            f.col("fea_int_app_usage_fitness_data_vol_post_covid")
            / (
                f.col("fea_int_app_usage_fitness_data_vol_post_covid")
                + f.col("fea_int_app_usage_fitness_data_vol_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_fitness_accessed_num_weeks_pre_with_covid_ratio",  # FID 38
            f.col("fea_int_app_usage_fitness_accessed_num_weeks_covid")
            / (
                f.col("fea_int_app_usage_fitness_accessed_num_weeks_covid")
                + f.col("fea_int_app_usage_fitness_accessed_num_weeks_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_fitness_accessed_num_weeks_pre_post_covid_ratio",  # FID 38
            f.col("fea_int_app_usage_fitness_accessed_num_weeks_post_covid")
            / (
                f.col("fea_int_app_usage_fitness_accessed_num_weeks_post_covid")
                + f.col("fea_int_app_usage_fitness_accessed_num_weeks_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_productivity_data_vol_pre_post_covid_ratio",  # FID 20
            f.col("fea_int_app_usage_productivity_data_vol_post_covid")
            / (
                f.col("fea_int_app_usage_productivity_data_vol_post_covid")
                + f.col("fea_int_app_usage_productivity_data_vol_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_productivity_data_vol_pre_with_covid_ratio",  # FID 20
            f.col("fea_int_app_usage_productivity_data_vol_covid")
            / (
                f.col("fea_int_app_usage_productivity_data_vol_covid")
                + f.col("fea_int_app_usage_productivity_data_vol_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_entertainment_data_vol_pre_post_covid_ratio",  # FID 33
            f.col("fea_int_app_usage_entertainment_data_vol_post_covid")
            / (
                f.col("fea_int_app_usage_entertainment_data_vol_post_covid")
                + f.col("fea_int_app_usage_entertainment_data_vol_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_app_usage_entertainment_data_vol_pre_with_covid_ratio",  # FID 33
            f.col("fea_int_app_usage_entertainment_data_vol_covid")
            / (
                f.col("fea_int_app_usage_entertainment_data_vol_covid")
                + f.col("fea_int_app_usage_entertainment_data_vol_pre_covid")
            ),
        )
    )
    return df_covid


def fea_int_app_usage_covid_fixed_all(
    df_fea_int_app_usage_pre_covid_fixed: pyspark.sql.DataFrame,
    df_fea_int_app_usage_covid_fixed: pyspark.sql.DataFrame,
    df_fea_int_app_usage_post_covid_fixed: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: list,
) -> pyspark.sql.DataFrame:
    """
    Final form of Internet App Usage Fixed/Static Covid-19 features

    Args:
        df_fea_int_app_usage_pre_covid_fixed: Internet App Usage pre-covid fixed/static features
        df_fea_int_app_usage_covid_fixed: Internet App Usage covid fixed/static features
        df_fea_int_app_usage_post_covid_fixed: Internet App Usage post covid fixed/static features

    Returns:
        Dataframe which contain Internet App Usage for Covid-19 related features
    """
    df_covid = join_all(
        [
            df_fea_int_app_usage_pre_covid_fixed,
            df_fea_int_app_usage_covid_fixed,
            df_fea_int_app_usage_post_covid_fixed,
        ],
        on=["msisdn"],
        how="outer",
    )
    df_covid = fea_int_app_usage_covid_fixed(df_covid)

    ### FEATURE SELECTION
    feature_columns = [
        "fea_int_app_usage_ecommerce_accessed_apps_pre_with_covid_ratio",
        "fea_int_app_usage_ecommerce_data_vol_pre_with_covid_ratio",
        "fea_int_app_usage_ecommerce_accessed_num_weeks_pre_with_covid_ratio",
        "fea_int_app_usage_ecommerce_to_non_data_vol_pre_with_covid_ratio",
        "fea_int_app_usage_ecommerce_duration_pre_with_covid_ratio",
        "fea_int_app_usage_transportation_and_logistics_accessed_num_weeks_pre_with_covid_ratio",
        "fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_pre_with_covid_ratio",
        "fea_int_app_usage_fitness_data_vol_weekday_pre_with_covid_ratio",
        "fea_int_app_usage_fitness_accessed_apps_pre_with_covid_ratio",
        "fea_int_app_usage_fitness_data_vol_pre_with_covid_ratio",
        "fea_int_app_usage_fitness_accessed_num_weeks_pre_with_covid_ratio",
        "fea_int_app_usage_productivity_data_vol_pre_with_covid_ratio",
        "fea_int_app_usage_entertainment_data_vol_pre_with_covid_ratio",
        "fea_int_app_usage_ecommerce_accessed_apps_pre_post_covid_ratio",
        "fea_int_app_usage_ecommerce_data_vol_pre_post_covid_ratio",
        "fea_int_app_usage_ecommerce_accessed_num_weeks_pre_post_covid_ratio",
        "fea_int_app_usage_ecommerce_to_non_data_vol_pre_post_covid_ratio",
        "fea_int_app_usage_ecommerce_duration_pre_post_covid_ratio",
        "fea_int_app_usage_transportation_and_logistics_accessed_num_weeks_pre_post_covid_ratio",
        "fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_pre_post_covid_ratio",
        "fea_int_app_usage_fitness_data_vol_weekday_pre_post_covid_ratio",
        "fea_int_app_usage_fitness_accessed_apps_pre_post_covid_ratio",
        "fea_int_app_usage_fitness_data_vol_pre_post_covid_ratio",
        "fea_int_app_usage_fitness_accessed_num_weeks_pre_post_covid_ratio",
        "fea_int_app_usage_productivity_data_vol_pre_post_covid_ratio",
        "fea_int_app_usage_entertainment_data_vol_pre_post_covid_ratio",
    ]

    required_feature_columns = get_required_output_columns(
        output_features=feature_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn"],
    )

    fea_df = df_covid.select(required_feature_columns)

    return fea_df


def fea_int_app_usage_covid_trend(
    df_fea_int_app_usage: pyspark.sql.DataFrame,
    df_fea_int_app_usage_covid_fixed: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: list,
) -> pyspark.sql.DataFrame:
    """
    Calculates features on Internet App Usage Covid-19

    Args:
        df_fea_int_app_usage_covid: Internet App Usage weekly aggregation during pre and post covid period

    Returns:
        fea_df: Dataframe with following features
            - msisdn: Unique Id
            - weekstart: Start of week
            - fea_int_app_usage_ecommerce_accessed_apps_trend: Weekly/monthly trend for ecommerce accessed apps
            - fea_int_app_usage_ecommerce_data_vol_trend: Weekly/monthly trend for ecommerce data volume
            - fea_int_app_usage_ecommerce_accessed_apps_num_weeks_trend: Weekly/monthly trend for ecommerce num of weeks accessed
            - fea_int_app_usage_ecommerce_accessed_apps_ratio_pre_covid_with_last: Ratio of current weeks to pre-covid period for for ecommerce accessed apps
            - fea_int_app_usage_ecommerce_data_vol_ratio_pre_covid_with_last: Ratio of current weeks to pre-covid period for for ecommerce data volume
            - fea_int_app_usage_ecommerce_to_non_ecommerce_data_vol_ratio: Ratio of ecommerce to non-ecommerce data volume
            - fea_int_app_usage_ecommerce_to_non_ecommerce_data_vol_trend: Weekly/monthly trend for ecommerce ecommerce to non-ecommerce data volume
            - fea_int_app_usage_ecommerce_duration_trend: Weekly/monthly trend for ecommerce duration
            - fea_int_app_usage_transportation_and_logistics_accessed_apps_num_weeks_trend: Weekly/monthly trend for transportation and logistics num of weeks accessed
            - fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_trend: Weekly/monthly trend for transportation and logistics data volume
            - fea_int_app_usage_fitness_accessed_apps_trend: Weekly/monthly trend for fitness accessed apps
            - fea_int_app_usage_fitness_data_vol_trend: Weekly/monthly trend for fitness data volume
            - fea_int_app_usage_fitness_accessed_apps_num_weeks_trend: Weekly/monthly trend for fitness num of weeks accessed

    """
    config_feature_int_app = config_feature["internet_apps_usage"]
    non_ecom_data_vol_cols = list(
        set(
            [
                f"{i[:-4]}_" + "{period_string}"
                for i in df_fea_int_app_usage.columns
                if ("data_vol" in i)
                and ("ecommerce" not in i)
                and ("fea_int_app_usage" in i)
                and ("_week" not in i)
                and ("_prev" not in i)
            ]
        )
    )

    df_fea_int_app_usage_covid_fixed = df_fea_int_app_usage_covid_fixed.select(
        "msisdn",
        "fea_int_app_usage_ecommerce_accessed_apps_pre_covid",
        "fea_int_app_usage_ecommerce_data_vol_pre_covid",
    )

    ### FEATURE IMPLEMENTATION TREND
    df_fea_int_app_usage = df_fea_int_app_usage.join(
        df_fea_int_app_usage_covid_fixed, ["msisdn"], how="left"
    )
    df_fea_int_app_usage = get_config_based_features(
        df=df_fea_int_app_usage,
        feature_config=config_feature_int_app[
            "fea_int_app_usage_ecommerce_accessed_apps_trend"  # FID 1
        ],
        column_expression=ratio_of_columns(
            "fea_int_app_usage_ecommerce_accessed_apps_{period_string}",
            "fea_int_app_usage_ecommerce_accessed_apps_01w",
        ),
    )

    df_fea_int_app_usage = get_config_based_features(
        df=df_fea_int_app_usage,
        feature_config=config_feature_int_app[
            "fea_int_app_usage_ecommerce_data_vol_trend"  # FID 2
        ],
        column_expression=ratio_of_columns(
            "fea_int_app_usage_ecommerce_data_vol_{period_string}",
            "fea_int_app_usage_ecommerce_data_vol_01w",
        ),
    )

    df_fea_int_app_usage = get_config_based_features(
        df=df_fea_int_app_usage,
        feature_config=config_feature_int_app[
            "fea_int_app_usage_ecommerce_accessed_apps_num_weeks_trend"  # FID 3
        ],
        column_expression=ratio_of_columns(
            "fea_int_app_usage_ecommerce_accessed_apps_num_weeks_{period_string}",
            "fea_int_app_usage_ecommerce_accessed_apps_num_weeks_01w",
        ),
    )
    df_fea_int_app_usage = get_config_based_features(
        df=df_fea_int_app_usage,
        feature_config=config_feature_int_app[
            "fea_int_app_usage_ecommerce_accessed_apps_ratio_pre_covid_with_last"  # FID 4
        ],
        column_expression=ratio_of_columns(
            "fea_int_app_usage_ecommerce_accessed_apps_{period_string}",
            "fea_int_app_usage_ecommerce_accessed_apps_pre_covid",
        ),
    )
    df_fea_int_app_usage = get_config_based_features(
        df=df_fea_int_app_usage,
        feature_config=config_feature_int_app[
            "fea_int_app_usage_ecommerce_data_vol_ratio_pre_covid_with_last"  # FID 6
        ],
        column_expression=ratio_of_columns(
            "fea_int_app_usage_ecommerce_data_vol_{period_string}",
            "fea_int_app_usage_ecommerce_data_vol_pre_covid",
        ),
    )
    df_fea_int_app_usage = get_config_based_features(
        df=df_fea_int_app_usage,
        feature_config=config_feature_int_app[
            "fea_int_app_usage_non_ecommerce_data_vol"
        ],
        column_expression=sum_of_columns(non_ecom_data_vol_cols),
    )
    df_fea_int_app_usage = get_config_based_features(
        df=df_fea_int_app_usage,
        feature_config=config_feature_int_app[
            "fea_int_app_usage_ecommerce_to_non_ecommerce_data_vol_ratio"  # FID 7
        ],
        column_expression=ratio_of_columns(
            "fea_int_app_usage_ecommerce_data_vol_{period_string}",
            "fea_int_app_usage_non_ecommerce_data_vol_{period_string}",
        ),
    )
    df_fea_int_app_usage = get_config_based_features(
        df=df_fea_int_app_usage,
        feature_config=config_feature_int_app[
            "fea_int_app_usage_ecommerce_to_non_ecommerce_data_vol_trend"  # FID 7
        ],
        column_expression=ratio_of_columns(
            "fea_int_app_usage_ecommerce_to_non_ecommerce_data_vol_ratio_{period_string}",
            "fea_int_app_usage_ecommerce_to_non_ecommerce_data_vol_ratio_01w",
        ),
    )
    df_fea_int_app_usage = get_config_based_features(
        df=df_fea_int_app_usage,
        feature_config=config_feature_int_app[
            "fea_int_app_usage_ecommerce_duration_trend"  # FID 7
        ],
        column_expression=ratio_of_columns(
            "fea_int_app_usage_ecommerce_duration_{period_string}",
            "fea_int_app_usage_ecommerce_duration_01w",
        ),
    )
    df_fea_int_app_usage = get_config_based_features(
        df=df_fea_int_app_usage,
        feature_config=config_feature_int_app[
            "fea_int_app_usage_transportation_and_logistics_accessed_apps_num_weeks_trend"  # FID 23
        ],
        column_expression=ratio_of_columns(
            "fea_int_app_usage_transportation_and_logistics_accessed_apps_num_weeks_{period_string}",
            "fea_int_app_usage_transportation_and_logistics_accessed_apps_num_weeks_01w",
        ),
    )
    df_fea_int_app_usage = get_config_based_features(
        df=df_fea_int_app_usage,
        feature_config=config_feature_int_app[
            "fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_trend"  # FID 30
        ],
        column_expression=ratio_of_columns(
            "fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_ratio_{period_string}",
            "fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_ratio_01w",
        ),
    )
    df_fea_int_app_usage = get_config_based_features(
        df=df_fea_int_app_usage,
        feature_config=config_feature_int_app[
            "fea_int_app_usage_fitness_accessed_apps_trend"  # FID 36
        ],
        column_expression=ratio_of_columns(
            "fea_int_app_usage_fitness_accessed_apps_{period_string}",
            "fea_int_app_usage_fitness_accessed_apps_01w",
        ),
    )
    df_fea_int_app_usage = get_config_based_features(
        df=df_fea_int_app_usage,
        feature_config=config_feature_int_app[
            "fea_int_app_usage_fitness_data_vol_trend"  # FID 37
        ],
        column_expression=ratio_of_columns(
            "fea_int_app_usage_fitness_data_vol_{period_string}",
            "fea_int_app_usage_fitness_data_vol_01w",
        ),
    )
    df_fea_int_app_usage = get_config_based_features(
        df=df_fea_int_app_usage,
        feature_config=config_feature_int_app[
            "fea_int_app_usage_fitness_accessed_apps_num_weeks_trend"  # FID 38
        ],
        column_expression=ratio_of_columns(
            "fea_int_app_usage_fitness_accessed_apps_num_weeks_{period_string}",
            "fea_int_app_usage_fitness_accessed_apps_num_weeks_01w",
        ),
    )

    ### FEATURE SELECTION
    feature_columns = get_config_based_features_column_names(
        config_feature_int_app["fea_int_app_usage_ecommerce_accessed_apps_trend"],
        config_feature_int_app["fea_int_app_usage_ecommerce_data_vol_trend"],
        config_feature_int_app[
            "fea_int_app_usage_ecommerce_accessed_apps_num_weeks_trend"
        ],
        config_feature_int_app[
            "fea_int_app_usage_ecommerce_accessed_apps_ratio_pre_covid_with_last"
        ],
        config_feature_int_app[
            "fea_int_app_usage_ecommerce_data_vol_ratio_pre_covid_with_last"
        ],
        config_feature_int_app[
            "fea_int_app_usage_ecommerce_to_non_ecommerce_data_vol_ratio"
        ],
        config_feature_int_app[
            "fea_int_app_usage_ecommerce_to_non_ecommerce_data_vol_trend"
        ],
        config_feature_int_app["fea_int_app_usage_ecommerce_duration_trend"],
        config_feature_int_app[
            "fea_int_app_usage_transportation_and_logistics_accessed_apps_num_weeks_trend"
        ],
        config_feature_int_app[
            "fea_int_app_usage_transportation_and_logistics_data_vol_weekday_weekend_trend"
        ],
        config_feature_int_app["fea_int_app_usage_fitness_accessed_apps_trend"],
        config_feature_int_app["fea_int_app_usage_fitness_data_vol_trend"],
        config_feature_int_app[
            "fea_int_app_usage_fitness_accessed_apps_num_weeks_trend"
        ],
    )

    required_feature_columns = get_required_output_columns(
        output_features=feature_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    fea_df = df_fea_int_app_usage.select(required_feature_columns)

    #### FILTER WEEKSTART
    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    fea_df = fea_df.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )

    return fea_df


def fea_internet_app_usage_covid(
    first_weekstart_params: str, last_weekstart_params: str
):
    def _fea_internet_app_usage_covid(
        df_app_usage_weekly: pyspark.sql.DataFrame,
        feature_mode: str,
        required_output_features: list,
        shuffle_partitions: int,
    ) -> pyspark.sql.DataFrame:
        """
        Calculates the internet app usage for each msisdn:

        Args:
            df_app_usage_weekly: Internet App Weekly Aggregated data (cadence: msisdn, weekstart, feature)
            feature_mode: Feature Mode [All OR List]
            required_output_features: Feature List

        Returns:
            fea_df: DataFrame with features, app internet usage
                - msisdn: Unique Id
                - weekstart: Weekly observation point
                - fea_int_app_usage_*_data_vol_*d: Data used in the window
                - fea_int_app_usage_*_accessed_apps_*d : App count used in the window
                - fea_int_app_usage_*_duration_*d : Duration used in the window
        """

        # Get All App Category which are required for the feature generations from required_output_features
        req_app_category = set()
        if "feature_list" == feature_mode:
            required_output_features = [
                fea
                for fea in required_output_features
                if fea.startswith("fea_int_app_usage_")
                and (
                    ("_data_vol_" in fea)
                    or ("_accessed_apps_" in fea)
                    or ("_duration_" in fea)
                )
            ]
            for fea in required_output_features:
                for fea_type in [
                    "_data_vol_",
                    "_accessed_apps_",
                    "_duration_",
                ]:
                    if fea_type in fea:
                        req_app_category.add(
                            re.search(f"fea_int_app_usage_(.*){fea_type}", fea).group(1)
                        )

            # These 4 categories are required in Social Media Features
            req_app_category.add("facebook")
            req_app_category.add("instagram")
            req_app_category.add("linkedin")
            req_app_category.add("youtube")

        if len(req_app_category) > 0:
            df_app_usage_weekly = df_app_usage_weekly.filter(
                f.col("category").isin(req_app_category)
            )

        # Partial get rolling window
        get_rolling_window = partial(
            rolling_window, oby="weekstart", optional_keys=["category"]
        )

        # Updated Shuffle Partitions
        spark = SparkSession.builder.getOrCreate()
        spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

        # Aggregating over Features
        df_app_usage_weekly = (
            df_app_usage_weekly.withColumn(
                "data_vol_01w", f.col("volume_in") + f.col("volume_out")
            )
            .withColumn("category", f.trim(f.lower(f.col("category"))))
            .withColumn(
                "data_vol_weekend_01w",
                f.col("4g_volume_in_weekend") + f.col("4g_volume_out_weekend"),
            )
            .withColumn(
                "data_vol_weekday_01w",
                f.col("4g_volume_in_weekday") + f.col("4g_volume_out_weekday"),
            )
        )

        # Adding accessed_apps_num_weeks
        df_app_usage_weekly = df_app_usage_weekly.withColumn(
            "accessed_apps_01w",
            f.when(
                f.col("accessed_app").isNotNull(), f.size(f.col("accessed_app"))
            ).otherwise(0),
        ).withColumn(
            "accessed_apps_num_weeks_01w",
            f.when(f.col("accessed_apps_01w") > 0, 1).otherwise(0),
        )
        # Calculating Features
        fea_df = (
            df_app_usage_weekly.withColumn("data_vol_01w", f.col("data_vol_01w"))
            .withColumn(
                "data_vol_03m",
                f.sum(f.col("data_vol_01w")).over(get_rolling_window(91)),
            )
            .withColumn(
                "data_vol_weekend_03m",
                f.sum(f.col("data_vol_weekend_01w")).over(get_rolling_window(91)),
            )
            .withColumn(
                "data_vol_weekday_03m",
                f.sum(f.col("data_vol_weekday_01w")).over(get_rolling_window(91)),
            )
            .withColumn(
                "data_vol_weekday_weekend_ratio_03m",
                f.col("data_vol_weekend_03m")
                / (f.col("data_vol_weekday_03m") + f.col("data_vol_weekend_03m")),
            )
            .withColumn(
                "duration_01w",
                f.col("4g_duration_weekday") + f.col("4g_duration_weekend"),
            )
            .withColumn(
                "duration_03m",
                f.sum(f.col("duration_01w")).over(get_rolling_window(91)),
            )
            .withColumn(
                "accessed_apps_03m",
                f.size(
                    f.array_distinct(
                        f.flatten(
                            f.collect_set(f.col("accessed_app")).over(
                                get_rolling_window(91)
                            )
                        )
                    )
                ),
            )
            .withColumn(
                "accessed_apps_num_weeks_03m",
                f.sum(f.col("accessed_apps_num_weeks_01w")).over(
                    get_rolling_window(91)
                ),
            )
            .select(
                "weekstart",
                "msisdn",
                "category",
                "data_vol_03m",
                "data_vol_weekday_03m",
                "data_vol_weekday_weekend_ratio_03m",
                "duration_03m",
                "accessed_apps_03m",
                "accessed_apps_num_weeks_03m",
            )
        )

        first_week_start = get_start_date(
            partition_column="weekstart", first_weekstart_params=first_weekstart_params
        ).strftime("%Y-%m-%d")
        last_week_start = get_end_date(
            partition_column="weekstart", last_weekstart_params=last_weekstart_params
        ).strftime("%Y-%m-%d")

        return fea_df.filter(
            f.col("weekstart").between(first_week_start, last_week_start)
        )

    return _fea_internet_app_usage_covid


def pivot_features_covid(
    fea_df: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: list,
    shuffle_partitions: int,
) -> pyspark.sql.DataFrame:
    """
    :param fea_df: Features dataframe
    :param  feature_mode: Feature Mode [All OR List]
    :param  required_output_features: Feature List
    :return: Pivoted features dataframe
    """

    if "feature_list" == feature_mode:
        required_output_features = [
            fea[18:]
            for fea in required_output_features
            if fea.startswith("fea_int_app_usage_")
            and (
                ("_data_vol_" in fea)
                or ("_accessed_apps_" in fea)
                or ("_duration_" in fea)
            )
        ]
        extra_columns_to_keep = [
            "msisdn",
            "weekstart",
            "instagram_data_vol_03m",
            "linkedin_data_vol_03m",
            "youtube_data_vol_03m",
        ]
        required_output_features.extend(extra_columns_to_keep)

    # Updated Shuffle Partitions
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

    # Pivoting Features
    fea_df = (
        fea_df.groupBy(f.col("weekstart"), f.col("msisdn"))
        .pivot("category")
        .agg(
            f.coalesce(f.sum(f.col("data_vol_03m")), f.lit(0)).alias("data_vol_03m"),
            f.coalesce(f.sum(f.col("data_vol_weekday_03m")), f.lit(0)).alias(
                "data_vol_weekday_03m"
            ),
            f.avg(f.col("data_vol_weekday_weekend_ratio_03m")).alias(
                "data_vol_weekday_weekend_ratio_03m"
            ),
            f.coalesce(f.sum(f.col("duration_03m")), f.lit(0)).alias("duration_03m"),
            f.coalesce(f.sum(f.col("accessed_apps_03m")), f.lit(0)).alias(
                "accessed_apps_03m"
            ),
            f.coalesce(f.sum(f.col("accessed_apps_num_weeks_03m")), f.lit(0)).alias(
                "accessed_apps_num_weeks_03m"
            ),
        )
    )

    if "feature_list" == feature_mode:
        fea_df = fea_df.select(required_output_features)

    return fea_df


def rename_and_prep_covid_feature_df(fea_suffix: str):
    def _rename_and_prep_covid_feature_df(
        fea_df: pyspark.sql.DataFrame,
        config_feature: dict,
        filter_weekstart: datetime.date,
    ) -> pyspark.sql.DataFrame:
        """
        Returns the dataframe with renamed column

        Args:
            fea_df: dataframe which contain features

        Returns:
            dataframe which contain renamed column features
        """
        for column in fea_df.columns:
            if not ("weekstart" == column or "msisdn" == column):
                fea_df = fea_df.withColumnRenamed(column, "fea_int_app_usage_" + column)

        filter_weekstart = filter_weekstart.strftime("%Y-%m-%d")
        config_feature_int_app = config_feature["internet_apps_usage"]
        df_fea_int_app_usage_covid = prepare_fea_int_app_usage_covid(
            fea_df=fea_df, config_feature=config_feature_int_app
        )
        df_fea_int_app_usage_covid = generate_fea_int_app_usage_covid_fixed(
            df_fea_int_app_usage_covid, fea_suffix, filter_weekstart
        ).drop("weekstart")

        return df_fea_int_app_usage_covid

    return _rename_and_prep_covid_feature_df


def prepare_fea_int_app_usage_covid(
    fea_df: pyspark.sql.DataFrame, config_feature: dict
) -> pyspark.sql.DataFrame:
    """
    Prepare & generate some data to create Internet App Usage Covid-19 features

    Args:
        fea_df: Regular internet app usage features

    Returns:
        Dataframe which contain some data to generate Internet App Usage for Covid-19 related features
    """
    non_ecom_data_vol_cols = list(
        set(
            [
                f"{i[:-4]}_" + "{period_string}"
                for i in fea_df.columns
                if ("data_vol" in i)
                and ("ecommerce" not in i)
                and ("fea_int_app_usage" in i)
                and ("_week" not in i)
            ]
        )
    )

    productivity_data_vol_cols = list(
        set(
            [
                f"{i[:-4]}_" + "{period_string}"
                for i in fea_df.columns
                if (
                    ("education" in i)
                    or ("expensive_hobby" in i)
                    or ("budget_hobby" in i)
                    or ("cooking" in i)
                    or ("fitness" in i)
                    or ("jobseeker" in i)
                    or ("pets" in i)
                    or ("photography" in i)
                    or ("property" in i)
                    or ("sports" in i)
                    or ("adult_sites" in i)
                    or ("dating" in i)
                    or ("gambling" in i)
                    or ("gaming" in i)
                    or ("video" in i)
                    or ("youtube" in i)
                    or ("instagram" in i)
                    or ("facebook" in i)
                )
                and ("_data_vol_weekday" in i)
                and ("fea_int_app_usage" in i)
            ]
        )
    )

    entertainment_data_vol_cols = list(
        set(
            [
                f"{i[:-4]}_" + "{period_string}"
                for i in fea_df.columns
                if (
                    ("adult_sites" in i)
                    or ("facebook" in i)
                    or ("gambling" in i)
                    or ("gaming" in i)
                    or ("instagram" in i)
                    or ("music" in i)
                    or ("photography" in i)
                    or ("social_media_sites" in i)
                    or ("sports" in i)
                    or ("techie_gadget" in i)
                    or ("travel" in i)
                    or ("video" in i)
                    or ("youtube" in i)
                )
                and ("_data_vol_weekday" in i)
                and ("fea_int_app_usage" in i)
            ]
        )
    )

    ### PREPARE COVID-19 DATA ###
    df_fea_int_app_usage_covid = join_all(
        [
            fea_df,
            get_config_based_features(
                df=fea_df,
                feature_config=config_feature[
                    "fea_int_app_usage_covid_non_ecommerce_data_vol"
                ],
                column_expression=sum_of_columns(non_ecom_data_vol_cols),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=fea_df,
                feature_config=config_feature[
                    "fea_int_app_usage_covid_productivity_data_vol"
                ],
                column_expression=sum_of_columns(productivity_data_vol_cols),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=fea_df,
                feature_config=config_feature[
                    "fea_int_app_usage_covid_entertainment_data_vol"
                ],
                column_expression=sum_of_columns(entertainment_data_vol_cols),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    df_fea_int_app_usage_covid = get_config_based_features(
        df=df_fea_int_app_usage_covid,
        feature_config=config_feature[
            "fea_int_app_usage_covid_ratio_ecommerce_to_non_data_vol"
        ],
        column_expression=ratio_of_columns(
            "fea_int_app_usage_ecommerce_data_vol_{period_string}",
            "fea_int_app_usage_covid_non_ecommerce_data_vol_{period_string}",
        ),
    )

    return df_fea_int_app_usage_covid
