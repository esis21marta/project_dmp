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

import pyspark
import pyspark.sql.functions as f

from utils import (
    avg_over_weekstart_window,
    diff_of_columns,
    get_config_based_features,
    get_config_based_features_column_names,
    get_end_date,
    get_required_output_columns,
    get_rolling_window,
    get_start_date,
    join_all,
    median_over_weekstart_window,
    ratio_of_columns,
    sum_of_columns_over_weekstart_window,
)


def fea_int_usage_covid_fixed_all(
    df_fea_int_usage_pre_covid_fixed: pyspark.sql.DataFrame,
    df_fea_int_usage_covid_fixed: pyspark.sql.DataFrame,
    df_fea_int_usage_post_covid_fixed: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: list,
) -> pyspark.sql.DataFrame:
    """
    Final form of Internet App Usage Fixed/Static Covid-19 features

    Args:
        df_fea_int_usage_pre_covid_fixed: Internet Usage pre-covid fixed/static features
        df_fea_int_usage_covid_fixed: Internet Usage covid fixed/static features
        df_fea_int_usage_post_covid_fixed: Internet Usage post covid fixed/static features

    Returns:
        Dataframe which contain Internet Usage for Covid-19 related features
    """
    df_covid = join_all(
        [
            df_fea_int_usage_pre_covid_fixed,
            df_fea_int_usage_covid_fixed,
            df_fea_int_usage_post_covid_fixed,
        ],
        on=["msisdn"],
        how="outer",
    )
    df_covid = generate_fea_covid_internet_usage_fixed(df_covid)

    ### FEATURE SELECTION
    feature_columns = [
        "fea_int_usage_avg_kb_data_usage_pre_with_covid_ratio",
        "fea_int_usage_avg_kb_data_usage_pre_post_covid_ratio",
        "fea_int_usage_tot_kb_data_usage_pre_with_covid_ratio",
        "fea_int_usage_tot_kb_data_usage_pre_post_covid_ratio",
        "fea_int_usage_tot_kb_data_usage_weekend_weekday_pre_with_covid_ratio",
        "fea_int_usage_tot_kb_data_usage_weekend_weekday_pre_post_covid_ratio",
        "fea_int_usage_stddev_kb_data_usage_pre_with_covid_ratio",
        "fea_int_usage_stddev_kb_data_usage_pre_post_covid_ratio",
        "fea_int_usage_kb_data_usage_change_pre_with_covid_ratio",
        "fea_int_usage_kb_data_usage_change_pre_post_covid_ratio",
    ]

    required_feature_columns = get_required_output_columns(
        output_features=feature_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn"],
    )

    fea_df = df_covid.select(required_feature_columns)

    return fea_df


def prepare_fea_covid_internet_usage(
    df_int_usage_weekly: pyspark.sql.DataFrame, config_feature: dict
) -> pyspark.sql.DataFrame:
    """
    Generates features for Internet Usage Covid-19

    Args:
        df_int_usage_weekly: Internet App Usage weekly aggregation during pre and post covid period

    Returns:
        out_df: Dataframe with covid-19 related features

    """
    config_feature_internet_usage = config_feature["internet_usage"]

    def fea_int_usage_stddev_tot_data_usage(period_string, period):
        if period == 7:
            return f.col("stddev_total_data_usage_day")
        else:
            return f.stddev(f.col("fea_int_usage_avg_kb_data_usage_01w")).over(
                get_rolling_window(period, oby="weekstart")
            )

    out_df = join_all(
        [
            df_int_usage_weekly,
            get_config_based_features(
                df=df_int_usage_weekly,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_avg_kb_data_usage"
                ],
                column_expression=avg_over_weekstart_window("tot_kb"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_int_usage_weekly,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_tot_kb_data_usage"
                ],
                column_expression=sum_of_columns_over_weekstart_window(["tot_kb"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_int_usage_weekly,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_avg_kb_data_usage_weekend"
                ],
                column_expression=avg_over_weekstart_window("tot_kb_weekend"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_int_usage_weekly,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_avg_kb_data_usage_weekday"
                ],
                column_expression=avg_over_weekstart_window("tot_kb_weekday"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_int_usage_weekly,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_med_total_data_usage"
                ],
                column_expression=median_over_weekstart_window(
                    "med_vol_data_tot_kb_day"
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )
    out_df = join_all(
        [
            out_df,
            get_config_based_features(
                df=out_df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_avg_kb_data_usage_weekend_weekday_ratio"
                ],
                column_expression=ratio_of_columns(
                    "fea_int_usage_avg_kb_data_usage_weekend_{period_string}",
                    "fea_int_usage_avg_kb_data_usage_weekday_{period_string}",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=out_df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_stddev_kb_data_usage"
                ],
                column_expression=fea_int_usage_stddev_tot_data_usage,
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=out_df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_kb_data_usage_change"
                ],
                column_expression=diff_of_columns(
                    "fea_int_usage_med_total_data_usage_{period_string}",
                    "fea_int_usage_avg_kb_data_usage_{period_string}",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )
    return out_df


def generate_fea_covid_internet_usage(
    df: pyspark.sql.DataFrame, fea_suffix: str, filter_weekstart: str = None,
) -> pyspark.sql.DataFrame:
    """
    Generate Internet App Usage Covid-19 features

    Args:
        df: Regular internet usage features
        fea_suffix: Suffix of feature name, can be `pre_covid`, `covid`, or `post_covid`
        filter_weekstart: Specific weekstart to be used by dataframe

    Returns:
        Dataframe which contain static Internet App Usage Covid-19 features
    """
    if filter_weekstart:
        df = df.filter(f.col("weekstart") == filter_weekstart)

    df_fea_int_usage_covid = (
        df.withColumnRenamed(
            "fea_int_usage_avg_kb_data_usage_03m",
            f"fea_int_usage_avg_kb_data_usage_{fea_suffix}",
        )
        .withColumnRenamed(
            "fea_int_usage_tot_kb_data_usage_03m",
            f"fea_int_usage_tot_kb_data_usage_{fea_suffix}",
        )
        .withColumnRenamed(
            "fea_int_usage_avg_kb_data_usage_weekend_weekday_ratio_03m",
            f"fea_int_usage_tot_kb_data_usage_weekend_weekday_ratio_{fea_suffix}",
        )
        .withColumnRenamed(
            "fea_int_usage_stddev_kb_data_usage_03m",
            f"fea_int_usage_stddev_kb_data_usage_{fea_suffix}",
        )
        .withColumnRenamed(
            "fea_int_usage_kb_data_usage_change_03m",
            f"fea_int_usage_kb_data_usage_change_{fea_suffix}",
        )
    )
    df_fea_int_usage_covid = df_fea_int_usage_covid.select(
        "msisdn",
        "weekstart",
        f"fea_int_usage_avg_kb_data_usage_{fea_suffix}",
        f"fea_int_usage_tot_kb_data_usage_{fea_suffix}",
        f"fea_int_usage_tot_kb_data_usage_weekend_weekday_ratio_{fea_suffix}",
        f"fea_int_usage_stddev_kb_data_usage_{fea_suffix}",
        f"fea_int_usage_kb_data_usage_change_{fea_suffix}",
    )
    return df_fea_int_usage_covid


def fea_int_usage_covid_fixed(fea_suffix: str):
    def _fea_int_usage_covid_fixed(
        df_int_usage_weekly: pyspark.sql.DataFrame,
        config_feature: dict,
        filter_weekstart: datetime.date,
    ) -> pyspark.sql.DataFrame:
        """
        Calculates fix features on Internet Usage Covid-19

        Args:
            df_int_usage_weekly: Internet App Usage weekly aggregation during pre and post covid period

        Returns:
            fea_df: Dataframe with following features
                - msisdn: Unique Id
                - weekstart: Start of week
                - fea_int_usage_avg_kb_data_usage_pre_with_covid_ratio: Ratio between pre and covid for average kilobytes of data usage
                - fea_int_usage_avg_kb_data_usage_pre_post_covid_ratio: Ratio between pre and post covid for average kilobytes of data usage
                - fea_int_usage_tot_kb_data_usage_pre_with_covid_ratio: Ratio between pre and covid for total kilobytes of data usage
                - fea_int_usage_tot_kb_data_usage_pre_post_covid_ratio: Ratio between pre and post covid for total kilobytes of data usage
                - fea_int_usage_tot_kb_data_usage_weekend_weekday_pre_with_covid_ratio: Ratio between pre and covid for weekend vs weekday kilobytes of data usage
                - fea_int_usage_tot_kb_data_usage_weekend_weekday_pre_post_covid_ratio: Ratio between pre and post covid for weekend vs weekday kilobytes of data usage
                - fea_int_usage_stddev_kb_data_usage_pre_with_covid_ratio: Ratio between pre and covid for stddev kilobytes of data usage
                - fea_int_usage_stddev_kb_data_usage_pre_post_covid_ratio: Ratio between pre and post covid for stddev kilobytes of data usage
                - fea_int_usage_kb_data_usage_change_pre_with_covid_ratio: Ratio of change data usage between pre and covid period
                - fea_int_usage_kb_data_usage_change_pre_post_covid_ratio: Ratio of change data usage between pre and post covid period

        """
        filter_weekstart = filter_weekstart.strftime("%Y-%m-%d")
        out_df = prepare_fea_covid_internet_usage(
            df_int_usage_weekly=df_int_usage_weekly, config_feature=config_feature
        )
        df_fea_int_usage_covid = generate_fea_covid_internet_usage(
            out_df, fea_suffix, filter_weekstart
        ).drop("weekstart")
        return df_fea_int_usage_covid

    return _fea_int_usage_covid_fixed


def fea_int_usage_covid(
    df_int_usage_weekly: pyspark.sql.DataFrame,
    df_fea_int_usage_pre_covid: pyspark.sql.DataFrame,
    df_fea_int_usage_covid: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: list,
) -> pyspark.sql.DataFrame:
    """
    Generate Internet App Usage for Covid-19 related features

    Args:
        df_int_usage_weekly: Internet Usage weekly aggregation
        df_fea_int_usage_pre_covid: Internet Usage pre-covid fixed/static features
        df_fea_int_usage_covid: Internet Usage covid fixed/static features

    Returns:
        Dataframe which contain Internet Usage for Covid-19 related features
    """
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

    ## Split dataframe into 3: covid, post-covid, after post-covid
    df_int_usage_weekly_covid = df_int_usage_weekly.filter(
        f.col("weekstart").between(covid_first_weekstart, covid_last_weekstart)
    )
    df_int_usage_weekly_post_covid = df_int_usage_weekly.filter(
        f.col("weekstart").between(
            post_covid_first_weekstart, post_covid_last_weekstart
        )
    )
    df_int_usage_weekly = join_all(
        [
            df_int_usage_weekly.filter(f.col("weekstart") > post_covid_last_weekstart),
            df_fea_int_usage_covid,
        ],
        on=["msisdn"],
        how="left",
    )

    ## Generate covid features
    df_int_usage_weekly_covid = prepare_fea_covid_internet_usage(
        df_int_usage_weekly_covid, config_feature
    )
    df_int_usage_weekly_covid = generate_fea_covid_internet_usage(
        df_int_usage_weekly_covid, "covid"
    )
    df_int_usage_weekly_post_covid = prepare_fea_covid_internet_usage(
        df_int_usage_weekly_post_covid, config_feature
    )
    df_int_usage_weekly_post_covid = generate_fea_covid_internet_usage(
        df_int_usage_weekly_post_covid, "post_covid"
    )

    df_covid = join_all(
        [df_int_usage_weekly_covid, df_int_usage_weekly_post_covid,],
        on=["msisdn", "weekstart"],
        how="outer",
    )
    df_covid = df_covid.join(df_fea_int_usage_pre_covid, ["msisdn"], how="left")
    df_covid = generate_fea_covid_internet_usage_fixed(df_covid)

    ### FEATURE SELECTION
    pre_with_covid_columns = [
        "fea_int_usage_avg_kb_data_usage_pre_with_covid_ratio",
        "fea_int_usage_tot_kb_data_usage_pre_with_covid_ratio",
        "fea_int_usage_tot_kb_data_usage_weekend_weekday_pre_with_covid_ratio",
        "fea_int_usage_stddev_kb_data_usage_pre_with_covid_ratio",
        "fea_int_usage_kb_data_usage_change_pre_with_covid_ratio",
    ]
    feature_columns = [
        *pre_with_covid_columns,
        "fea_int_usage_avg_kb_data_usage_pre_post_covid_ratio",
        "fea_int_usage_tot_kb_data_usage_pre_post_covid_ratio",
        "fea_int_usage_tot_kb_data_usage_weekend_weekday_pre_post_covid_ratio",
        "fea_int_usage_stddev_kb_data_usage_pre_post_covid_ratio",
        "fea_int_usage_kb_data_usage_change_pre_post_covid_ratio",
    ]

    required_feature_columns = get_required_output_columns(
        output_features=feature_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_int_usage_weekly = df_int_usage_weekly.select(required_feature_columns)
    df_covid = df_covid.select(required_feature_columns)

    fea_df = df_int_usage_weekly.union(df_covid)

    # fillna for pre & covid features
    pre_with_covid_columns = [i for i in fea_df.columns if i in pre_with_covid_columns]
    for column in pre_with_covid_columns:
        fea_df = fea_df.withColumn(
            column,
            f.when(
                f.col(column).isNull(),
                f.last(f.col(column), ignorenulls=True).over(
                    get_rolling_window(-1, oby="weekstart")
                ),
            ).otherwise(f.col(column)),
        )

    return fea_df


def generate_fea_covid_internet_usage_fixed(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Generate Fixed/Static Internet Usage Covid-19 features

    Args:
        df: Internet usage features on pre and post covid periot

    Returns:
        Dataframe which contain static Internet Usage Covid-19 features
    """
    df_covid = (
        df.withColumn(
            "fea_int_usage_avg_kb_data_usage_pre_with_covid_ratio",  # FID 12
            f.col("fea_int_usage_avg_kb_data_usage_covid")
            / (
                f.col("fea_int_usage_avg_kb_data_usage_covid")
                + f.col("fea_int_usage_avg_kb_data_usage_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_usage_avg_kb_data_usage_pre_post_covid_ratio",  # FID 12
            f.col("fea_int_usage_avg_kb_data_usage_post_covid")
            / (
                f.col("fea_int_usage_avg_kb_data_usage_post_covid")
                + f.col("fea_int_usage_avg_kb_data_usage_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_usage_tot_kb_data_usage_pre_with_covid_ratio",  # FID 15
            f.col("fea_int_usage_tot_kb_data_usage_covid")
            / (
                f.col("fea_int_usage_tot_kb_data_usage_covid")
                + f.col("fea_int_usage_tot_kb_data_usage_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_usage_tot_kb_data_usage_pre_post_covid_ratio",  # FID 15
            f.col("fea_int_usage_tot_kb_data_usage_post_covid")
            / (
                f.col("fea_int_usage_tot_kb_data_usage_post_covid")
                + f.col("fea_int_usage_tot_kb_data_usage_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_usage_tot_kb_data_usage_weekend_weekday_pre_with_covid_ratio",  # FID 16
            f.col("fea_int_usage_tot_kb_data_usage_weekend_weekday_ratio_covid")
            / (
                f.col("fea_int_usage_tot_kb_data_usage_weekend_weekday_ratio_covid")
                + f.col(
                    "fea_int_usage_tot_kb_data_usage_weekend_weekday_ratio_pre_covid"
                )
            ),
        )
        .withColumn(
            "fea_int_usage_tot_kb_data_usage_weekend_weekday_pre_post_covid_ratio",  # FID 16
            f.col("fea_int_usage_tot_kb_data_usage_weekend_weekday_ratio_post_covid")
            / (
                f.col("fea_int_usage_tot_kb_data_usage_weekend_weekday_ratio_pre_covid")
                + f.col(
                    "fea_int_usage_tot_kb_data_usage_weekend_weekday_ratio_post_covid"
                )
            ),
        )
        .withColumn(
            "fea_int_usage_stddev_kb_data_usage_pre_with_covid_ratio",  # FID 14
            f.col("fea_int_usage_stddev_kb_data_usage_covid")
            / (
                f.col("fea_int_usage_stddev_kb_data_usage_covid")
                + f.col("fea_int_usage_stddev_kb_data_usage_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_usage_stddev_kb_data_usage_pre_post_covid_ratio",  # FID 14
            f.col("fea_int_usage_stddev_kb_data_usage_post_covid")
            / (
                f.col("fea_int_usage_stddev_kb_data_usage_post_covid")
                + f.col("fea_int_usage_stddev_kb_data_usage_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_usage_kb_data_usage_change_pre_with_covid_ratio",  # FID 13
            f.col("fea_int_usage_kb_data_usage_change_covid")
            / (
                f.col("fea_int_usage_kb_data_usage_change_covid")
                + f.col("fea_int_usage_kb_data_usage_change_pre_covid")
            ),
        )
        .withColumn(
            "fea_int_usage_kb_data_usage_change_pre_post_covid_ratio",  # FID 13
            f.col("fea_int_usage_kb_data_usage_change_post_covid")
            / (
                f.col("fea_int_usage_kb_data_usage_change_post_covid")
                + f.col("fea_int_usage_kb_data_usage_change_pre_covid")
            ),
        )
    )

    return df_covid


def fea_int_usage_covid_trend(
    df_int_usage_weekly: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: list,
) -> pyspark.sql.DataFrame:
    """
    Calculates trend of features on Internet Usage Covid-19

    Args:
        df_fea_int_usage: Internet Usage Features Dataframe

    Returns:
        fea_df: Dataframe with following features
            - msisdn: Unique Id
            - weekstart: Start of week
            - fea_int_usage_stddev_total_data_usage_trend: Trend of stddev of total data usage in kilobytes
            - fea_int_usage_tot_kb_data_usage_trend: Trend of total data usage in kilobytes

    """

    def fea_int_usage_daily_avg_kb_data_usage(period_string, period):
        return f.col(f"fea_int_usage_tot_kb_data_usage_{period_string}") / period

    def fea_int_usage_stddev_total_data_usage(period_string, period):
        if period == 7:
            return f.col("stddev_total_data_usage_day")
        else:
            return f.stddev(f.col("fea_int_usage_daily_avg_kb_data_usage_01w")).over(
                get_rolling_window(period, oby="weekstart")
            )

    config_feature_internet_usage = config_feature["internet_usage"]

    ## Pre-process stddev and tot_kb data usage
    out_df = get_config_based_features(
        df=df_int_usage_weekly,
        feature_config=config_feature_internet_usage["fea_int_usage_tot_kb_data_usage"],
        column_expression=sum_of_columns_over_weekstart_window(["tot_kb"]),
    )

    out_df = get_config_based_features(
        df=out_df,
        feature_config=config_feature_internet_usage[
            "fea_int_usage_daily_avg_kb_data_usage"
        ],
        column_expression=fea_int_usage_daily_avg_kb_data_usage,
    )

    out_df = get_config_based_features(
        df=out_df,
        feature_config=config_feature_internet_usage[
            "fea_int_usage_stddev_total_data_usage"
        ],
        column_expression=fea_int_usage_stddev_total_data_usage,
    )

    out_df = join_all(
        [
            get_config_based_features(
                df=out_df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_stddev_total_data_usage_trend"
                ],
                column_expression=ratio_of_columns(
                    "fea_int_usage_stddev_total_data_usage_{period_string}",
                    "fea_int_usage_stddev_total_data_usage_01w",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=out_df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_tot_kb_data_usage_trend"
                ],
                column_expression=ratio_of_columns(
                    "fea_int_usage_tot_kb_data_usage_{period_string}",
                    "fea_int_usage_tot_kb_data_usage_01w",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    feature_columns = get_config_based_features_column_names(
        config_feature_internet_usage["fea_int_usage_stddev_total_data_usage_trend"],
        config_feature_internet_usage["fea_int_usage_tot_kb_data_usage_trend"],
    )

    required_feature_columns = get_required_output_columns(
        output_features=feature_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    fea_df = out_df.select(required_feature_columns)

    #### FILTER WEEKSTART
    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    fea_df = fea_df.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )

    return fea_df
