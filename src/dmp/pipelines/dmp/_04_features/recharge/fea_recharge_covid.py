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

from typing import List

import pyspark
import pyspark.sql.functions as f

from utils import (
    avg_monthly_over_weekstart_window,
    avg_over_weekstart_window,
    division_of_columns,
    get_config_based_features,
    get_config_based_features_column_names,
    get_custom_window,
    get_end_date,
    get_required_output_columns,
    get_rolling_window,
    get_start_date,
    join_all,
    ratio_of_columns,
    sum_of_columns_over_weekstart_window,
)


def fea_recharge_covid_rename(
    fea_df: pyspark.sql.DataFrame, suffix: str
) -> pyspark.sql.DataFrame:
    """
    Returns the dataframe with renamed column

    Args:
        fea_df: dataframe which contain features

    Returns:
        dataframe which contain renamed column features
    """
    for column in fea_df.columns:
        if not column in ["weekstart", "msisdn"]:
            fea_df = fea_df.withColumnRenamed(column, column.replace("03m", suffix))
    return fea_df


def generate_fea_topup_behav_covid(
    df: pyspark.sql.DataFrame, config_feature_recharge: dict
) -> pyspark.sql.DataFrame:
    """
    Generate topup behaviours covid-19 features

    Args:
        df: Recharge weekly aggregation

    Returns:
        Dataframe which contain topup behaviours covid-19 features
    """
    df_topup_behav_covid = join_all(
        [
            get_config_based_features(
                df=df,
                feature_config=config_feature_recharge["fea_rech_tot_amt_covid_sum"],
                column_expression=sum_of_columns_over_weekstart_window(["tot_amt"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_recharge["fea_rech_tot_trx_covid_sum"],
                column_expression=sum_of_columns_over_weekstart_window(["tot_trx"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_recharge["fea_rech_amt_covid_weekly_avg"],
                column_expression=avg_over_weekstart_window("tot_amt"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_recharge[
                    "fea_rech_amt_covid_monthly_avg"
                ],
                column_expression=avg_monthly_over_weekstart_window("tot_amt"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_recharge[
                    "fea_rech_tot_amt_digi_covid_sum"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["tot_amt_digi"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )
    df_topup_behav_covid = join_all(
        [
            df_topup_behav_covid,
            get_config_based_features(
                df=df_topup_behav_covid,
                feature_config=config_feature_recharge["fea_rech_tot_amt_covid_avg"],
                column_expression=division_of_columns(
                    "fea_rech_tot_amt_covid_sum_{period_string}",
                    "fea_rech_tot_trx_covid_sum_{period_string}",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_topup_behav_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_tot_amt_digi_covid_ratio"
                ],
                column_expression=division_of_columns(
                    "fea_rech_tot_amt_digi_covid_sum_{period_string}",
                    "fea_rech_tot_amt_covid_sum_{period_string}",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )
    df_topup_behav_covid = get_config_based_features(
        df=df_topup_behav_covid,
        feature_config=config_feature_recharge["fea_rech_tot_amt_digi_covid_trends"],
        column_expression=ratio_of_columns(
            "fea_rech_tot_amt_digi_covid_ratio_{period_string}",
            "fea_rech_tot_amt_digi_covid_ratio_01w",
        ),
    )

    topup_feature_columns = get_config_based_features_column_names(
        config_feature_recharge["fea_rech_tot_amt_covid_sum"],
        config_feature_recharge["fea_rech_tot_trx_covid_sum"],
        config_feature_recharge["fea_rech_amt_covid_weekly_avg"],
        config_feature_recharge["fea_rech_amt_covid_monthly_avg"],
        config_feature_recharge["fea_rech_tot_amt_digi_covid_sum"],
        config_feature_recharge["fea_rech_tot_amt_covid_avg"],
        config_feature_recharge["fea_rech_tot_amt_digi_covid_ratio"],
        config_feature_recharge["fea_rech_tot_amt_digi_covid_trends"],
    )

    df_topup_behav_covid = df_topup_behav_covid.select(
        "msisdn", "weekstart", *topup_feature_columns
    )

    return df_topup_behav_covid


def generate_fea_balance_covid(
    df: pyspark.sql.DataFrame, config_feature_recharge: dict
) -> pyspark.sql.DataFrame:
    """
    Generate Account Balance covid-19 features

    Args:
        df: Balance weekly aggregation

    Returns:
        Dataframe which contain Account Balance covid-19 features
    """

    def fea_rech_bal_stddev(period_string, period):
        if period == 7:
            return f.col("account_balance_stddev")
        else:
            return f.stddev(f.col("account_balance_avg")).over(
                get_rolling_window(period, oby="weekstart")
            )

    df_bal_covid = join_all(
        [
            get_config_based_features(
                df=df,
                feature_config=config_feature_recharge["fea_rech_bal_covid_stddev"],
                column_expression=fea_rech_bal_stddev,
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_recharge["fea_rech_bal_covid_avg"],
                column_expression=avg_over_weekstart_window("account_balance_avg"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    bal_feature_columns = get_config_based_features_column_names(
        config_feature_recharge["fea_rech_bal_covid_stddev"],
        config_feature_recharge["fea_rech_bal_covid_avg"],
    )

    df_bal_covid = df_bal_covid.select("msisdn", "weekstart", *bal_feature_columns)

    return df_bal_covid


def generate_fea_pkg_prchse_covid(
    df: pyspark.sql.DataFrame, config_feature_recharge: dict
) -> pyspark.sql.DataFrame:
    """
    Generate Package Purchases covid-19 features

    Args:
        df: Package Purchases weekly aggregation

    Returns:
        Dataframe which contain Package Purchases covid-19 features
    """
    df_fea_pkg_prchse_covid = join_all(
        [
            get_config_based_features(
                df=df,
                feature_config=config_feature_recharge["fea_rech_trx_pkg_prchse_sum"],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["trx_pkg_prchse"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_recharge["fea_rech_rev_pkg_prchse_sum"],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["rev_pkg_prchse"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    df_fea_pkg_prchse_covid = get_config_based_features(
        df=df_fea_pkg_prchse_covid,
        feature_config=config_feature_recharge[
            "fea_rech_rev_pkg_prchse_covid_product_val"
        ],
        column_expression=division_of_columns(
            "fea_rech_rev_pkg_prchse_sum_{period_string}",
            "fea_rech_trx_pkg_prchse_sum_{period_string}",
        ),
        return_only_feature_columns=True,
        columns_to_keep_from_original_df=["msisdn", "weekstart"],
    )

    pkg_prchse_feature_columns = get_config_based_features_column_names(
        config_feature_recharge["fea_rech_rev_pkg_prchse_covid_product_val"],
    )

    df_fea_pkg_prchse_covid = df_fea_pkg_prchse_covid.select(
        "msisdn", "weekstart", *pkg_prchse_feature_columns
    )

    return df_fea_pkg_prchse_covid


def fea_recharge_covid_fixed(suffix: str):
    def _fea_recharge_covid_fixed(
        df_rech_weekly_covid: pyspark.sql.DataFrame,
        df_digi_rech_weekly_covid: pyspark.sql.DataFrame,
        df_acc_bal_weekly_covid: pyspark.sql.DataFrame,
        df_chg_pkg_prchse_weekly_covid: pyspark.sql.DataFrame,
        config_feature: dict,
    ) -> pyspark.sql.DataFrame:
        """
        Calculates features on Recharge domain during pre-covid period:

        Args:
            df_rech_weekly_*covid: Recharge weekly aggregation during pre-covid period
            df_digi_rech_weekly_*covid:: Digital recharge weekly aggregation during pre-covid period
            df_acc_bal_weekly_*covid:: Account balance table aggregated weekly during pre-covid period
            df_chg_pkg_prchse_weekly_*covid:: charge packs weekly table during pre-covid period

        Returns:
            fea_df: Dataframe with following features
                - msisdn: Unique Id
                - fea_rech_tot_amt_*covid:_sum_*: Total amount of recharge during pre-covid period
                - fea_rech_tot_trx_*covid:_sum_*: Total recharge transaction during pre-covid period
                - fea_rech_amt_*covid:_weekly_avg_*: Average of amount of recharge weekly during pre-covid period
                - fea_rech_amt_*covid:_monthly_avg_*: Average of amount of recharge monthly during pre-covid period
                - fea_rech_tot_amt_digi_*covid:_sum_*: Total amount of digital recharge during pre-covid period
                - fea_rech_tot_amt_*covid:_avg_*: Average of amount of recharge per transaction recharge during pre-covid period
                - fea_rech_tot_amt_digi_*covid:_ratio_*: Ratio of digital recharge to total recharge during pre-covid period
                - fea_rech_tot_amt_digi_*covid:_trends_*: Trends of digital recharge to total recharge during pre-covid period
                - fea_rech_bal_*covid:_stddev_*: Std Dev of Balance during pre-covid period
                - fea_rech_bal_*covid:_avg_*: Average of balance during pre-covid period
                - fea_rech_rev_pkg_prchse_product_val_*covid:_*: Revenue of package purchase during pre-covid period

        """

        config_feature_recharge = config_feature["recharge"]

        df_rech_weekly_covid = df_rech_weekly_covid.join(
            df_digi_rech_weekly_covid, ["msisdn", "weekstart"], how="left"
        )

        #### FEATURE IMPLEMENTATION : Topup Behav
        df_topup_behav_covid = generate_fea_topup_behav_covid(
            df_rech_weekly_covid, config_feature_recharge
        )
        df_topup_behav_covid = df_topup_behav_covid.withColumn(
            "rank",
            f.row_number().over(
                get_custom_window(
                    partition_by_cols=["msisdn"], order_by_col="weekstart", asc=False
                )
            ),
        )
        df_topup_behav_covid = df_topup_behav_covid.filter(f.col("rank") == 1).drop(
            "weekstart", "rank"
        )

        #### FEATURE IMPLEMENTATION : Balance
        df_bal_covid = generate_fea_balance_covid(
            df_acc_bal_weekly_covid, config_feature_recharge
        )
        df_bal_covid = df_bal_covid.withColumn(
            "rank",
            f.row_number().over(
                get_custom_window(
                    partition_by_cols=["msisdn"], order_by_col="weekstart", asc=False
                )
            ),
        )
        df_bal_covid = df_bal_covid.filter(f.col("rank") == 1).drop("weekstart", "rank")

        #### FEATURE IMPLEMENTATION : Package Purchase
        df_fea_pkg_prchse_covid = generate_fea_pkg_prchse_covid(
            df_chg_pkg_prchse_weekly_covid, config_feature_recharge
        )

        df_fea_pkg_prchse_covid = df_fea_pkg_prchse_covid.withColumn(
            "rank",
            f.row_number().over(
                get_custom_window(
                    partition_by_cols=["msisdn"], order_by_col="weekstart", asc=False
                )
            ),
        )
        df_fea_pkg_prchse_covid = df_fea_pkg_prchse_covid.filter(
            f.col("rank") == 1
        ).drop("weekstart", "rank")

        #### JOIN
        fea_df = join_all(
            [df_topup_behav_covid, df_bal_covid, df_fea_pkg_prchse_covid],
            on=["msisdn"],
            how="full",
        )
        # Rename df
        fea_df = fea_recharge_covid_rename(fea_df, suffix)

        return fea_df

    return _fea_recharge_covid_fixed


def fea_recharge_covid_fixed_joined(
    df_fea_pre_covid: pyspark.sql.DataFrame,
    df_fea_covid: pyspark.sql.DataFrame,
    df_fea_post_covid: pyspark.sql.DataFrame,
    config_feature_recharge: dict,
    granularity: List[str] = ["msisdn"],
) -> pyspark.sql.DataFrame:
    """
    Generate Recharge Covid-19 fixed/static features

    Args:
        df_fea_pre_covid: Recharge pre-covid fixed/static features
        df_fea_covid: Recharge covid fixed/static features
        df_fea_post_covid: Recharge post covid fixed/static features
        granularity: Granularity of the features

    Returns:
        Dataframe which contain Recharge for fixed/static Covid-19 related features
    """
    df_covid = join_all(
        [df_fea_covid, df_fea_post_covid,], on=granularity, how="outer",
    )
    df_covid = df_covid.join(df_fea_pre_covid, ["msisdn"], how="left")
    df_covid.cache()

    fea_df = join_all(
        [
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_tot_amt_pre_with_covid_avg_ratio"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_tot_amt_covid_avg_covid",
                    "fea_rech_tot_amt_covid_avg_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_tot_amt_pre_post_covid_avg_ratio"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_tot_amt_covid_avg_post_covid",
                    "fea_rech_tot_amt_covid_avg_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_amt_pre_with_covid_weekly_avg_ratio"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_amt_covid_weekly_avg_covid",
                    "fea_rech_amt_covid_weekly_avg_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_amt_pre_post_covid_weekly_avg_ratio"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_amt_covid_weekly_avg_post_covid",
                    "fea_rech_amt_covid_weekly_avg_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_amt_pre_with_covid_monthly_avg_ratio"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_amt_covid_monthly_avg_covid",
                    "fea_rech_amt_covid_monthly_avg_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_amt_pre_post_covid_monthly_avg_ratio"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_amt_covid_monthly_avg_post_covid",
                    "fea_rech_amt_covid_monthly_avg_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_tot_trx_pre_with_covid_ratio"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_tot_trx_covid_sum_covid",
                    "fea_rech_tot_trx_covid_sum_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_tot_trx_pre_post_covid_ratio"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_tot_trx_covid_sum_post_covid",
                    "fea_rech_tot_trx_covid_sum_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_tot_amt_digi_pre_with_covid_ratio"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_tot_amt_digi_covid_ratio_covid",
                    "fea_rech_tot_amt_digi_covid_ratio_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_tot_amt_digi_pre_post_covid_ratio"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_tot_amt_digi_covid_ratio_post_covid",
                    "fea_rech_tot_amt_digi_covid_ratio_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_tot_amt_digi_pre_with_covid_trends"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_tot_amt_digi_covid_trends_covid",
                    "fea_rech_tot_amt_digi_covid_trends_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_tot_amt_digi_pre_post_covid_trends"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_tot_amt_digi_covid_trends_post_covid",
                    "fea_rech_tot_amt_digi_covid_trends_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_bal_pre_with_covid_stddev_ratio"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_bal_covid_stddev_covid",
                    "fea_rech_bal_covid_stddev_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_bal_pre_post_covid_stddev_ratio"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_bal_covid_stddev_post_covid",
                    "fea_rech_bal_covid_stddev_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_bal_pre_with_covid_avg_ratio"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_bal_covid_avg_covid", "fea_rech_bal_covid_avg_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_bal_pre_post_covid_avg_ratio"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_bal_covid_avg_post_covid",
                    "fea_rech_bal_covid_avg_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_rev_pkg_prchse_product_val_pre_with_covid_ratio"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_rev_pkg_prchse_covid_product_val_covid",
                    "fea_rech_rev_pkg_prchse_covid_product_val_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
            get_config_based_features(
                df=df_covid,
                feature_config=config_feature_recharge[
                    "fea_rech_rev_pkg_prchse_product_val_pre_post_covid_ratio"
                ],
                column_expression=ratio_of_columns(
                    "fea_rech_rev_pkg_prchse_covid_product_val_post_covid",
                    "fea_rech_rev_pkg_prchse_covid_product_val_pre_covid",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=granularity,
            ),
        ],
        on=granularity,
        how="outer",
    )
    return fea_df


def fea_recharge_covid_fixed_all(
    df_fea_pre_covid: pyspark.sql.DataFrame,
    df_fea_covid: pyspark.sql.DataFrame,
    df_fea_post_covid: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
    granularity: List[str] = ["msisdn"],
) -> pyspark.sql.DataFrame:
    """
    Final form of Recharge Covid-19 features

    Args:
        df_fea_pre_covid: Recharge pre-covid fixed/static features
        df_fea_covid: Recharge covid fixed/static features
        df_fea_post_covid: Recharge post covid fixed/static features
        granularity: Granularity of the features

    Returns:
        Dataframe which contain Recharge for Covid-19 related features
    """
    config_feature_recharge = config_feature["recharge"]

    fea_df = fea_recharge_covid_fixed_joined(
        df_fea_pre_covid,
        df_fea_covid,
        df_fea_post_covid,
        config_feature_recharge,
        granularity,
    )

    #### FEATURE SELECTION
    feature_columns = get_config_based_features_column_names(
        config_feature_recharge["fea_rech_tot_amt_pre_with_covid_avg_ratio"],
        config_feature_recharge["fea_rech_tot_amt_pre_post_covid_avg_ratio"],
        config_feature_recharge["fea_rech_amt_pre_with_covid_weekly_avg_ratio"],
        config_feature_recharge["fea_rech_amt_pre_post_covid_weekly_avg_ratio"],
        config_feature_recharge["fea_rech_amt_pre_with_covid_monthly_avg_ratio"],
        config_feature_recharge["fea_rech_amt_pre_post_covid_monthly_avg_ratio"],
        config_feature_recharge["fea_rech_tot_trx_pre_with_covid_ratio"],
        config_feature_recharge["fea_rech_tot_trx_pre_post_covid_ratio"],
        config_feature_recharge["fea_rech_tot_amt_digi_pre_with_covid_ratio"],
        config_feature_recharge["fea_rech_tot_amt_digi_pre_post_covid_ratio"],
        config_feature_recharge["fea_rech_tot_amt_digi_pre_with_covid_trends"],
        config_feature_recharge["fea_rech_tot_amt_digi_pre_post_covid_trends"],
        config_feature_recharge["fea_rech_bal_pre_with_covid_stddev_ratio"],
        config_feature_recharge["fea_rech_bal_pre_post_covid_stddev_ratio"],
        config_feature_recharge["fea_rech_bal_pre_with_covid_avg_ratio"],
        config_feature_recharge["fea_rech_bal_pre_post_covid_avg_ratio"],
        config_feature_recharge[
            "fea_rech_rev_pkg_prchse_product_val_pre_with_covid_ratio"
        ],
        config_feature_recharge[
            "fea_rech_rev_pkg_prchse_product_val_pre_post_covid_ratio"
        ],
    )

    required_topup_output_columns = get_required_output_columns(
        output_features=feature_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=granularity,
    )

    fea_df = fea_df.select(required_topup_output_columns)

    return fea_df


def fea_recharge_covid(
    df_rech_weekly: pyspark.sql.DataFrame,
    df_digi_rech_weekly: pyspark.sql.DataFrame,
    df_acc_bal_weekly: pyspark.sql.DataFrame,
    df_chg_pkg_prchse_weekly: pyspark.sql.DataFrame,
    df_fea_fixed_pre_covid: pyspark.sql.DataFrame,
    df_fea_fixed_all_covid: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Generate Recharge Covid-19 features

    Args:
        df_rech_weekly: Recharges table aggregated weekly
        df_digi_rech_weekly: Digital Recharge table aggregated weekly
        df_acc_bal_weekly: Account balance table aggregated weekly
        df_chg_pkg_prchse_weekly: charge packs table aggregated weekly
        df_fea_fixed_pre_covid: Recharge pre-covid fixed/static features
        df_fea_fixed_all_covid: Recharge covid fixed/static features

    Returns:
        Dataframe which contain Recharge for Covid-19 related features
    """
    config_feature_recharge = config_feature["recharge"]
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
    df_rech_weekly = df_rech_weekly.join(
        df_digi_rech_weekly, ["msisdn", "weekstart"], how="left"
    )
    df_rech_weekly_covid = df_rech_weekly.filter(
        f.col("weekstart").between(covid_first_weekstart, covid_last_weekstart)
    )
    df_rech_weekly_post_covid = df_rech_weekly.filter(
        f.col("weekstart").between(
            post_covid_first_weekstart, post_covid_last_weekstart
        )
    )
    df_rech_weekly = df_rech_weekly.filter(
        f.col("weekstart") > post_covid_last_weekstart
    ).select("msisdn", "weekstart")

    df_acc_bal_weekly_covid = df_acc_bal_weekly.filter(
        f.col("weekstart").between(covid_first_weekstart, covid_last_weekstart)
    )
    df_acc_bal_weekly_post_covid = df_acc_bal_weekly.filter(
        f.col("weekstart").between(
            post_covid_first_weekstart, post_covid_last_weekstart
        )
    )
    df_chg_pkg_prchse_weekly_covid = df_chg_pkg_prchse_weekly.filter(
        f.col("weekstart").between(covid_first_weekstart, covid_last_weekstart)
    )
    df_chg_pkg_prchse_weekly_post_covid = df_chg_pkg_prchse_weekly.filter(
        f.col("weekstart").between(
            post_covid_first_weekstart, post_covid_last_weekstart
        )
    )

    #### FEATURE IMPLEMENTATION : Topup Behav
    df_topup_behav_covid = generate_fea_topup_behav_covid(
        df_rech_weekly_covid, config_feature_recharge
    )
    df_topup_behav_post_covid = generate_fea_topup_behav_covid(
        df_rech_weekly_post_covid, config_feature_recharge
    )

    #### FEATURE IMPLEMENTATION : Balance
    df_bal_covid = generate_fea_balance_covid(
        df_acc_bal_weekly_covid, config_feature_recharge
    )
    df_bal_post_covid = generate_fea_balance_covid(
        df_acc_bal_weekly_post_covid, config_feature_recharge
    )

    #### FEATURE IMPLEMENTATION : Package Purchase
    df_fea_pkg_prchse_covid = generate_fea_pkg_prchse_covid(
        df_chg_pkg_prchse_weekly_covid, config_feature_recharge
    )
    df_fea_pkg_prchse_post_covid = generate_fea_pkg_prchse_covid(
        df_chg_pkg_prchse_weekly_post_covid, config_feature_recharge
    )

    #### COVID JOIN
    fea_covid_df = join_all(
        [df_topup_behav_covid, df_bal_covid, df_fea_pkg_prchse_covid],
        on=["msisdn", "weekstart"],
        how="full",
    )

    # Rename df
    fea_covid_df = fea_recharge_covid_rename(fea_covid_df, "covid")

    #### POST-COVID JOIN
    fea_post_covid_df = join_all(
        [df_topup_behav_post_covid, df_bal_post_covid, df_fea_pkg_prchse_post_covid],
        on=["msisdn", "weekstart"],
        how="full",
    )
    # Rename df
    fea_post_covid_df = fea_recharge_covid_rename(fea_post_covid_df, "post_covid")

    granularity = ["msisdn", "weekstart"]
    fea_covid_all = fea_recharge_covid_fixed_joined(
        df_fea_fixed_pre_covid,
        fea_covid_df,
        fea_post_covid_df,
        config_feature_recharge,
        granularity,
    )

    # join with after post-covid
    fea_covid_all = df_rech_weekly.join(
        fea_covid_all, ["msisdn", "weekstart"], how="outer"
    )

    # Fill covid period null
    fea_covid_all = fea_covid_all.alias("A").join(
        df_fea_fixed_all_covid.alias("B"), ["msisdn"], how="left"
    )
    post_covid_cond = f.col("A.weekstart") > covid_last_weekstart
    after_post_covid_cond = f.col("A.weekstart") > post_covid_last_weekstart

    fea_covid_all = fea_covid_all.groupBy("msisdn", "weekstart").agg(
        f.first(
            f.when(
                post_covid_cond, f.col("B.fea_rech_tot_amt_pre_with_covid_avg_ratio")
            ).otherwise(f.col("A.fea_rech_tot_amt_pre_with_covid_avg_ratio"))
        ).alias("fea_rech_tot_amt_pre_with_covid_avg_ratio"),
        f.first(
            f.when(
                post_covid_cond, f.col("B.fea_rech_amt_pre_with_covid_weekly_avg_ratio")
            ).otherwise(f.col("A.fea_rech_amt_pre_with_covid_weekly_avg_ratio"))
        ).alias("fea_rech_amt_pre_with_covid_weekly_avg_ratio"),
        f.first(
            f.when(
                post_covid_cond,
                f.col("B.fea_rech_amt_pre_with_covid_monthly_avg_ratio"),
            ).otherwise(f.col("A.fea_rech_amt_pre_with_covid_monthly_avg_ratio"))
        ).alias("fea_rech_amt_pre_with_covid_monthly_avg_ratio"),
        f.first(
            f.when(
                post_covid_cond, f.col("B.fea_rech_tot_trx_pre_with_covid_ratio")
            ).otherwise(f.col("A.fea_rech_tot_trx_pre_with_covid_ratio"))
        ).alias("fea_rech_tot_trx_pre_with_covid_ratio"),
        f.first(
            f.when(
                post_covid_cond, f.col("B.fea_rech_tot_amt_digi_pre_with_covid_ratio")
            ).otherwise(f.col("A.fea_rech_tot_amt_digi_pre_with_covid_ratio"))
        ).alias("fea_rech_tot_amt_digi_pre_with_covid_ratio"),
        f.first(
            f.when(
                post_covid_cond, f.col("B.fea_rech_tot_amt_digi_pre_with_covid_trends")
            ).otherwise(f.col("A.fea_rech_tot_amt_digi_pre_with_covid_trends"))
        ).alias("fea_rech_tot_amt_digi_pre_with_covid_trends"),
        f.first(
            f.when(
                post_covid_cond, f.col("B.fea_rech_bal_pre_with_covid_stddev_ratio")
            ).otherwise(f.col("A.fea_rech_bal_pre_with_covid_stddev_ratio"))
        ).alias("fea_rech_bal_pre_with_covid_stddev_ratio"),
        f.first(
            f.when(
                post_covid_cond, f.col("B.fea_rech_bal_pre_with_covid_avg_ratio")
            ).otherwise(f.col("A.fea_rech_bal_pre_with_covid_avg_ratio"))
        ).alias("fea_rech_bal_pre_with_covid_avg_ratio"),
        f.first(
            f.when(
                post_covid_cond,
                f.col("B.fea_rech_rev_pkg_prchse_product_val_pre_with_covid_ratio"),
            ).otherwise(
                f.col("A.fea_rech_rev_pkg_prchse_product_val_pre_with_covid_ratio")
            )
        ).alias("fea_rech_rev_pkg_prchse_product_val_pre_with_covid_ratio"),
        f.first(
            f.when(
                after_post_covid_cond,
                f.col("B.fea_rech_tot_amt_pre_post_covid_avg_ratio"),
            ).otherwise(f.col("A.fea_rech_tot_amt_pre_post_covid_avg_ratio"))
        ).alias("fea_rech_tot_amt_pre_post_covid_avg_ratio"),
        f.first(
            f.when(
                after_post_covid_cond,
                f.col("B.fea_rech_amt_pre_post_covid_weekly_avg_ratio"),
            ).otherwise(f.col("A.fea_rech_amt_pre_post_covid_weekly_avg_ratio"))
        ).alias("fea_rech_amt_pre_post_covid_weekly_avg_ratio"),
        f.first(
            f.when(
                after_post_covid_cond,
                f.col("B.fea_rech_amt_pre_post_covid_monthly_avg_ratio"),
            ).otherwise(f.col("A.fea_rech_amt_pre_post_covid_monthly_avg_ratio"))
        ).alias("fea_rech_amt_pre_post_covid_monthly_avg_ratio"),
        f.first(
            f.when(
                after_post_covid_cond, f.col("B.fea_rech_tot_trx_pre_post_covid_ratio")
            ).otherwise(f.col("A.fea_rech_tot_trx_pre_post_covid_ratio"))
        ).alias("fea_rech_tot_trx_pre_post_covid_ratio"),
        f.first(
            f.when(
                after_post_covid_cond,
                f.col("B.fea_rech_tot_amt_digi_pre_post_covid_ratio"),
            ).otherwise(f.col("A.fea_rech_tot_amt_digi_pre_post_covid_ratio"))
        ).alias("fea_rech_tot_amt_digi_pre_post_covid_ratio"),
        f.first(
            f.when(
                after_post_covid_cond,
                f.col("B.fea_rech_tot_amt_digi_pre_post_covid_trends"),
            ).otherwise(f.col("A.fea_rech_tot_amt_digi_pre_post_covid_trends"))
        ).alias("fea_rech_tot_amt_digi_pre_post_covid_trends"),
        f.first(
            f.when(
                after_post_covid_cond,
                f.col("B.fea_rech_bal_pre_post_covid_stddev_ratio"),
            ).otherwise(f.col("A.fea_rech_bal_pre_post_covid_stddev_ratio"))
        ).alias("fea_rech_bal_pre_post_covid_stddev_ratio"),
        f.first(
            f.when(
                after_post_covid_cond, f.col("B.fea_rech_bal_pre_post_covid_avg_ratio")
            ).otherwise(f.col("A.fea_rech_bal_pre_post_covid_avg_ratio"))
        ).alias("fea_rech_bal_pre_post_covid_avg_ratio"),
        f.first(
            f.when(
                after_post_covid_cond,
                f.col("B.fea_rech_rev_pkg_prchse_product_val_pre_post_covid_ratio"),
            ).otherwise(
                f.col("A.fea_rech_rev_pkg_prchse_product_val_pre_post_covid_ratio")
            )
        ).alias("fea_rech_rev_pkg_prchse_product_val_pre_post_covid_ratio"),
    )

    #### FEATURE SELECTION
    feature_columns = get_config_based_features_column_names(
        config_feature_recharge["fea_rech_tot_amt_pre_with_covid_avg_ratio"],
        config_feature_recharge["fea_rech_tot_amt_pre_post_covid_avg_ratio"],
        config_feature_recharge["fea_rech_amt_pre_with_covid_weekly_avg_ratio"],
        config_feature_recharge["fea_rech_amt_pre_post_covid_weekly_avg_ratio"],
        config_feature_recharge["fea_rech_amt_pre_with_covid_monthly_avg_ratio"],
        config_feature_recharge["fea_rech_amt_pre_post_covid_monthly_avg_ratio"],
        config_feature_recharge["fea_rech_tot_trx_pre_with_covid_ratio"],
        config_feature_recharge["fea_rech_tot_trx_pre_post_covid_ratio"],
        config_feature_recharge["fea_rech_tot_amt_digi_pre_with_covid_ratio"],
        config_feature_recharge["fea_rech_tot_amt_digi_pre_post_covid_ratio"],
        config_feature_recharge["fea_rech_tot_amt_digi_pre_with_covid_trends"],
        config_feature_recharge["fea_rech_tot_amt_digi_pre_post_covid_trends"],
        config_feature_recharge["fea_rech_bal_pre_with_covid_stddev_ratio"],
        config_feature_recharge["fea_rech_bal_pre_post_covid_stddev_ratio"],
        config_feature_recharge["fea_rech_bal_pre_with_covid_avg_ratio"],
        config_feature_recharge["fea_rech_bal_pre_post_covid_avg_ratio"],
        config_feature_recharge[
            "fea_rech_rev_pkg_prchse_product_val_pre_with_covid_ratio"
        ],
        config_feature_recharge[
            "fea_rech_rev_pkg_prchse_product_val_pre_post_covid_ratio"
        ],
    )

    required_topup_output_columns = get_required_output_columns(
        output_features=feature_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=granularity,
    )

    fea_covid_all = fea_covid_all.select(required_topup_output_columns)

    return fea_covid_all
