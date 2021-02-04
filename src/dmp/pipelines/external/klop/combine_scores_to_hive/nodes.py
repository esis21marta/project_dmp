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

from datetime import date
from typing import Any, Dict

import pyspark
from pyspark.sql import functions as f
from pyspark.sql.types import StringType
from pyspark.sql.window import Window


def join_scores(
    default_probability_table: pyspark.sql.DataFrame,
    income_prediction_table: pyspark.sql.DataFrame,
    propensity_table: pyspark.sql.DataFrame,
    columns_to_include: Dict[Any, Any],
) -> pyspark.sql.DataFrame:
    """
    Combines three scores : default probability, income prediction, and propensity, into one single table in Hive.

    :param default_probability_table: Dataframe containing the default probability score
    :param income_prediction_table: Dataframe containing the income prediction score
    :param propensity_table: Dataframe containing the loan propensity score
    :param columns_to_include: Dictionary columns to be included for each of the scoring table

    :return: Joined dataframe table
    """
    default_probability_table = default_probability_table.select(
        columns_to_include["default_probability_table"]
    )
    income_prediction_table = income_prediction_table.select(
        columns_to_include["income_prediction_table"]
    )
    propensity_table = propensity_table.select(columns_to_include["propensity_table"])

    joined_table = default_probability_table.join(
        income_prediction_table, "msisdn"
    ).join(propensity_table, "msisdn")

    return joined_table


def combine_scores_to_hive(
    joined_table: pyspark.sql.DataFrame,
    default_postgres_columns: Dict[Any, Any],
    columns_to_rename: Dict[Any, Any],
) -> pyspark.sql.DataFrame:
    """
    Combines three scores : default probability, income prediction, and propensity, into one single table in Hive.

    :param joined_table: Dataframe containing the filtered MSISDN
    :param default_postgres_columns: Default value for postgres table which are not included in the scoring table
    :param columns_to_rename: Columns to be renamed so that it matches the postgres Klop table accordingly
    :return: Dataframe with multiple scores and default values to be inserted to Klop Scoring table
    """

    # add additional columns required by Klop DB Postgres format
    for key, value in default_postgres_columns.items():
        value = value if value is not "None" else None
        joined_table = joined_table.withColumn(key, f.lit(value))

    # rename columns accordingly
    for key, value in columns_to_rename.items():
        joined_table = joined_table.withColumnRenamed(key, value)

    today = date.today()
    joined_table = joined_table.withColumn(
        "refresh_date", f.lit(today).cast(StringType())
    )

    return joined_table


def add_features_flags(
    scoring_table: pyspark.sql.DataFrame, features_flags: Dict[Any, Any],
) -> pyspark.sql.DataFrame:
    """
    Add additional flags of 0/1 for FB, IG, and Data User.

    :param scoring_table: Dataframe containing the scoring table along with the scores
    :param features_flags: Dictionary containing the features to be flagged, as well as the renamed columns accordingly.
    :return: Dataframe containing the scoring table along with the features flags.
    """

    for feature_name, column_renamed in features_flags.items():
        scoring_table = scoring_table.withColumn(
            column_renamed, f.when(f.col(feature_name) > 0, 1).otherwise(0)
        )

    return scoring_table


def add_quantiles_flag(scoring_table: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Based on the predefined features and quantiles threshold, add the quantiles label as columns.

    :param scoring_table: Dataframe containing the scoring table
    :return: Dataframe containing the scoring table along with the quantiles flags.
    """

    scoring_table = scoring_table.withColumn(
        "quantile_risk",
        f.ntile(4).over(Window.partitionBy().orderBy("default_probability_score")),
    ).withColumn(
        "quantile_prop",
        f.ntile(4).over(Window.partitionBy().orderBy("loan_propensity")),
    )

    return scoring_table
