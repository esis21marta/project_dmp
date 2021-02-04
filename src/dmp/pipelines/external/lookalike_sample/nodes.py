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

from collections import Counter
from math import ceil

import numpy as np
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, lower, trim

num_repartition = 5
random_seed = 42
np.random.seed(random_seed)


def generate_population(
    population_table: pyspark.sql.DataFrame,
    cb_pre_dd: pyspark.sql.DataFrame,
    cb_post_dd: pyspark.sql.DataFrame,
    kr_default_probability: pyspark.sql.DataFrame,
    latest_home_location_month: str,
    date_user_active_on: str,
    home_days_threshold: int,
    territory_filter: list,
) -> pyspark.sql.DataFrame:

    """
    Returns the target population to be sampled on

    :param population_table: Name of the table of the whole customer base with home location
    :param cb_pre_dd: Customer base for prepaid customers
    :param cb_post_dd: Customer base for postpaid customers
    :param kr_default_probability: Master table for default probability training dataset
    :param latest_home_location_month: The referenced month for the population_table
    :param date_user_active_on: The referenced date to obtain active customer from customer base tables
    :param home_days_threshold: The minimum days a user must resides in the territory to be considered
    :param territory_filter: Territory of interest
    :return: The population table ready to be sampled
    """

    filter_population_on_territory = population_table.filter(
        col("mo_id") == latest_home_location_month
    ).filter(
        (
            (lower(trim(col("home1_kabupaten_name"))).isin(territory_filter))
            & (col("home1_days") > home_days_threshold)
        )
        | (
            (lower(trim(col("home2_kabupaten_name"))).isin(territory_filter))
            & (col("home2_days") > home_days_threshold)
        )
        | (
            (lower(trim(col("home3_kabupaten_name"))).isin(territory_filter))
            & (col("home3_days") > home_days_threshold)
        )
        | (
            (lower(trim(col("home4_kabupaten_name"))).isin(territory_filter))
            & (col("home4_days") > home_days_threshold)
        )
    )
    cb_pre_dd = cb_pre_dd.filter(col("event_date") == date_user_active_on)
    cb_pre_dd = cb_pre_dd.withColumn("prepost_flag", lit("prepaid"))
    cb_post_dd = cb_post_dd.filter(col("event_date") == date_user_active_on)
    cb_post_dd = cb_post_dd.withColumn("prepost_flag", lit("postpaid"))
    cb_combined = cb_pre_dd.union(cb_post_dd)

    filter_population_on_territory_los = filter_population_on_territory.alias("A").join(
        cb_combined.select("msisdn", "activation_date", "prepost_flag").alias("B"),
        col("A.imsi") == col("B.msisdn"),
    )

    population_exc_kredivo = filter_population_on_territory_los.join(
        kr_default_probability, "msisdn", how="left_anti"
    )

    return population_exc_kredivo.repartition(num_repartition)


def generate_sample(
    population: pyspark.sql.DataFrame, num_sample: int
) -> pyspark.sql.DataFrame:

    """
    Returns the sampled population

    :param population: the population to be sampled on
    :param num_sample: The number of MSISDN to be sampled
    :return: The sampled population
    """

    no_count = population.count()
    samples = population.sample(False, num_sample / no_count, seed=random_seed)

    return samples.repartition(num_repartition)


def pick_sample(
    kr_default_probability: pyspark.sql.DataFrame, samples: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:

    """
    Returns the sampled population with similar weekstart distribution as the original default probability training dataset

    :param kr_default_probability: Master table for the default probability training dataset
    :param samples: The sampled MSISDN
    :return: The sampled population with similar weekstart distribution as the original default probability training dataset
    """

    spark = SparkSession.builder.getOrCreate()
    kr_default_probability = kr_default_probability.toPandas()

    application_dates = list(
        kr_default_probability["application_date"].str.slice(0, 10)
    )
    application_dates = [
        date for date in application_dates if len(date) == 10 and date[:4] > "2000"
    ]

    samples_msisdn_act_date = samples.toPandas()[["msisdn", "activation_date"]].values
    ratio = len(samples_msisdn_act_date) / len(application_dates)

    application_date_count = dict(Counter(application_dates))
    for application_date in application_date_count:
        application_date_count[application_date] = ceil(
            ratio * application_date_count[application_date]
        )

    results = []
    for (msisdn, activation_date) in samples_msisdn_act_date:
        valid_application_dates = [
            (app_date, count)
            for (app_date, count) in application_date_count.items()
            if app_date > activation_date and count > 0
        ]
        dates = [elt[0] for elt in valid_application_dates]
        counts = [elt[1] for elt in valid_application_dates]
        sum_counts = sum(counts)
        if sum_counts > 0:
            normalized_counts = [count / sum_counts for count in counts]
            selected_application_date = np.random.choice(dates, p=normalized_counts)
            results.append((msisdn, selected_application_date, activation_date))

            application_date_count[selected_application_date] -= 1
            if application_date_count[selected_application_date] <= 0:
                del application_date_count[selected_application_date]

    samples_result = pd.DataFrame(
        results, columns=["msisdn", "application_date", "activation_date"]
    )

    samples_result[["application_date", "activation_date"]] = samples_result[
        ["application_date", "activation_date"]
    ].astype(str)
    samples_result = spark.createDataFrame(samples_result)

    return samples_result.repartition(num_repartition)
