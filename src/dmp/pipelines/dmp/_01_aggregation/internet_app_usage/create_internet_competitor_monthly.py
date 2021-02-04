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

import logging

import pyspark
import pyspark.sql.functions as f
import pyspark.sql.types as t
from dateutil.relativedelta import relativedelta

from utils import get_config_parameters, get_end_date, get_start_date
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)


def _monthly_aggregation(
    df_competitor_monthly: pyspark.sql.DataFrame, month_dt_dict
) -> pyspark.sql.DataFrame:
    """
    Creates monthly Usage for the partner MSISDNs and required categories

    Args:
        df_competitor_monthly: Monthly Usage DataFrame.

    Returns:
        Dataframe with Internet usage monthly aggregated.
    """

    dt_func_udf = f.udf(lambda x: month_dt_dict.get(x), t.StringType())

    df_competitor_monthly = df_competitor_monthly.withColumn(
        "month", f.to_date(f.col("month").cast(t.StringType()), "yyyyMM")
    )
    df_competitor_monthly = df_competitor_monthly.withColumn(
        "month", f.date_format("month", "yyyy-MM")
    )

    df = (
        df_competitor_monthly.groupBy("msisdn", "month")
        .agg(
            f.sum(
                f.when(
                    f.lower(f.col("apps")).isin(
                        [
                            "indihome",
                            "myrepublic",
                            "xlhome",
                            "myrepublicindonesia",
                            "firstmedia",
                        ]
                    ),
                    f.col("trx"),
                ).otherwise(0)
            )
            .cast(t.LongType())
            .alias("comp_websites_visits"),
            f.sum(
                f.when(
                    f.lower(f.col("apps")).isin(
                        [
                            "indihome",
                            "myrepublic",
                            "xlhome",
                            "myrepublicindonesia",
                            "firstmedia",
                        ]
                    ),
                    f.col("payload"),
                ).otherwise(0)
            )
            .cast(t.LongType())
            .alias("comp_websites_consumption"),
            f.sum(
                f.when(
                    f.lower(f.col("apps")).isin(
                        [
                            "bimaplus",
                            "indosatmyim3",
                            "myxl",
                            "smartfren",
                            "indosat",
                            "xlaxiata",
                            "xlhome",
                            "myxlpostpaid",
                            "mysmartfren",
                            "myooredoo",
                        ]
                    ),
                    f.col("trx"),
                ).otherwise(0)
            )
            .cast(t.LongType())
            .alias("comp_apps_websites_visits"),
            f.sum(
                f.when(
                    f.lower(f.col("apps")).isin(
                        [
                            "bimaplus",
                            "indosatmyim3",
                            "myxl",
                            "smartfren",
                            "indosat",
                            "xlaxiata",
                            "xlhome",
                            "myxlpostpaid",
                            "mysmartfren",
                            "myooredoo",
                        ]
                    ),
                    f.col("payload"),
                ).otherwise(0)
            )
            .cast(t.LongType())
            .alias("comp_apps_websites_consumption"),
        )
        .withColumn(
            "month_mapped_dt", f.to_date(dt_func_udf(f.col("month")), "yyyy-MM-dd")
        )
    )
    return df


def create_internet_app_competitor_monthly(
    df_competitor_monthly: pyspark.sql.DataFrame, month_dt_dict
) -> None:
    """
    Creates monthly aggregation for smy BCP usage table.

    Args:
        df_competitor_monthly: Monthly Usage DataFrame.

    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date(period="1cm", context=None, partition_column="month")
    end_date = get_end_date(context=None, partition_column="month", period="1cm")

    monthly_agg_catalog = conf_catalog["l2_internet_app_usage_competitor_monthly"]

    load_args = conf_catalog["l1_internet_app_competitor_monthly"]["load_args"]
    save_args = monthly_agg_catalog["save_args"]
    save_args.pop("partitionBy", None)
    file_path = get_file_path(filepath=monthly_agg_catalog["filepath"])
    file_format = monthly_agg_catalog["file_format"]
    partitions = int(monthly_agg_catalog["partitions"])
    log.info(
        "Starting Daily Aggregation for Dates {start_date} to {end_date}".format(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
        )
    )
    log.info(f"File Path: {file_path}")
    log.info(f"File Format: {file_format}")
    log.info(f"Load Args: {load_args}")
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")

    while start_date < end_date:
        month = start_date.strftime(load_args["partition_date_format"])
        log.info("Starting Monthly Aggregation for month: {}".format(month))

        df_data = df_competitor_monthly.filter(
            f.col(load_args["partition_column"]) == month
        )
        df = _monthly_aggregation(df_data, month_dt_dict).drop(f.col("month"))

        mo_id = start_date.strftime("%Y-%m")
        partition_file_path = "{file_path}/mo_id={month}".format(
            file_path=file_path, month=mo_id
        )
        df.repartition(numPartitions=partitions).write.save(
            partition_file_path, file_format, **save_args
        )

        log.info("Completed Monthly Aggregation for month: {}".format(month))

        start_date = start_date.replace(day=1)

        start_date = start_date + relativedelta(months=1)
