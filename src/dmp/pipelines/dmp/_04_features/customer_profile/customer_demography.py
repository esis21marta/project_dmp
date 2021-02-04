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
import pyspark.sql.functions as f

from utils import get_window_to_pick_last_row, next_week_start_day


def feat_customer_demographics(
    lynx_demography_df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Return telco account profile demographic features as modeled by Project Lynx's customer demographic segementation

    Args:
        lynx_demography_df: Project Lynx input table

    Returns:
        df:
        - msisdn: Unique identifier
        - weekstart: start of the week for the transaction
        - age: predicted age of msisdn
        - age_prob: confidence of age prediction
        - gender: predicted gender of msisdn
        - gender_prob: confidence of gender prediction
        - marital_status: predicted marital status of msisdn
        - marital_status_prob: confidence of marital_status prediction
        - household_status: household status of msisdn (e.g. parent)
        - religion: predicted religion of msisdn
        - religion_status: confidence of religion prediction
        - occupation: predicted occupation of msisdn
        - occupation_status: confidence of occupation prediction
        - education: predicted education of msisdn
        - education_status: confidence of education prediction
        - income_category: predicted income_category of msisdn
    """

    demography_output_columns = [
        "msisdn",
        "weekstart",
        "age",
        "age_prob",
        "gender",
        "gender_prob",
        "marital_status",
        "marital_status_prob",
        "household_status",
        "household_status_prob",
        "religion",
        "religion_prob",
        "occupation",
        "occupation_prob",
        "education",
        "education_prob",
        "income_category",
    ]

    lynx_demography_df = (
        lynx_demography_df.withColumn("trx_date", f.to_date("month", "yyyyMM"))
        .withColumn("weekstart", next_week_start_day("trx_date"))
        .withColumn("rank", f.row_number().over(get_window_to_pick_last_row()))
        .where(f.col("rank") == 1)
        .select(demography_output_columns)
        .repartition(800)
    )

    return lynx_demography_df
