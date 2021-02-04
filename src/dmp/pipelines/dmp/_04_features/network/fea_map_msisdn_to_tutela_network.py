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

from utils import get_end_date, get_start_date


def fea_map_msisdn_to_tutela_network(
    fea_customer_profile: pyspark.sql.DataFrame,
    fea_network_tutela: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:

    fea_customer_profile = fea_customer_profile.select(
        "msisdn", "weekstart", "fea_custprof_kecamatan"
    )

    fea_customer_profile = fea_customer_profile.withColumnRenamed(
        "fea_custprof_kecamatan", "kecamatan"
    )

    fea_network_tutela = fea_network_tutela.withColumnRenamed("location", "kecamatan")

    df_final = fea_customer_profile.join(
        fea_network_tutela, on=["kecamatan", "weekstart"], how="left"
    )

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return df_final.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )
