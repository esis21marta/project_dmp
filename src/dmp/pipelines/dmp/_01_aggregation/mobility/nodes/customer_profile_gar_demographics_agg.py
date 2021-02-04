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

from utils import encode


def mobility_customer_profile_gar_demographics(
    ext_gar_demographics: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    # defining the udf for geohash encode and neighbour geohash

    geo_encode_udf = f.udf(lambda x, y: encode(x, y, precision=5), t.StringType())

    df = ext_gar_demographics.withColumn(
        "geohash",
        geo_encode_udf(ext_gar_demographics.latitude, ext_gar_demographics.longitude),
    )

    df_gar_demographics = df.groupBy("geohash").agg(
        f.avg(f.col("emp_agr")).alias("emp_agr"),
        f.avg(f.col("emp_gov")).alias("emp_gov"),
        f.avg(f.col("emp_ind")).alias("emp_ind"),
        f.avg(f.col("emp_ser")).alias("emp_ser"),
        f.avg(f.col("ic_high")).alias("ic_high"),
        f.avg(f.col("ic_mhg")).alias("ic_mhg"),
        f.avg(f.col("ic_mlw")).alias("ic_mlw"),
        f.avg(f.col("ic_low")).alias("ic_low"),
        f.avg(f.col("tot_val")).alias("tot_val"),
    )

    return df_gar_demographics
