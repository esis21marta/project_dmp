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
from pyspark.sql.window import Window


def create_handset_lookup_data(
    df_handset_dim: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Filters any one device per IMEI.

    Args:
        df_handset_dim: Lookup static table for each IMEIs.

    Returns:
        Dataframe with one row (device) per IMEI.
    """

    # Selecting one device per IMEI
    df_handset_dim = df_handset_dim.withColumn(
        "row_number",
        f.row_number().over(Window.partitionBy("tac").orderBy("device_type")),
    )

    df_handset_lookup = df_handset_dim.filter(f.col("row_number") == 1).select(
        "tac", "manufacturer", "market_name", "device_type"
    )

    return df_handset_lookup
