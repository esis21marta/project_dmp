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


def fill_bcp_time_series(
    df_weekly_scaffold: pyspark.sql.DataFrame,
    df_app_feature_mapping: pyspark.sql.DataFrame,
    df_data: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Creates and fill for weekly time series for bcp usage tables

    Args:
        df_weekly_scaffold: Dataframe which contains all week information.
        df_app_feature_mapping: Application to Feature Mapping Mapping.
        df_data: Dataframe which contain msisdn from data will be distinct.

    Returns:
        Dataframe with weekly filled time series for bcp usage tables
    """
    # Get All Categories
    df_categories = df_app_feature_mapping.select("category").distinct()

    # Creating final scaffold
    df_scaffold = df_weekly_scaffold.crossJoin(f.broadcast(df_categories))

    df_data = df_scaffold.join(df_data, ["msisdn", "weekstart", "category"], how="left")

    return df_data
