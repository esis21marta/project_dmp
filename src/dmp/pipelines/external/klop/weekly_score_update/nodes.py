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

from typing import Any, Dict

import pyspark


def join_to_whitelist(
    klop_master_whitelist: pyspark.sql.DataFrame,
    joined_score_table: pyspark.sql.DataFrame,
    score_columns: Dict[Any, Any],
) -> pyspark.sql.DataFrame:
    """
    """
    for key, _ in score_columns.items():
        klop_master_whitelist = klop_master_whitelist.drop(key)

    joined_table = klop_master_whitelist.join(
        joined_score_table, on="msisdn", how="left"
    )
    joined_table = joined_table.fillna(1.0, subset=["default_probability_score"])

    return joined_table


def drop_duplicate_msisdns(
    joined_table: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    """
    uniq_table = joined_table.drop_duplicates(subset=["msisdn", "refresh_date"])

    return uniq_table
