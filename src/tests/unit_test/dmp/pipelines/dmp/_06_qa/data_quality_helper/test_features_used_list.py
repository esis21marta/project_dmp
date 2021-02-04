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

import pytest
from pyspark.sql import Row, SparkSession

from src.dmp.pipelines.dmp._06_qa.data_quality_helper.features_list import (
    get_features_used_list,
)


class TestFeaturesUsed:
    @pytest.mark.skip(reason="Fixed in QA PR")
    def test_features_used_list(self, spark_session: SparkSession):
        sc = spark_session.sparkContext
        df_data = sc.parallelize(
            [
                ("123", "2019-10-07", "2"),
                ("456", "2019-10-07", "5"),
                ("789", "2019-10-21", "6"),
                ("123", "2019-10-21", "7"),
            ]
        )

        df_data = spark_session.createDataFrame(
            df_data.map(lambda x: Row(msisdn=x[0], weekstart=x[1], fea_a=x[2]))
        )

        assert sorted(get_features_used_list(df_data, "all")) == ["fea_a"]
