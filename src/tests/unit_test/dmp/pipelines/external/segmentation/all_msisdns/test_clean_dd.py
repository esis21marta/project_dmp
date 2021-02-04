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

from pyspark.sql import Row, SparkSession

from src.dmp.pipelines.external.segmentation.all_msisdns.nodes import clean_dd


def sanity_test(spark_session: SparkSession):
    sc = spark_session.sparkContext

    data = sc.parallelize(
        [
            ("1", "2019-06-30", "xxx"),
            ("2", "2019-06-14", "xxx"),
            ("3", "2018-06-14", "xxx"),
            ("4", "2019-05-31", "xxx"),
        ]
    )

    df = spark_session.createDataFrame(
        data.map(lambda x: Row(msisdn=x[0], event_date=x[1], useless_column=x[2]))
    )

    out = clean_dd(df, 2019, 6)
    assert out.columns == [
        "msisdn",
    ]

    out_list = [[i[0]] for i in out.select("msisdn").collect()]
    assert len(out_list) == 1
    assert sorted(out_list) == [
        ["1"],
    ]
