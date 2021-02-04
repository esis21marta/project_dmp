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

from src.dmp.pipelines.external.segmentation.common.nodes import clean_kr


def sanity_test(spark_session: SparkSession):
    sc = spark_session.sparkContext

    data = sc.parallelize(
        [
            ("first_user_number", "xxx", "xxx"),
            ("11", "2019-06-14", "good"),
            ("12", "2019-06-14", "bad"),
            ("13", "2019-06-14", "indeterminate"),
            ("14", "2019-06-14", "bad"),
            ("14", "2019-06-14", "bad"),
            ("16", "2019-04-14", "good"),
        ]
    )

    df = spark_session.createDataFrame(
        data.map(lambda x: Row(msisdn=x[0], application_date=x[1], flag_bad_61=x[2]))
    )

    out = clean_kr(df, 2019, 6)
    assert out.columns == [
        "msisdn",
        "default_61",
        "kr_customer",
    ]

    out_list = [
        [i[0], str(i[1]), str(i[2])]
        for i in out.select("msisdn", "default_61", "kr_customer").collect()
    ]
    assert len(out_list) == 4
    assert sorted(out_list) == [
        ["11", "0", "1"],
        ["12", "1", "1"],
        ["13", "None", "1"],
        ["14", "1", "1"],
    ]
