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

from pyspark.sql import SparkSession

from src.dmp.pipelines.dmp._06_qa.data_quality_helper.sample_msisdn import (
    get_sample_fraction,
)


class TestSampleMsisdn:
    """
    Test Sample MSISDN Node
    """

    def test_get_sample_fraction_works_for_sample_size_smaller_than_distinct_msisdn_size(
        self, spark_session: SparkSession
    ):
        """
        When sample count is less than distinct msisdn count it should return fraction less than 1
        Args:
            spark_session:

        Returns:

        """
        actual_fraction = get_sample_fraction(
            sample_count=1_000, distinct_msisdn_count=10_000
        )
        assert actual_fraction == 0.1

    def test_get_sample_fraction_works_for_sample_size_greater_than_distinct_msisdn_size(
        self, spark_session: SparkSession
    ):
        """
        When sample count is more than distinct count it should return 1.0
        Args:
            spark_session:

        Returns:

        """
        actual_fraction = get_sample_fraction(
            sample_count=100_000, distinct_msisdn_count=10_000
        )
        assert actual_fraction == 1.0

    def test_get_sample_fraction_works_for_sample_size_same_as_distinct_msisdn_size(
        self, spark_session: SparkSession
    ):
        """
        When sample count is equal to distinct count it should return 1.0
        Args:
            spark_session:

        Returns:

        """
        actual_fraction = get_sample_fraction(
            sample_count=10_000, distinct_msisdn_count=10_000
        )
        assert actual_fraction == 1.0
