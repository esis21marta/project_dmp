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

import logging

from utils import create_external_hive_table, get_config_parameters
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)


def build_hive_master_table(master: str, master_hive: str):
    def _build_hive_master_table(pipeline: str) -> None:
        if "scoring" == pipeline:
            conf_catalog = get_config_parameters(config="catalog")
            master_catalog = conf_catalog[master]

            master_hive_catalog = conf_catalog[master_hive]

            file_path = get_file_path(filepath=master_catalog["filepath"])
            file_format = master_catalog["file_format"]
            partition_columns = master_catalog["save_args"].pop("partitionBy", None)
            create_versions = master_catalog.pop("create_versions", None)
            if create_versions:
                if type(partition_columns) == list:
                    partition_columns.insert(0, "created_at")
                elif partition_columns is None:
                    partition_columns = ["created_at"]
                else:
                    partition_columns = ["created_at", partition_columns]

            table_schema = master_hive_catalog["database"]
            table_name = master_hive_catalog["table"]
            log.info(f"File Path: {file_path}")
            log.info(f"File Format: {file_format}")
            log.info(f"Partition Columns: {partition_columns}")
            log.info(f"Table Schema: {table_schema}")
            log.info(f"Table Name: {table_name}")

            create_external_hive_table(
                table_schema=table_schema,
                table_name=table_name,
                file_path=file_path,
                file_format=file_format,
                partition_columns=partition_columns,
            )

    return _build_hive_master_table
