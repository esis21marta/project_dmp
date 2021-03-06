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

from utils import get_config_parameters


def check_catalog_tables_exists_wrapper(project_context):
    def check_catalog_tables_exists(_):
        log = logging.getLogger(__name__)
        missing_files = []
        catalog = get_config_parameters(project_context, "catalog")
        parquet_catalog = [
            key
            for key, value in catalog.items()
            if value.get("file_format", "") == "parquet"
        ]
        all_file_exists = True
        for parquet_catalog_item in parquet_catalog:
            try:
                project_context.catalog.load(parquet_catalog_item)
            except:
                log.error(f"QA Check - {parquet_catalog_item} file doesn't exists.")
                missing_files.append(parquet_catalog_item)
                all_file_exists = False
        if not all_file_exists:
            log.error(f"Missing files in catalog {missing_files}")

    return check_catalog_tables_exists
