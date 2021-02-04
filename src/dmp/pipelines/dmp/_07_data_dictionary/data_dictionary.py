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

from src.dmp.pipelines.dmp._07_data_dictionary.data_dictionary_helper import write_yaml


def data_dictionary(
    data_dictionary_column_tabular, data_dictionary_table: str,
):
    """Combine Data Dictionary for columns and tables

    Arguments:
        data_dictionary_column {pyspark.sql.DataFrame} -- Data dictionary for columns
        data_dictionary_table {pyspark.sql.DataFrame} -- Data dictionary for tables

    Returns:
        pyspark.sql.DataFrame -- Combined data dictionary for tables and columns
    """
    data_dict_columns = {
        catalog_table_name.strip(): {"columns": {}}
        for catalog_table_name in set(data_dictionary_column_tabular["Table Name"])
    }

    for item in data_dictionary_column_tabular.to_dict(orient="records"):
        data_dict_columns[item["Table Name"].strip()]["columns"][
            item["Data Element"]
        ] = {
            "business_term": item["Business Term"],
            "description": item["Description"],
            "biz_logic": item["Biz Logic"],
            "origin_source_data": [
                x.strip() for x in str(item["Origin (Source Data)"]).split(",")
            ],
            "sensitivity": item["Sensitivity (PII/non PII)"],
            "impact": item["Impact"],
            "time_aggregation": item["Time Aggregation"],
            "possible_values": item.get("Possible Values", ""),
        }
    for table_name in data_dictionary_table.keys():
        data_dictionary_table[table_name]["columns"] = data_dict_columns.get(
            table_name, {}
        ).get("columns", {})
    write_yaml("docs/data_dictionary/data_dictionary.yaml", data_dictionary_table)
    return None
