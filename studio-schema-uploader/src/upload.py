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

import argparse
import logging

import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

import studio
from src.datatype_mapper import map_datatype
from src.helpers import concat_rows, remove_rows

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

conf = SparkConf().setMaster("local").setAppName("STUDIO_SCHEMA_UPLOADER")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

parser = argparse.ArgumentParser(description="Uploads Schema To Studio")
parser.add_argument("-o", "--omit_rows", help="Number Of Rows To Omit", default=0)
required_arguments = parser.add_argument_group("required arguments")
required_arguments.add_argument(
    "-f", "--filename", help="Excel File Name, Needs Absolute Path", required=True
)
required_arguments.add_argument(
    "-s", "--sheetname", help="Excel Sheet Name", required=True
)
required_arguments.add_argument("--data_set", help="Data Set Name", required=True)
required_arguments.add_argument("--data_source", help="Data Source Name", required=True)
required_arguments.add_argument(
    "-c",
    "--cols",
    type=str,
    help="List Of Columns To Be Used For (Column Name, Type, Description)",
    required=True,
)
parser = parser.parse_args()

excel_file_name = parser.filename
sheet_name = parser.sheetname
omit_rows = int(parser.omit_rows)
cols = parser.cols.split(",")
cols = [col.strip() for col in cols]

logger.info("EXCEL FILE NAME: {}".format(excel_file_name))
logger.info("SHEET NAME: {}".format(sheet_name))
logger.info("OMIT ROWS: {}".format(omit_rows))
logger.info("SELECTED COLUMNS: {}".format(cols))

df = pd.read_excel(excel_file_name, sheet_name=sheet_name)

if omit_rows > 0:
    df = remove_rows(df, omit_rows)

description_cols = cols[2:]
df["Description"] = df[description_cols].apply(concat_rows, args=(cols,), axis=1)

cols = cols[:2] + ["Description"]
df = df[cols]
df.columns = ["Name", "Type", "Description"]

df = df.dropna()
df["Type"] = df["Type"].apply(map_datatype)

data_set = parser.data_set
data_source = parser.data_source
studio.create_schema(
    df=df, data_set_name=data_set, data_source_name=data_source, is_schema_df=True
)
