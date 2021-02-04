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


import configparser
import logging
import os
from datetime import datetime

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
curr_path = os.path.abspath(os.path.dirname(__file__))

config_parser = configparser.ConfigParser()
config_parser.read(os.path.join(curr_path, "..", "schema.ini"))
sections_dict = config_parser.__dict__["_sections"].copy()
sections = list(sections_dict.keys())


def gen_random_list(attr, datatype):
    _func = ""
    datatype = str(datatype).lower()
    d1 = datetime.strptime("1/1/2019 1:30 PM", "%m/%d/%Y %I:%M %p")
    d2 = datetime.strptime("8/1/2019 4:50 AM", "%m/%d/%Y %I:%M %p")
    if attr == "msisdn":
        _func = "gen_random_id()"
    elif attr == "calltype":
        _func = "gen_random_calltype()"
    elif attr == "anumber":
        _func = "gen_random_id()"
    elif attr == "bnumber":
        _func = "gen_random_id()"
    elif attr == "total_duration":
        _func = "gen_random_id()"
    elif attr == "total_trx":
        _func = "gen_random_id()"
    elif str(attr).strip().lower() == "tot_trx":
        _func = "gen_random_int(maxm=50)"
    elif "varchar" in datatype:
        _func = "gen_random_string(20)"
    elif datatype == "date_yyyy_mm_dd":
        _func = "gen_random_date(d1, d2, True)"
    elif datatype == "date_yyyymm":
        _func = "gen_date_yyyymm()"
    elif datatype == "int":
        _func = "gen_random_int()"
    elif "bigint" in datatype:
        _func = "gen_random_int(True)"
    elif datatype == "binary":
        _func = "gen_random_binary_int()"
    elif "decimal" in datatype:
        precision = eval(datatype[datatype.find("(") :])
        _func = "gen_random_decimal(precision[0], precision[1])"
    elif "timestamp" in datatype:
        _func = "gen_random_date(d1, d2)"
    elif "dayofweek" in datatype:
        _func = "gen_random_day_of_week()"
    elif datatype == "time_hh24_mm_ss":
        _func = "gen_random_time_hh_mm_ss()"

    randomly_gen_values = []
    for _ in range(0, 500):
        randomly_gen_values.append(eval(_func))
    return randomly_gen_values


for section in sections:
    section_list = []

    for k, v in sections_dict[section].items():
        section_list.append(gen_random_list(k, v))

    target_path = os.path.join(
        curr_path, "..", "..", "data", "01_raw", f"{section}.csv"
    )

    if os.path.exists(target_path):
        logger.info(f"{section} already exists, not re-creating")

    else:
        df = pd.DataFrame(section_list).T
        df.columns = sections_dict[section].keys()
        df.to_csv(target_path, index=False)

        logger.info(f"Generating random data for {section}")
