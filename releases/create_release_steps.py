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

"""project-thanatos
"""

import json
import os
import pathlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict

from kedro.framework.context import load_context

from utils import GetKedroContext, get_config_parameters

from .release_files import FILES_TO_MOVE, HDFS_BASE_PATH, RELEASE_BRANCH

# Absolute path of current file
base_path = pathlib.Path(__file__).parent.absolute()


def _get_file_path_from_catalog(catalog_name: str):
    return get_config_parameters(config="catalog").get(catalog_name)["filepath"]


def _get_partition_column_from_catalog(catalog_name: str):
    return {
        "partition_column": (
            get_config_parameters(config="catalog")
            .get(catalog_name)
            .get("load_args")
            .get("partition_column")
        ),
        "partition_column_date_format": (
            get_config_parameters(config="catalog")
            .get(catalog_name)
            .get("load_args")
            .get("partition_date_format")
        ),
    }


def _create_release_step(
    key: str, relative_path: str, stage_flag: bool, prod_flag: bool, old_steps: Dict
):
    old_step = old_steps.get(key, None)
    if old_step:
        stage_flag = old_step.get("stage", False)
        prod_flag = old_step.get("prod", False)
    return {
        "dev_file_path": os.path.join(HDFS_BASE_PATH["dev"], relative_path),
        "stage_file_path": os.path.join(HDFS_BASE_PATH["stage"], relative_path),
        "prod_file_path": os.path.join(HDFS_BASE_PATH["prod"], relative_path),
        "stage": stage_flag,
        "prod": prod_flag,
    }


def create_release_steps() -> None:
    env = "dmp_production"
    project_context = load_context(Path.cwd(), env=env)
    GetKedroContext(context=project_context, env=env)

    # Absolute path to Release Steps File
    release_file = os.path.join(base_path, f"release_steps/{RELEASE_BRANCH}.json")

    # Read Existing Release Steps File
    if os.path.exists(release_file):
        with open(release_file, "r") as rf:
            old_steps = json.load(rf)
    else:
        old_steps = {}

    # Creating Release Steps Dictionary
    release_steps = {}
    for key, extras in FILES_TO_MOVE.items():
        stage_flag = False  # False means Steps hasn't ran yet on stage
        prod_flag = False  # False means Steps hasn't ran yet on stage

        relative_path = _get_file_path_from_catalog(key)

        first_weekstart = extras.get("first_weekstart")
        last_weekstart = extras.get("last_weekstart")

        if first_weekstart and last_weekstart:
            partition = _get_partition_column_from_catalog(key)

            first_weekstart = datetime.strptime(first_weekstart, "%Y-%m-%d").date()
            last_weekstart = datetime.strptime(last_weekstart, "%Y-%m-%d").date()
            current_weekstart = first_weekstart
            while current_weekstart <= last_weekstart:
                str_date = f"{partition['partition_column']}={current_weekstart.strftime(partition['partition_column_date_format'])}"
                release_steps[f"{key}_{str_date}"] = _create_release_step(
                    f"{key}_{str_date}",
                    os.path.join(relative_path, str_date),
                    stage_flag,
                    prod_flag,
                    old_steps,
                )
                current_weekstart = current_weekstart + timedelta(days=7)
        else:
            release_steps[key] = _create_release_step(
                key, relative_path, stage_flag, prod_flag, old_steps
            )

    # Writing Release Steps Dictionary into Release Steps File
    with open(release_file, "w") as rs:
        rs.write(json.dumps(release_steps, indent=4))
