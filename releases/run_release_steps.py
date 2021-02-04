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

import json
import logging
import os
import pathlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from kedro.framework.context import load_context
from pyspark.sql import SparkSession

from utils import GetKedroContext

from .release_files import RELEASE_BRANCH

# Absolute path of current file
base_path = pathlib.Path(__file__).parent.absolute()

log = logging.getLogger(__name__)

# Absolute path to Release Steps File
release_file = os.path.join(base_path, f"release_steps/{RELEASE_BRANCH}.json")
with open(release_file, "r") as rf:
    release_steps = json.load(rf)

tables_copied = 0
total_tables_to_copy = len(release_steps)


def run_release_steps(env: str = "stage") -> None:
    global release_steps
    global tables_copied
    kedro_env = "dmp_production"
    project_context = load_context(Path.cwd(), env=kedro_env)
    GetKedroContext(context=project_context, env=kedro_env)

    valid_envs = ["stage", "prod"]
    if env not in valid_envs:
        raise IOError(
            f"Invalid environment provided: {env}.\n"
            f"Valid values for environment are {valid_envs}"
        )

    source_path = "dev_file_path"
    target_path = "stage_file_path"
    if "prod" == env:
        source_path = "stage_file_path"
        target_path = "prod_file_path"

    # Running Release Steps
    with ThreadPoolExecutor(max_workers=10) as executor:
        for key in release_steps:
            step = release_steps[key]
            # Check if release step is already ran previously or not
            if step[env] is False:
                # Run Release Step
                source_file_path = step[source_path]
                target_file_path = step[target_path]
                # # Get Parent Directory for target path
                # target_file_path = pathlib.Path(target_file_path).parent
                # # Create Parent Directory if not exists
                # _create_directory(file_path=target_file_path)
                # Copy Files to target path
                executor.submit(
                    _copy_files, source_file_path, target_file_path, key, env
                )
            else:
                log.info(f"Skipping {key} (env:{env})")
                tables_copied += 1
                log.info(
                    f"Completed {tables_copied}/{total_tables_to_copy} ({round(tables_copied/total_tables_to_copy*100, 2)}% completed) tasks."
                )


def _get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def _copy_files(source_path: str, target_path: str, key: str, env: str) -> None:
    global release_steps
    global tables_copied
    log.info(f"Coping {source_path} to {target_path}")
    try:
        spark = _get_spark()
        df = spark.read.parquet(source_path)
        df.write.mode("overwrite").parquet(target_path)
        log.info(f"Copied {source_path} to {target_path}")
        # Writing updated Release Steps Dictionary into Release Steps File
        release_steps[key][env] = True
        with open(release_file, "w") as rs:
            json.dump(release_steps, rs)
        tables_copied += 1
        log.info(
            f"Completed {tables_copied}/{total_tables_to_copy} ({round(tables_copied/total_tables_to_copy*100, 2)}% completed) tasks."
        )
    except Exception as e:
        log.error(f"Failed to copy {source_path} to {target_path}")
        log.error(e)
