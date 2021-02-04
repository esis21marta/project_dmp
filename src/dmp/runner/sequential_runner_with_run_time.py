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

import time
from collections import Counter
from itertools import chain

from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.runner import SequentialRunner
from kedro.runner.runner import run_node

from utils import log_runtime


class SequentialRunnerWithRunTime(SequentialRunner):
    def _log_run_time(self, node_run_time_info):
        node_run_time_info["duration"] = round(
            (time.time() - node_run_time_info["node_start_time"]) / 60, 3
        )
        log_runtime(node_run_time_info)
        self._logger.info(
            f"Node {node_run_time_info['node_name']} took {node_run_time_info['duration']} minute(s) to execute"
        )

    def _run(
        self, pipeline: Pipeline, catalog: DataCatalog, run_id: str = None
    ) -> None:
        """The method implementing sequential pipeline running.

        Args:
            pipeline: The ``Pipeline`` to run.
            catalog: The ``DataCatalog`` from which to fetch data.

        Raises:
            Exception: in case of any downstream node failure.
        """
        nodes = pipeline.nodes
        done_nodes = set()

        load_counts = Counter(chain.from_iterable(n.inputs for n in nodes))

        for exec_index, node in enumerate(nodes):
            node.node_run_time_info = {
                "node_name": node.name,
                "node_inputs": node.inputs,
                "node_outputs": node.outputs,
                "node_start_time": time.time(),
                "runner": "SequentialRunnerWithRunTime",
            }
            try:
                run_node(node, catalog)
                done_nodes.add(node)
            except Exception:
                node.node_run_time_info["status"] = "FAILED"
                self._log_run_time(node.node_run_time_info)
                self._suggest_resume_scenario(pipeline, done_nodes)
                raise

            # decrement load counts and release any data sets we've finished with
            for data_set in node.inputs:
                load_counts[data_set] -= 1
                if load_counts[data_set] < 1 and data_set not in pipeline.inputs():
                    catalog.release(data_set)
            for data_set in node.outputs:
                if load_counts[data_set] < 1 and data_set not in pipeline.outputs():
                    catalog.release(data_set)

            node.node_run_time_info["status"] = "SUCCESS"
            self._log_run_time(node.node_run_time_info)
            self._logger.info(f"Completed {exec_index + 1} out of {len(nodes)} tasks")
