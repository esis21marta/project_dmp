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

"""Application entry point."""
import logging
import os
import re
from abc import ABC
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Type, Union

from kedro.framework.context import KedroContext, load_context
from kedro.pipeline import Pipeline
from kedro.runner import AbstractRunner
from pyspark import SparkConf
from pyspark.sql import SparkSession

from src.dmp.pipeline import create_pipelines
from utils import GetKedroContext

logger = logging.getLogger(__name__)


class ProjectContext(KedroContext, ABC):
    """Users can override the remaining methods from the parent class here, or create new ones
    (e.g. as required by plugins)

    """

    def handle_stringy_param(self, param, default) -> bool:
        res = self.params.get(param, default)
        if isinstance(res, str):
            res = res.lower().startswith("t")

        assert isinstance(res, bool)
        return res

    def __init__(
        self,
        project_path: Union[Path, str],
        env: str = None,
        desc: str = None,
        extra_params: dict = None,
    ):
        if extra_params is None:
            extra_params = {}

        for date_column in ["first_weekstart", "last_weekstart"]:
            if date_column in extra_params:
                extra_params[date_column] = datetime.strptime(
                    extra_params[date_column], "%Y-%m-%d"
                ).date()

        super().__init__(project_path, env, extra_params)

        use_spark = self.handle_stringy_param("use_spark", True)
        if use_spark:
            self._spark_session = None
            self.init_spark_session(env, desc)

    def init_spark_session(self, env=None, desc=None, yarn=True) -> None:
        """Initializes a SparkSession using the config defined in project's conf folder."""

        if self._spark_session:
            return self._spark_session
        parameters = self.config_loader.get("spark*", "spark*/**")
        spark_conf = SparkConf().setAll(parameters.items())
        if desc:
            desc = re.sub(r"\s+", "_", desc.strip().lower())
            app_name = f"{self.project_name}-{desc}"
        else:
            app_name = self.project_name

        spark_session_conf = (
            SparkSession.builder.appName(app_name)
            .enableHiveSupport()
            .config(conf=spark_conf)
        )
        if yarn:
            self._spark_session = spark_session_conf.master("yarn").getOrCreate()
        else:
            self._spark_session = spark_session_conf.getOrCreate()

        self._spark_session.sparkContext.setLogLevel("WARN")
        utils_path = os.path.join(Path.cwd(), "external_lib/utils.zip")
        self._spark_session.sparkContext.addPyFile(utils_path)
        ml_facade_path = os.path.join(Path.cwd(), "external_lib/ml_facade.zip")
        self._spark_session.sparkContext.addPyFile(ml_facade_path)
        logger.info(
            f"Spark Application ID: {self._spark_session.sparkContext.applicationId}"
        )
        logger.info(f"Kedro Environment: {env}")

    project_name = "project_darwin"
    project_version = "0.16.4"

    def _get_pipelines(self) -> Dict[str, Pipeline]:
        return create_pipelines(project_context=self)

    def _get_feed_dict(self) -> Dict[str, Any]:
        """Get parameters and return the feed dictionary."""
        params = self.params
        feed_dict = {"parameters": params}

        def _add_param_to_feed_dict(param_name, param_value):
            key = "params:{}".format(param_name)
            feed_dict[key] = param_value

            if isinstance(param_value, dict):
                for key, val in param_value.items():
                    _add_param_to_feed_dict("{}.{}".format(param_name, key), val)

        for param_name, param_value in params.items():
            _add_param_to_feed_dict(param_name, param_value)

        return feed_dict


def main(
    tags: Iterable[str] = None,
    env: str = None,
    runner: Type[AbstractRunner] = None,
    node_names: Iterable[str] = None,
    from_nodes: Iterable[str] = None,
    to_nodes: Iterable[str] = None,
    from_inputs: Iterable[str] = None,
):
    """Application main entry point.

    Args:
        tags: An optional list of node tags which should be used to
            filter the nodes of the ``Pipeline``. If specified, only the nodes
            containing *any* of these tags will be run.
        env: An optional parameter specifying the environment in which
            the ``Pipeline`` should be run.
        runner: An optional parameter specifying the runner that you want to run
            the pipeline with.
        node_names: An optional list of node names which should be used to filter
            the nodes of the ``Pipeline``. If specified, only the nodes with these
            names will be run.
        from_nodes: An optional list of node names which should be used as a
            starting point of the new ``Pipeline``.
        to_nodes: An optional list of node names which should be used as an
            end point of the new ``Pipeline``.
        from_inputs: An optional list of input datasets which should be used as a
            starting point of the new ``Pipeline``.

    """
    project_context = load_context(Path.cwd(), env=env)
    GetKedroContext(context=project_context, env=env)
    project_context.run(
        tags=tags,
        runner=runner,
        node_names=node_names,
        from_nodes=from_nodes,
        to_nodes=to_nodes,
        from_inputs=from_inputs,
    )


if __name__ == "__main__":
    main()
