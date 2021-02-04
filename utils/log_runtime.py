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

import datetime
import logging

from sqlalchemy import Column, DateTime, MetaData, Numeric, String, Table, create_engine

from .get_kedro_context import GetKedroContext
from .helpers import get_config_parameters

logger = logging.getLogger(__name__)


def get_spark_job_link(project_context):
    return f"http://10.54.3.5:18089/history/{project_context._spark_session.sparkContext.applicationId}"


def log_runtime(runtime_info, context=None):
    """
    :param logging_info: logging info (
    :param context: kedro context
    :return: -
    """
    if context is None:
        context = GetKedroContext.get_context()

    conf_catalog = get_config_parameters(
        project_context=context, config="credentials.yml"
    )

    conf_parameters = get_config_parameters(
        project_context=context, config="parameters.yml"
    )

    conf_spark = get_config_parameters(project_context=context, config="spark.yml")

    if conf_parameters["log_node_runtime"]:
        con = conf_catalog["qa_credentials_con_string"]["con"]
        conf_catalog = get_config_parameters(config="catalog")
        tbl = conf_catalog["logging_runtime_db"]["table_name"]

        ## Reformatting for sqlalchemy
        runtime_info["node_inputs"] = ",".join(runtime_info["node_inputs"])
        runtime_info["node_outputs"] = ",".join(runtime_info["node_outputs"])
        runtime_info["spark_config_queue"] = conf_spark.get("spark.yarn.queue", None)
        runtime_info["node_start_time"] = datetime.datetime.fromtimestamp(
            runtime_info["node_start_time"]
        )
        runtime_info["spark_config_executors"] = conf_spark.get(
            "spark.dynamicAllocation.maxExecutors", None
        )
        runtime_info["spark_config_vcores"] = conf_spark.get(
            "spark.executor.cores", None
        )
        runtime_info["spark_config_memory"] = conf_spark.get(
            "spark.executor.memory", None
        )
        runtime_info["spark_job_link"] = get_spark_job_link(context)

        meta = MetaData()
        table = Table(
            tbl,
            meta,
            Column("node_name", String),
            Column("node_inputs", String),
            Column("node_outputs", String),
            Column("node_start_time", DateTime),
            Column("duration", Numeric),
            Column("status", String),
            Column("spark_config_queue", String),
            Column("spark_config_executors", Numeric),
            Column("spark_config_vcores", Numeric),
            Column("spark_config_memory", String),
            Column("spark_job_link", String),
            Column("runner", String),
        )

        engine = create_engine(con, echo=False)
        with engine.connect() as connection:
            connection.execute(table.insert(), runtime_info)
