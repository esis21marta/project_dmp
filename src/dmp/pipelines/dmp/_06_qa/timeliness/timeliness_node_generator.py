from kedro.pipeline import node

from src.dmp.pipelines.dmp._06_qa.data_quality_helper.spark_df_to_oracle_db import (
    spark_df_to_oracle_db_wrapper,
)
from src.dmp.pipelines.dmp._06_qa.timeliness.timeliness import check_timeliness_wrapper
from src.dmp.pipelines.dmp._06_qa.timeliness.timeliness_score import timeliness_score
from utils import get_config_parameters


def get_timeliness_nodes():
    catalog = get_config_parameters(config="parameters_qa")[f"qa_master"]["catalog"]
    nodes = []
    for name, catalog_name in catalog.items():
        nodes = nodes + [
            node(
                func=check_timeliness_wrapper(name, catalog_name),
                inputs=["params:pipeline",],
                outputs=f"timeliness_{name}_metrics",
                name=f"de_qa_timeliness_{name}",
            ),
            node(
                func=spark_df_to_oracle_db_wrapper(
                    target_catalog=f"l7_qa_timeliness_{name}_metrics_db",
                    upsert=True,
                    primary_keys=["table_name", "run_time"],
                ),
                inputs=f"timeliness_{name}_metrics",
                outputs=f"l7_qa_timeliness_{name}_metrics_db",
                name=f"timeliness_{name}_db",
            ),
            node(
                func=timeliness_score,
                inputs=[
                    f"timeliness_{name}_metrics",
                    catalog_name,
                    "params:feature_domain_mapping",
                ],
                outputs=f"timeliness_{name}_score",
                name=f"timeliness_{name}_score",
            ),
            node(
                func=spark_df_to_oracle_db_wrapper(
                    target_catalog=f"l7_qa_timeliness_{name}_score_db",
                    upsert=True,
                    primary_keys=[
                        "run_time",
                        "layer",
                        "master_mode",
                        "dimension",
                        "domain",
                    ],
                ),
                inputs=f"timeliness_{name}_score",
                outputs=f"l7_qa_timeliness_{name}_score_db",
                name=f"timeliness_{name}_score_db",
            ),
        ]
    return nodes
