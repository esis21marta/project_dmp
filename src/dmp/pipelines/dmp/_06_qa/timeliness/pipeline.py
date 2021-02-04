import logging

from kedro.pipeline import Pipeline

from utils import get_config_parameters

from .timeliness_node_generator import get_timeliness_nodes

logger = logging.getLogger(__name__)


def create_pipeline() -> Pipeline:
    pipeline_mode = get_config_parameters(None, "parameters").get("pipeline")

    if pipeline_mode not in ["training", "scoring"]:

        logger.info(f"QA Pipeline is supported only in 'training' or 'scoring'.")
        return Pipeline([])
    return Pipeline(nodes=get_timeliness_nodes(), tags=["de_qa_timeliness"],)
