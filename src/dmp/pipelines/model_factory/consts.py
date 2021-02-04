import numbers
import os

import schema

TARGET_SOURCE_COL = "target_source"
TARGET_COL = "target"
IS_TREATMENT_COL = "is_treatment"
PREDICTION_COL = "predicted_uplift"
FEATURE_COL = "feature"
IMPORTANCE_COL = "importance"
QUANTILE_COL = "quantile"
MODEL_COL = "model"
RANDOM_COL = "random"
CUMLIFT_COL = "cumlift"
CUMGAIN_COL = "cumgain"
CUMPROP_COL = "cumprop"

COL_CONF_SCHEMA = schema.Schema(
    {
        "required": [str],
        "excluded": [str],
        "exclude_dtype_prefix": [str],
        "exclude_suffix": [str],
        "exclude_prefix": [str],
    }
)

MODEL_MEMBER_SCHEMA = schema.Schema(
    {
        "model datasource": str,
        "output datasource": str,
        "keyword": str,
        "channel": str,
        "criteria 1": numbers.Number,
        schema.Optional("model id"): object,
    }
)

JOINT_EVAL_SCHEMA = schema.Schema(
    {"model members": {str: MODEL_MEMBER_SCHEMA}, schema.Optional(str): object}
)

USE_CACHE_FOR_DATES = os.environ.get("USE_CACHE_FOR_DATES", "false") == "true"

FEATURE_SELECTION_RUN_SCHEMA = schema.Schema(
    {
        "artifact_path": schema.Regex(r"^hdfs://.*$"),
        "loadmethod": schema.Or("csv"),
        "use_case": str,
        "model_name": str,
        schema.Optional("required_features"): [str],
        "model_value": numbers.Number,
    }
)

FEATURE_SELECTION_RUNS_SCHEMA = schema.Schema([FEATURE_SELECTION_RUN_SCHEMA])
