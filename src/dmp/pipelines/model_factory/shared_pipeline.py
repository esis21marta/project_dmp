import schema
from kedro.framework.context import KedroContext
from schema import Schema

from src.dmp.pipelines.model_factory import consts


class ConfigError(Exception):
    pass


def config_checks(project_context: KedroContext):
    """
    This pre-runs checks on the project configuration.  This should fail
    bad configurations **early** rather than after 20 minutes of running

    There could be two reasons you're looking at this code:

    1. You're running the model factory and this code threw an error.
    2. You're running some **other** kedro process,  and this code threw
       an error, and you're wondering why something in the model factory
       is broken.

    Number 1 is easy to fix - you need to fix your model config.

    Number 2 is a little harder. It's likely that you need to fix up the
    first block of code in this function,  which detects if you're doing
    a model factory run. If you can tighten the detection to **exclude**
    your use case, but still **include** the model factory,  then things
    should start working again.

    :param project_context: the kedro context.  This is generally passed
        down in **kwargs** in pipelines

    :raises: ConfigError if there is a config problem
    """

    # Edit here if you're in situation number 2
    is_model_factory_run = "column_selection" in project_context.params

    if not is_model_factory_run:
        return

    try:
        consts.COL_CONF_SCHEMA.validate(project_context.params["column_selection"])
    except Exception:
        msg = (
            "Unexpected value for column_selection key. This means one of two"
            " things. If you are doing things with the **model factory** it means"
            " that your column selections are mis-configured. If you are not doing"
            " things with the model factory, it means that your params file has "
            " a key called 'column_selection' in it, which is confusing the system."
            " Consider re-naming this key, or tightening the condition used to "
            " work out if the model selection tool checks a schema (the `if` "
            " condition above where this error is raised from)."
        )
        raise ConfigError(msg)

    if "use_case" not in project_context.params:
        msg = f"Missing use_case key in config - got config {project_context.params}"
        raise ConfigError(msg)

    use_cases = {"inlife", "fourg", "churn"}
    if project_context.params["use_case"] not in use_cases:
        msg = (
            f"Unrecognised use_case {project_context.params['use_case']} - should be"
            f" one of {use_cases}"
        )
        raise ConfigError(msg)

    # This schema checks the shap settings, while letting all other keys
    # in the parameters pass.
    tmt_v_control_shap_settings_schema = Schema(
        {
            "shap_treatment_vs_control": {
                "n_folds": int,
                "n_iterations": int,
                "train_frac": float,
                "n_features": int,
            },
            schema.Optional(object): object,
        }
    )
    tmt_v_control_shap_settings_schema.validate(project_context.params)

    if "factory_feature_selection_config" not in project_context.params:
        raise ConfigError(
            "Cannot find feature selection config. It should appear in the key"
            " `factory_feature_selection_config`, and it should probably be in"
            " `config/base/parameters.yml`, so that it will be shared by all use cases"
        )
    consts.FEATURE_SELECTION_RUNS_SCHEMA.validate(
        project_context.params["factory_feature_selection_config"]
    )
