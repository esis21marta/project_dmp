import logging

from kedro.framework.context import KedroContext

import src.dmp.pipelines.model_factory.consts

from . import common_functions, propensity_pipelines, shared_pipeline, uplift_pipelines

__all__ = [
    "create_preprocessing_pipeline",
    "create_pipeline",
    "create_eda_pipeline",
]


def get_use_case(**kwargs):
    try:
        kedro_ctx: KedroContext = kwargs["project_context"]
    except KeyError:
        raise KeyError(
            "Unable to find the project context in keyword args,"
            " be sure to pass **kwargs down the chain of pipelines."
        )

    # Our use-case comes from params, but if it's missing, it means that
    # the pipeline is  probably being run in some non-model factory way.
    # In that case, we just need to pass back **any** pipeline, so we go
    # with inlife.
    if "use_case" in kedro_ctx.params:
        use_case = kedro_ctx.params["use_case"]
        logging.debug(f"Detected use case {use_case}")
    else:
        logging.debug(f"No use-case detected, using inlife for default")
        use_case = "inlife"

    return use_case


def create_preprocessing_pipeline(**kwargs):
    use_case = get_use_case(**kwargs)
    if use_case in {"inlife", "fourg"}:
        return uplift_pipelines.create_preprocessing_pipeline(**kwargs)
    else:
        return propensity_pipelines.create_preprocessing_pipeline(**kwargs)


def create_pipeline(**kwargs):
    create_pipe = create_preprocessing_pipeline(**kwargs)
    eda_pipe = create_eda_pipeline(**kwargs)
    model_pipeline = create_model_pipeline(**kwargs)

    if model_pipeline:
        return create_pipe + eda_pipe + model_pipeline
    else:
        return create_pipe + eda_pipe


def create_eda_pipeline(**kwargs):
    use_case = get_use_case(**kwargs)
    if use_case in {"inlife", "fourg"}:
        return uplift_pipelines.create_eda_pipeline(**kwargs)
    else:
        return propensity_pipelines.create_eda_pipeline(**kwargs)


def create_model_pipeline(**kwargs):
    use_case = get_use_case(**kwargs)
    if use_case in {"inlife", "fourg"}:
        return uplift_pipelines.create_model_pipeline(**kwargs)
    else:
        return propensity_pipelines.create_model_pipeline(**kwargs)


def kedro_inject_with_name(names):
    """
    This can be used as a kedro node when you'd like take several values
    from the data dict, based on name,  and turn them into a dict to use
    in your pipeline.

    :param names:
    :return:
    """

    def injector(*values):
        return dict(zip(names, values))

    return injector
