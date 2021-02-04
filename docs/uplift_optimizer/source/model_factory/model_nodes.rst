.. py:module:: src.dmp.pipelines.model_factory.model_nodes
.. py:currentmodule:: src.dmp.pipelines.model_factory.model_nodes

Modeling Training & Evaluation
==============================

Description
-----------

Trains multiple uplift models on subset of population using cross-validation to
enable computation of confidence intervals. It trains the models using all the features,
then selects the top features and repeats the process. Evaluation metrics such as
cumulative lift, cumulative gain and uplift by quantiles are computed and plotted.

Functions
---------

.. autofunction:: create_uplift_model
.. autofunction:: drop_msisdns
.. autofunction:: model_uplift_all_features
.. autofunction:: model_uplift_best_features
.. autofunction:: log_calibrated_model_to_pai
.. autofunction:: log_shap_best_features
