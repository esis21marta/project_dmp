.. py:module:: src.dmp.pipelines.model_factory.preprocessing_nodes
.. py:currentmodule:: src.dmp.pipelines.model_factory.preprocessing_nodes

Data Preprocessing
==================

Description
-----------

Starting from the master table containing all MSISDNs, week starts,
features and information relevant to the target, produce a table containing
all MSISDNs, features and the ready target for the week start of interest
(just before the start date of the campaign).

Functions
---------

.. autofunction:: pai_log_campaign_name
.. autofunction:: pai_log_use_case
.. autofunction:: select_campaign
.. autofunction:: remove_duplicate_msisdn_weekstart
.. autofunction:: add_is_treatment_column
.. autofunction:: get_important_dates
.. autofunction:: add_target_source_col
.. autofunction:: filter_dates
.. autofunction:: add_target
.. autofunction:: select_training_day
.. autofunction:: column_selection
.. autofunction:: feature_selection
.. autofunction:: sample_rows_and_features
.. autofunction:: convert_to_pandas
.. autofunction:: dataset_checks
