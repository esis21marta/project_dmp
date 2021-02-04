Model Factory Pipeline
======================

This pipeline contains the following:

- Data preprocessing:
    Starting from the master table containing all MSISDNs, week starts,
    features and information relevant to the target, produce a table containing
    all MSISDNs, features and the ready target for the week start of interest
    (just before the start date of the campaign).

- Exploratory analysis:
    Plot the variation of the information relevant to the target across week starts,
    and the distribution of the target comparing control vs treatment, taker vs non-takers.
    It also trains models to classify control vs treatment and taker vs non-takers using
    the data before campaign start. It computes the population counts and take up rates.

- Model training & evaluation:
    Trains multiple uplift models on subset of population using cross-validation to
    enable computation of confidence intervals. It trains the models using all the features,
    then selects the top features and repeats the process. Evaluation metrics such as
    cumulative lift, cumulative gain and uplift by quantiles are computed and plotted.

- Helper functions:
    Auxiliary functions used across the codebase.

------

**Contents**

.. toctree::
   :maxdepth: 2

   preprocessing_nodes.rst
   eda_nodes.rst
   model_nodes.rst
   common_functions.rst
