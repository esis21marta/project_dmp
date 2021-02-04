.. py:module:: src.dmp.pipelines.model_factory.eda_nodes
.. py:currentmodule:: src.dmp.pipelines.model_factory.eda_nodes

Exploratory Analysis
====================

Description
-----------

Plot the variation of the information relevant to the target across week starts,
and the distribution of the target comparing control vs treatment, taker vs non-takers.
It also trains models to classify control vs treatment and taker vs non-takers using
the data before campaign start. It computes the population counts and take up rates.

Functions
---------

.. autofunction:: plot_target
.. autofunction:: count_populations
.. autofunction:: target_mean_by_group
.. autofunction:: learn_treatment_vs_control
.. autofunction:: learn_taker_vs_nontaker
