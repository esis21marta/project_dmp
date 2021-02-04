Model Scoring Pipeline
======================

This pipeline loads trained models from Performance AI,
performs scoring on the input msisdns, selects one campaign
per MSISDN with the highest score, discards campaign-MSISDN combinations
with scores that are below the user specified threshold and saves the output.
It also computes the score correlations between different models
and saves them on Performance AI.

------

**Contents**

.. toctree::
   :maxdepth: 2

   nodes.rst
   utils.rst
