# Introduction
This pipeline trains a cluster algorithm and scores a population.

The inputs are `4g_master_complete`, for scoring, and `4g_master_complete_downsampled`, for training.

# Content of the pipeline

The main steps for are:
- Selecting a segment of interest (Segment meaning #TODO)
- bringing the dataset locally (`to_pandas` command)
- Data cleaning
- Applying PCA
- Applying k-means
- Storing (i.e., bringing the result back to hadoop and saving it)
- Reporting (i.e., running statistics about the final clusters)

The steps are the same for training and scoring, but, due to constrains in the dataset size supported in Pandas,
there are separate pipelines for both. The pipelines call the same nodes, however.
For scoring, before bringing the dataset to the local cdsw machine, we split the dataset into many (default is 15)
different datasets, and run the rest of the pipeline separately for each. as a last step, we merge each pipeline.

# kedro run
In oreder to run the training pipeline run from the command line
```
kedro run --env=cvm --pipeline=fg_clustering
```

To run the Scoring pipeline:
```
kedro run --env=cvm --pipeline=fg_clustering_scoring
```

#PS:
In certain cases, the scoring pipeline fails to execute due to memory constrains. In these cases,
I advice to run the pipeline by parts, like the following
```shell script
kedro run --env=cvm --pipeline=fg_clustering_scoring --to-nodes="Splitting Data sets (scoring)"
kedro run --env=cvm --pipeline=fg_clustering_scoring --from-nodes=portion_0_.PysparktoPandas
kedro run --env=cvm --pipeline=fg_clustering_scoring --from-nodes=portion_1_.PysparktoPandas
kedro run --env=cvm --pipeline=fg_clustering_scoring --from-nodes=portion_2_.PysparktoPandas
...
kedro run --env=cvm --pipeline=fg_clustering_scoring --from-nodes=concatenate_results_of_clustering
```
