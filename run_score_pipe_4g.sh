# ############################################ running 4g #############################################

###### how to run 4g training ######
#First run newest library
#second run preprocessing
#Thirdh run training
####################################

###### how to run 4g scoring ######
#First run newest library
#second run preprocessing
#fourth run scoring
###################################

##########################################################################################################################################
## First run newest library ##
pip3 install --upgrade pip
pip3 install -r src/requirements.txt
pip3 install -r src/requirements.txt
pip3 install -r src/requirements.txt
pip3 install -r src/requirements.txt
kedro install

## second run preprocessing ##
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_segmentation

## Thirdh run training ##
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering --from-nodes=start_complete --to-nodes="picking a segment"
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering --node=count_missing
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering --from-nodes="casting cols with decimals to float" --to-nodes=PysparktoPandas
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering --node=remove_positive_4g
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering --node=remove_unit_of_analysis
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering --node=Transforming
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering --node=plot_histogram
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering --node=separate_numerical_categorical
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering --node=pca_dimension_reduction
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering --node=Tranform_dimension
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering --node=Kmeans_Clustering_fit
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering --node=Kmeans_Clustering_predict_pca
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering --node=reporting1
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering --node=reporting2
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering --node=reporting3
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering --node=reporting4
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering --node=reporting5

## fourth run scoring ##
# please install this scikit-learn==0.22.1 version due to pickle version when create cluster, if using new version scikit learn, please skip this install process
pip3 install scikit-learn==0.22.1
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --to-nodes="Splitting Data sets (scoring)"
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_0_.PysparktoPandas --to-nodes=portion_0_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_1_.PysparktoPandas --to-nodes=portion_1_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_2_.PysparktoPandas --to-nodes=portion_2_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_3_.PysparktoPandas --to-nodes=portion_3_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_4_.PysparktoPandas --to-nodes=portion_4_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_5_.PysparktoPandas --to-nodes=portion_5_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_6_.PysparktoPandas --to-nodes=portion_6_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_7_.PysparktoPandas --to-nodes=portion_7_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_8_.PysparktoPandas --to-nodes=portion_8_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_9_.PysparktoPandas --to-nodes=portion_9_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_10_.PysparktoPandas --to-nodes=portion_10_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_11_.PysparktoPandas --to-nodes=portion_11_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_12_.PysparktoPandas --to-nodes=portion_12_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_13_.PysparktoPandas --to-nodes=portion_13_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_14_.PysparktoPandas --to-nodes=portion_14_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_15_.PysparktoPandas --to-nodes=portion_15_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_16_.PysparktoPandas --to-nodes=portion_16_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_17_.PysparktoPandas --to-nodes=portion_17_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_18_.PysparktoPandas --to-nodes=portion_18_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_19_.PysparktoPandas --to-nodes=portion_19_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_20_.PysparktoPandas --to-nodes=portion_20_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_21_.PysparktoPandas --to-nodes=portion_21_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_22_.PysparktoPandas --to-nodes=portion_22_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_23_.PysparktoPandas --to-nodes=portion_23_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_24_.PysparktoPandas --to-nodes=portion_24_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_25_.PysparktoPandas --to-nodes=portion_25_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_26_.PysparktoPandas --to-nodes=portion_26_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_27_.PysparktoPandas --to-nodes=portion_27_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_28_.PysparktoPandas --to-nodes=portion_28_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_29_.PysparktoPandas --to-nodes=portion_29_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_30_.PysparktoPandas --to-nodes=portion_30_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_31_.PysparktoPandas --to-nodes=portion_31_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_32_.PysparktoPandas --to-nodes=portion_32_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_33_.PysparktoPandas --to-nodes=portion_33_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_34_.PysparktoPandas --to-nodes=portion_34_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_35_.PysparktoPandas --to-nodes=portion_35_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_36_.PysparktoPandas --to-nodes=portion_36_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_37_.PysparktoPandas --to-nodes=portion_37_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_38_.PysparktoPandas --to-nodes=portion_38_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_39_.PysparktoPandas --to-nodes=portion_39_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_40_.PysparktoPandas --to-nodes=portion_40_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_41_.PysparktoPandas --to-nodes=portion_41_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_42_.PysparktoPandas --to-nodes=portion_42_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_43_.PysparktoPandas --to-nodes=portion_43_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_44_.PysparktoPandas --to-nodes=portion_44_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_45_.PysparktoPandas --to-nodes=portion_45_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_46_.PysparktoPandas --to-nodes=portion_46_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_47_.PysparktoPandas --to-nodes=portion_47_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_48_.PysparktoPandas --to-nodes=portion_48_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_49_.PysparktoPandas --to-nodes=portion_49_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_50_.PysparktoPandas --to-nodes=portion_50_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_51_.PysparktoPandas --to-nodes=portion_51_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_52_.PysparktoPandas --to-nodes=portion_52_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_53_.PysparktoPandas --to-nodes=portion_53_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_54_.PysparktoPandas --to-nodes=portion_54_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_55_.PysparktoPandas --to-nodes=portion_55_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_56_.PysparktoPandas --to-nodes=portion_56_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_57_.PysparktoPandas --to-nodes=portion_57_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_58_.PysparktoPandas --to-nodes=portion_58_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_59_.PysparktoPandas --to-nodes=portion_59_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_60_.PysparktoPandas --to-nodes=portion_60_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_61_.PysparktoPandas --to-nodes=portion_61_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_62_.PysparktoPandas --to-nodes=portion_62_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_63_.PysparktoPandas --to-nodes=portion_63_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_64_.PysparktoPandas --to-nodes=portion_64_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_65_.PysparktoPandas --to-nodes=portion_65_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_66_.PysparktoPandas --to-nodes=portion_66_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_67_.PysparktoPandas --to-nodes=portion_67_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_68_.PysparktoPandas --to-nodes=portion_68_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_69_.PysparktoPandas --to-nodes=portion_69_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_70_.PysparktoPandas --to-nodes=portion_70_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_71_.PysparktoPandas --to-nodes=portion_71_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_72_.PysparktoPandas --to-nodes=portion_72_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_73_.PysparktoPandas --to-nodes=portion_73_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_74_.PysparktoPandas --to-nodes=portion_74_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_75_.PysparktoPandas --to-nodes=portion_75_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_76_.PysparktoPandas --to-nodes=portion_76_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_77_.PysparktoPandas --to-nodes=portion_77_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_78_.PysparktoPandas --to-nodes=portion_78_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --from-nodes=portion_79_.PysparktoPandas --to-nodes=portion_79_.output_
kedro run --env=internal/fourg/clustering_202001 --pipeline=fg_clustering_scoring --node=concatenate_results_of_clustering
##########################################################################################################################################
