import logging

import pyspark

log = logging.getLogger(__name__)


def check_feature_mapping(
    df_feature: pyspark.sql.DataFrame, features_mapping: dict
) -> pyspark.sql.DataFrame:
    """
    Feature mapping to check if there are feature names that changed
    rename changed with existing column in master table

    Args:
        df_feature: Feature DataFrame which have all the Key, Weekstart, and Feature Columns
        feature_mapping: parameters mapping between existing feature name and the one being developed

    Returns:
        df_fea: Checked (and modified) dataframe
    """

    original = set(features_mapping.keys())
    renamed = set(features_mapping.values())
    renamed.discard("")

    # to check if there is overlap between existing feature names and new feature names
    duplicate_columns = original.intersection(renamed)

    if duplicate_columns:
        # To filter column where new feature name is same as existing feature names.
        # It's a rare scenario, but valid.
        actual_duplicate_columns = []
        for column in duplicate_columns:
            if features_mapping[column] != column:
                actual_duplicate_columns.append(column)

        if actual_duplicate_columns:
            raise Exception(
                f"New feature name(s) overlap with existing feature names: {actual_duplicate_columns}. Please check parameters interface file."
            )

    # check for duplication in new feature names
    repeated_values = []
    feature_names = {}
    for val in features_mapping.values():
        if val != "":
            if feature_names.get(val) != None:
                repeated_values.append(val)
            else:
                feature_names[val] = 1

    if repeated_values:
        raise Exception(
            f"New feature name(s) overlap with each other: {repeated_values}. Please check parameters interface file."
        )

    fea_not_present = list(set(df_feature.columns) - set(features_mapping.keys()))

    if fea_not_present:
        log.info(f"Feature(s) not present in the feature interface: {fea_not_present}")

    for column in df_feature.columns:
        if column in features_mapping and features_mapping[column] != "":
            log.info(
                f"Renaming column for feature {column} as {features_mapping[column]}"
            )
            df_feature = df_feature.withColumnRenamed(column, features_mapping[column])

    return df_feature
