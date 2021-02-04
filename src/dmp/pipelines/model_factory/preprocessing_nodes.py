import logging
import random
from typing import List, Tuple, Union

import pai
import pandas as pd
import pyspark
import pyspark.sql.functions as f
from great_expectations import dataset as ge_dataset
from great_expectations.core import ExpectationValidationResult
from pyspark.sql import Window
from pyspark.sql.types import StructField

from src.dmp.pipelines.model_factory.consts import (
    COL_CONF_SCHEMA,
    FEATURE_COL,
    IMPORTANCE_COL,
    IS_TREATMENT_COL,
    TARGET_COL,
    TARGET_SOURCE_COL,
)


def pai_log_campaign_name(pai_campaign_name: str) -> None:
    """
    Logging the campaign name to PAI, for production's aggregating function to
    identify campaigns.

    :param pai_campaign_name: Campaign name to be logged by PAI
    """
    pai.log_params({"campaign_name": pai_campaign_name})


def pai_log_use_case(use_case: str) -> None:
    """
    Logging the use case to PAI, for production's aggregating function to
    identify use cases.

    :param use_case: Use case to be logged by PAI
    """
    pai.log_params({"use_case": use_case})


def select_campaign(
    campaign_name_column: str,
    campaign_names: List[str],
    *dataframes: List[pyspark.sql.DataFrame],
) -> pyspark.sql.DataFrame:
    """
    Selects the rows of the input DataFrame that correspond to the campaign of interest.

    :param campaign_name_column: Column containing the campaign names
    :param campaign_names: Names corresponding to the campaign of interest
    :param dataframes: Input DataFrames to select rows from
    :return: DataFrame containing only the campaign
    """

    assert len(dataframes) > 0, "No input dataset !"

    columns_intersection = set(dataframes[0].columns)
    for _df in dataframes[1:]:
        columns_intersection &= set(_df.columns)
    columns_intersection = list(columns_intersection)

    df = (
        dataframes[0]
        .select(columns_intersection)
        .filter(f.col(campaign_name_column).isin(campaign_names))
    )
    for _df in dataframes[1:]:
        df = df.unionByName(
            _df.select(columns_intersection).filter(
                f.col(campaign_name_column).isin(campaign_names)
            )
        )

    return df


def remove_duplicate_msisdn_weekstart(
    df: pyspark.sql.DataFrame, msisdn_column: str, weekstart_column: str,
) -> pyspark.sql.DataFrame:
    """
    Removes the rows of the input DataFrame that contain duplicate msisdn and weekstart pairs.

    :param df: Input DataFrame
    :param msisdn_column: Column containing the msisdns
    :param weekstart_column: Column containing the weekstarts
    :return: DataFrame without duplicated msisdn and weekstart pairs
    """
    return df.drop_duplicates(subset=[msisdn_column, weekstart_column])


def add_is_treatment_column(
    df: pyspark.sql.DataFrame, is_control_column: str,
) -> pyspark.sql.DataFrame:
    """
    Adds IS_TREATMENT column, boolean opposite of the is_control_column.

    :param df: Input DataFrame
    :param is_control_column: Column flagging the control population
    :return: DataFrame with IS_TREATMENT column
    """
    return df.withColumn(IS_TREATMENT_COL, f.lit(1) - f.col(is_control_column))


def drop_kartuhalo(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Drop kartuhalo users from the sample

    :param df: Input DataFrame
    :return: Filtered DataFrame without kartuhalo users
    """
    return apply_generic_filter(df, "lower(fea_custprof_brand) != 'kartuhalo'")


def apply_generic_filter(
    df: pyspark.sql.DataFrame, filter_str: str,
) -> pyspark.sql.DataFrame:
    """
    Applies filter defined by sql - like statement

    :param df: Input DataFrame
    :param filter_str: String with filter to apply
    :return: Filtered DataFrame
    """
    if filter_str is None or filter_str == "":
        return df
    else:
        logging.info(f"Cutting down to '{filter_str}'")
        return df.filter(filter_str)


def add_churn_propensity_target(
    df: pyspark.sql.DataFrame,
    weekstart_col_name: str,
    msisdn_col_name: str,
    target_window_weeks: int,
    target_lag: int,
) -> pyspark.sql.DataFrame:
    """
    Adds churn propensity target to a given dataset `df`.

    The target is defined as follows:
    1 if user is inactive in period that
        starts on `weekstart_col_name` + `target_lag` weeks,
        ends on `weekstart_col_name` + `target_lag` weeks + `target_window_weeks` weeks,
    0 if active in at least one week,

    :param msisdn_col_name: the name of the MSISDN column
    :param weekstart_col_name: the name of the weekstart col
    :param df: Input DataFrame
    :param target_window_weeks: Number of weeks used for definitions of observation start and end
    :param target_lag: Number of weeks preceding target period and features
    :return: DataFrame with churn propensity target column added
    """
    msisdn_col = f.col(msisdn_col_name)
    week_col = f.col(weekstart_col_name)
    target_source_col = f.col(TARGET_SOURCE_COL)
    msisdn_partition = Window.partitionBy(msisdn_col).orderBy(week_col)
    observation_period = msisdn_partition.rowsBetween(
        target_lag, target_lag + target_window_weeks - 1
    )
    is_observation_period_full = (
        f.count("*").over(observation_period) == target_window_weeks
    )
    target_col = f.when(~is_observation_period_full, f.lit(1)).otherwise(
        f.min(target_source_col).over(observation_period)
    )
    return df.withColumn(TARGET_COL, target_col)


def get_important_dates(
    df: pyspark.sql.DataFrame,
    campaign_start_date_column: str,
    campaign_end_date_column: str,
    target_window_weeks: int,
) -> Tuple[str, str, str]:
    """
    Returns observation start date, campaign start Monday and observation end date in format %Y-%m-%d.

    Campaign start Monday is the the Monday before the earliest campaign start date or the start date itself
    if it falls on Monday. The observation start date is chosen so that the number of weekstarts from the date
    (included) until campaign start Monday (excluded) equals target_window_weeks. The observation end date is
    chosen so that the number of weekstarts from the campaign start Monday (included) and the date (included)
    also equals target_window_weeks.

    :param df: Input DataFrame
    :param campaign_start_date_column: Column containing the campaign start dates
    :param campaign_end_date_column: Column containing the campaign end dates
    :param target_window_weeks: Number of weeks used for definitions of observation start and end
    :return: Tuple (observation_start, campaign_start_monday, observation_end)
    """
    treatment_only = df.filter(f.col(IS_TREATMENT_COL) == 1)
    earliest_start_date = treatment_only.select(
        f.min(campaign_start_date_column)
    ).first()[0]
    if earliest_start_date is None:
        raise ValueError(
            "Start date was None - this may indicate there"
            " are now rows in the dataset, possibly due to"
            " an incorrect campaign name or table name in "
            " parameters.yml or catalog.yml. Ensure that you "
            " updated both of them...."
        )
    pai.log_params({"date_campaign_start_date": earliest_start_date})

    if campaign_end_date_column in treatment_only.columns:
        latest_end_date = treatment_only.select(
            f.max(campaign_end_date_column)
        ).first()[0]
        pai.log_params({"date_campaign_end_date": latest_end_date})

    earliest_start_date = pd.to_datetime(earliest_start_date, format="%Y-%m-%d")
    campaign_start_monday = earliest_start_date - pd.Timedelta(
        days=earliest_start_date.weekday()
    )

    observation_start = campaign_start_monday - pd.Timedelta(
        weeks=target_window_weeks - 1
    )
    observation_end = campaign_start_monday + pd.Timedelta(weeks=(target_window_weeks))

    campaign_start_monday = campaign_start_monday.strftime(format="%Y-%m-%d")
    observation_start = observation_start.strftime(format="%Y-%m-%d")
    observation_end = observation_end.strftime(format="%Y-%m-%d")

    pai.log_params({"date_campaign_start_monday": campaign_start_monday})
    pai.log_params({"date_observation_start": observation_start})
    pai.log_params({"date_observation_end": observation_end})

    return observation_start, campaign_start_monday, observation_end


def add_target_source_col(df: pyspark.sql.DataFrame, target_source: dict):
    """
    This adds the `target_source` column to the master data
    table, which will be later on used to compute the target.

    Having this floating around opens some interesting opportunities for
    EDA and checks.

    :param df: The master data table
    :param target_source: the value to calculate. You should pass in
        a string that we can pass into pyspark.sql.functions.expr, which
        is normally an SQL-like expression.

        PAI will record all the parameters used, and by default all string
        parameters are firstly validated if they are files. Hence, it's
        necessary to wrap the target_source string in a dict.  Doing
        this will prevent PAI from trying to be clever, and perhaps causes
        a `filename too long error` if your SQL string is too long.

        This means that your params looks like this:

        target_source:
            pai_workaround: "SQL STRING"

    :return: the original dataframe, with an extra column included with
        the result of the expression, and with name `target_source`.
    """
    expr = target_source["pai_workaround"]
    assert TARGET_SOURCE_COL not in df.columns, "DF already has target source"
    return df.withColumn(TARGET_SOURCE_COL, f.expr(expr))


def filter_dates(
    df: pyspark.sql.DataFrame,
    weekstart_column: str,
    date_min: str = None,
    date_max: str = None,
) -> pyspark.sql.DataFrame:
    """
    Filters out only dates between `date_min` and `date_max`.

    :param df: Input DataFrame
    :param weekstart_column: Column containing the weekstarts
    :param date_min: Minimum date to filter with, if `None` then no limit
    :param date_max: Maximum date to filter with, if `None` then no limit
    :return: DataFrame limited to given time frame
    """
    if date_min is not None:
        df = df.filter(f.col(weekstart_column) >= date_min)
        pai.log_params({"date_of_interest_min": date_min})
    if date_max is not None:
        df = df.filter(f.col(weekstart_column) <= date_max)
        pai.log_params({"date_of_interest_max": date_max})
    return df


def add_target(
    df: pyspark.sql.DataFrame,
    msisdn_col_name: str,
    use_case: str,
    campaign_start_monday: str,
    weekstart_col_name: str,
):
    """
    Add a target column to the table based on the target source and
    use case specific logic.

    :param df: The DataFrame to process
    :param msisdn_col_name: The name of the MSISDN column
    :param use_case: Use case name
    :param campaign_start_monday: Monday the week of the start of the campaign
    :param weekstart_col_name: The name of the week col

    :return: DataFrame with the newly added target column with name `target`.
    """
    IS_PRE = "is_pre"

    msg = f"Target source col does not exist"
    assert TARGET_SOURCE_COL in df.columns, msg
    msg = f"Target col already exists, found {TARGET_COL} in {df.columns}"
    assert TARGET_COL not in df.columns, msg

    msisdn_col = f.col(msisdn_col_name)
    week_col = f.col(weekstart_col_name)
    df = df.withColumn(IS_PRE, week_col <= campaign_start_monday)

    if use_case == "inlife":
        msisdn_before_status = (
            df.filter(f.col(IS_PRE))
            .groupby(msisdn_col_name)
            .agg(f.mean(TARGET_SOURCE_COL).alias("agg-before"))
        )
        msisdn_after_status = (
            df.filter(~f.col(IS_PRE))
            .groupby(msisdn_col_name)
            .agg(f.mean(TARGET_SOURCE_COL).alias("agg-after"))
        )

        # Keep only users that were present during the entire
        # observation period (before & after) because we
        # are focused only on in-life customer only
        joint_target = msisdn_before_status.join(
            msisdn_after_status, on=msisdn_col_name, how="inner"
        )
        target_status = joint_target.withColumn(
            TARGET_COL, f.col("agg-after") - f.col("agg-before")
        ).select(msisdn_col, f.col(TARGET_COL))

        # Lastly, join with the other rows to complete the process.
        return df.join(target_status, on=msisdn_col_name, how="inner")

    elif use_case == "fourg":
        nonzero_fourg = f.coalesce(f.col(TARGET_SOURCE_COL), f.lit(0))
        pre_user_cond = (f.col(IS_PRE)) & (nonzero_fourg > 0)
        df_msisdn_pre_user = df.filter(pre_user_cond).select(msisdn_col_name).distinct()
        df_without_pre_user = df.join(
            df_msisdn_pre_user, on=msisdn_col_name, how="left_anti"
        )
        df_with_target = df_without_pre_user.withColumn(
            TARGET_COL, f.when(nonzero_fourg > 0, f.lit(1)).otherwise(f.lit(0))
        )
        return df_with_target.withColumn(
            TARGET_COL,
            f.max(TARGET_COL).over(Window.partitionBy(f.col(msisdn_col_name))),
        )
    else:
        raise NotImplementedError(f"Unknown use case: {use_case}")


def select_training_day(
    df: pyspark.sql.DataFrame,
    weekstart_column: str,
    campaign_start_monday: str,
    train_offset_weeks: int,
) -> pyspark.sql.DataFrame:
    """
    Selects the rows of the DataFrame that are on the training day,
    train_offset_weeks before the campaign_start_monday

    :param df: Input DataFrame
    :param weekstart_column: Column containing the weekstarts
    :param campaign_start_monday: Monday the week of the start of the campaign
    :param train_offset_weeks: Weeks from the training day to the campaign_start_monday
    :return: DataFrame limited to the training day
    """
    training_day = pd.to_datetime(
        campaign_start_monday, format="%Y-%m-%d"
    ) - pd.Timedelta(weeks=train_offset_weeks)
    training_day = training_day.strftime(format="%Y-%m-%d")
    pai.log_params({"date_training_day": training_day})
    return df.filter(f.col(weekstart_column) == f.lit(training_day))


def _is_any_prefix(v: str, prefixes: List[str]) -> bool:
    """
    Check if the given prefix is in the list of prefixes:

    :param v: The given prefix to be checked
    :param prefixes: The list of prefixes candidates
    :return: A boolean value indicating if `v` is in `prefixes`
    """
    for p in prefixes:
        if v.startswith(p):
            return True
    return False


def _is_any_suffix(v: str, suffixes: List[str]) -> bool:
    """
    Check if the given suffix is in the list of suffixes:

    :param v: The given suffix to be checked
    :param suffixes: The list of suffixes candidates
    :return: A boolean value indicating if `v` is in `suffixes`
    """
    for s in suffixes:
        if v.endswith(s):
            return True
    return False


def _col_selection_loop(
    columns: List[StructField],
    feature_col_prefix: str,
    col_conf: dict,
    *required_columns: str,
) -> List[str]:
    """
    Selects columns according to the below if-else rules:
    1. Check if column is in required list, keep if yes;
    2. Check if column is in excluded list, drop if yes;
    3. Check if column's data type is in excluded dtype list, drop if yes;
    4. Check if column's prefix is in the excluded prefix list, drop if yes;
    5. Check if column's suffix is in the excluded suffix list, drop if yes;
    6. Check if this is a feature column, keep if yes;
    7. If all above rules are not satisfied, then drop this column

    :param columns: Columns of the DataFrame
    :param feature_col_prefix: Prefix for features
    :param col_conf: Schema configuration for required columns
    :param required_columns: Extra columns required, not specified in `col_conf`
    :return: List of columns to keep for the DataFrame
    """
    kept_cols = {TARGET_COL, *required_columns}

    for col in columns:
        if col.name in col_conf["required"]:
            kept_cols.add(col.name)
        # Handle a bunch of exclusion cases
        #   - Force excluded
        #   - excluded by dtype prefix
        #   - excluded by prefix or suffix
        elif col.name in col_conf["excluded"]:
            pass
        elif _is_any_prefix(col.dataType.typeName(), col_conf["exclude_dtype_prefix"]):
            pass
        elif _is_any_prefix(col.name, col_conf["exclude_prefix"]):
            pass
        elif _is_any_suffix(col.name, col_conf["exclude_suffix"]):
            pass
        # If all exclusions pass and the name starts with the prefix for
        # features, we include it.
        elif col.name.startswith(feature_col_prefix):
            kept_cols.add(col.name)
        # Any other case is a do-nothing - ie, exclusion.
        else:
            pass
    return sorted(kept_cols)


def column_selection(
    df: pyspark.sql.DataFrame,
    feat_col_prefix: str,
    col_conf: dict,
    *required_columns: str,
) -> pyspark.sql.DataFrame:
    """ Select columns according to `col_conf`.

    :param df: Input DataFrame
    :param feat_col_prefix: Prefix for features.
    :param col_conf: Schema configuration for required columns
    :param required_columns: Extra columns required, not specified in `col_conf`
    :return: DataFrame with only selected columns
    """
    COL_CONF_SCHEMA.validate(col_conf)
    cols = list(df.schema)
    kept_cols = _col_selection_loop(cols, feat_col_prefix, col_conf, *required_columns)

    return df.select(*kept_cols)


def _get_feature_importance_from_pai(pai_run_id: str) -> pd.DataFrame:
    """
    Loads feature and importance from given PAI run_id

    :param pai_run_id: PAI run_id to get feature importance information
    :return: DataFrame with feature and importance column of the features logged in PAI
    """
    IMPORTANCE_PREFIX = "importance_"
    PREFIX_LEN = len(IMPORTANCE_PREFIX)

    pai_features = pai.load_features(run_id=pai_run_id)
    # select importance cols and remove "importance_" prefix
    importance_cols = [
        col for col in pai_features.columns if col.startswith(IMPORTANCE_PREFIX)
    ]
    importance_df = pai_features[importance_cols].rename(
        columns=lambda col: col[PREFIX_LEN:]
    )
    # select first row as series, rename series and index
    importance_df = (
        importance_df.iloc[0]
        .rename(IMPORTANCE_COL)
        .rename_axis(index=FEATURE_COL)
        .reset_index()
    )
    return importance_df


def feature_selection(
    df: pyspark.sql.DataFrame, selected_features_run_id: str, *required_columns: str,
) -> pyspark.sql.DataFrame:
    """
    Select only selected features and other required columns from master table
    Selected features will be obtained from the PAI run_id specified

    :param df: Spark dataframe of processed master table
    :param selected_features_run_id: PAI run_id to load selected features
    :param required_columns: Other required columns

    :return: Spark dataframe with selected features and required columns
    """

    if selected_features_run_id:
        # get selected features
        selected_features_df = _get_feature_importance_from_pai(
            selected_features_run_id
        )
        selected_features = selected_features_df[FEATURE_COL].tolist()

        # union with required_columns and make sure selected_cols are in df.columns using intersection
        required_columns += (TARGET_COL,)
        selected_cols = {*selected_features} | {*required_columns}
        selected_cols = selected_cols & {*df.columns}
        selected_cols = sorted(selected_cols)

        return df.select(*selected_cols)
    # if run_id not specified, return df unmodified
    return df


def sample_rows_and_features(
    df: Union[pyspark.sql.DataFrame, pd.DataFrame],
    col_config: dict,
    n_rows_to_sample: int,
    n_features_to_sample: int,
    feature_column_prefix: str,
) -> pyspark.sql.DataFrame:
    """
    Samples out rows & features. Limiting the number of rows and columns
    is useful for training under limited computational resources.

    In cases where the frame has less rows or columns than desired, this
    function will not perform any filtering on that axis.

    "Required" features are separate from the n_features_to_sample limit.

    :param df: The DataFrame to sample
    :param col_config: The column config. Any columns in "required" will
        be force-included.
    :param n_rows_to_sample: How many rows to sample
    :param n_features_to_sample: How many features to sample in addition to
        "required" features
    :param feature_column_prefix: Only features with this prefix will be
        sampled - this way you can easily keep all your targets and flag
        values etc.

    :return: The sampled DataFrame
    """
    COL_CONF_SCHEMA.validate(col_config)
    if isinstance(df, pd.DataFrame):
        rows = df.shape[0]
    else:
        rows = df.count()
    # Empty n_rows_to_sample disables sampling by row
    if n_rows_to_sample and (rows > n_rows_to_sample):
        if isinstance(df, pd.DataFrame):
            df = df.sample(n_rows_to_sample)
        else:
            df = df.sample(withReplacement=False, fraction=(n_rows_to_sample / rows))

    # Only columns that are:
    #  - featurish
    #  - not explicitly required
    # are candidates for dropping.
    droppable_feat_cols = []
    for col in df.columns:
        if not col.startswith(feature_column_prefix):
            continue
        if col in col_config["required"]:
            continue
        droppable_feat_cols.append(col)

    features_to_drop = []
    # Empty n_features_to_sample disables sampling by column
    if n_features_to_sample and (len(droppable_feat_cols) > n_features_to_sample):
        n_to_drop = len(droppable_feat_cols) - n_features_to_sample
        sample = random.sample(droppable_feat_cols, k=n_to_drop)
        features_to_drop.extend(sample)

    if isinstance(df, pd.DataFrame):
        return df.drop(features_to_drop, 1)
    else:
        return df.drop(*features_to_drop)


def convert_to_pandas(df: pyspark.sql.DataFrame) -> pd.DataFrame:
    """
    Converts a spark dataframe to a pandas dataframe

    :param df: The Spark DataFrame to convert
    :return: The converted pandas DataFrame
    """
    return df.toPandas()


def dataset_checks(
    df: pd.DataFrame, msisdn_col: str, *required_non_null: str
) -> pd.DataFrame:
    """
    Performs dataset checks:
     - All treatment statuses known
     - All conversion statuses known
     - MSISDNs appear only once

    :param df: The input DataFrame
    :param msisdn_col: The column that contains msisdns
    :param required_non_null: The columns that should not have null values
    :return: The input DataFrame
    """
    assert isinstance(df, pd.DataFrame)

    verifier = ge_dataset.PandasDataset(df)
    _assert_prop(verifier.expect_table_row_count_to_be_between(min_value=1))
    _assert_prop(verifier.expect_column_to_exist(msisdn_col))
    _assert_prop(verifier.expect_column_values_to_be_unique(msisdn_col))
    for col in required_non_null + (TARGET_COL,):
        _assert_prop(verifier.expect_column_to_exist(col))
        _assert_prop(verifier.expect_column_values_to_not_be_null(col))
    return df


def _assert_prop(prop: ExpectationValidationResult) -> None:
    """
    Utility function to assert with error message

    :param prop: Validation result
    """
    msg = f"Failed data check {prop}"
    assert prop.success, msg
