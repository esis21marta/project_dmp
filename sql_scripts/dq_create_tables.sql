CREATE TABLE dev_qa_metrics (
    weekstart DATE,
    columns VARCHAR2(1024),
    max FLOAT(22),
    same_percent FLOAT(22),
    negative_value_percentage FLOAT(22),
    null_percentage FLOAT(22),
    mean FLOAT(22),
    min FLOAT(22),
    row_count NUMBER(38),
    percentile_0_1 FLOAT(22),
    percentile_0_2 FLOAT(22),
    percentile_0_25 FLOAT(22),
    percentile_0_3 FLOAT(22),
    percentile_0_4 FLOAT(22),
    percentile_0_5 FLOAT(22),
    percentile_0_6 FLOAT(22),
    percentile_0_7 FLOAT(22),
    percentile_0_75 FLOAT(22),
    percentile_0_8 FLOAT(22),
    percentile_0_9 FLOAT(22),
    layer VARCHAR2(128),
    run_time DATE,
    file_path VARCHAR2(2048),
    old_file_path  VARCHAR2(2048),
    table_name VARCHAR2(256),
    master_mode VARCHAR2(64)
)

CREATE TABLE dev_qa_availability (
    weekstart_min DATE,
    weekstart_max DATE,
    weekstart_distinct NUMBER(38),
    expected_weeks NUMBER(38),
    missing_weeks NUMBER(38),
    available_weeks NUMBER(38),
    layer VARCHAR2(128),
    run_time DATE,
    file_path VARCHAR2(2048),
    old_file_path  VARCHAR2(2048),
    table_name VARCHAR2(256),
    master_mode VARCHAR2(64)
)

CREATE TABLE dev_qa_monthly_unique_msisdn (
    month DATE,
    unique_msisdn_cnt NUMBER(38),
    layer VARCHAR2(128),
    run_time DATE,
    file_path VARCHAR2(2048),
    old_file_path  VARCHAR2(2048),
    table_name VARCHAR2(256),
    master_mode VARCHAR2(64)
)

CREATE TABLE dev_qa_outliers (
    weekstart DATE,
    columns VARCHAR2(1024),
    count_lower_outlier NUMBER(38),
    count_higher_outlier NUMBER(38),
    layer VARCHAR2(128),
    run_time DATE,
    file_path VARCHAR2(2048),
    old_file_path  VARCHAR2(2048),
    table_name VARCHAR2(256),
    master_mode VARCHAR2(64)
)

CREATE TABLE dev_threshold_output (
    current_weekstart DATE,
    columns VARCHAR2(1024),
    layer VARCHAR2(128),
    domain VARCHAR2(128),
    run_time DATE,
    table_name VARCHAR(256),
    max FLOAT(22),
    same_percent FLOAT(22),
    negative_value_percentage FLOAT(22),
    null_percentage FLOAT(22),
    mean FLOAT(22),
    min FLOAT(22),
    row_count NUMBER(38),
    percentile_0_1 FLOAT(22),
    percentile_0_2 FLOAT(22),
    percentile_0_25 FLOAT(22),
    percentile_0_3 FLOAT(22),
    percentile_0_4 FLOAT(22),
    percentile_0_5 FLOAT(22),
    percentile_0_6 FLOAT(22),
    percentile_0_7 FLOAT(22),
    percentile_0_75 FLOAT(22),
    percentile_0_8 FLOAT(22),
    percentile_0_9 FLOAT(22),
    missing_weeks FLOAT(22),
    file_path VARCHAR2(2048),
    old_file_path  VARCHAR2(2048),
    master_mode VARCHAR2(64),
    weekstart_max FLOAT(22),
    weekstart_min FLOAT(22)
)

CREATE TABLE dev_threshold_output_agg (
    current_weekstart DATE,
    no_of_columns NUMBER(38),
    no_of_metrics NUMBER(38),
    total_count NUMBER(38),
    green_count NUMBER(38),
    red_count NUMBER(38),
    percent_green FLOAT(22),
    percent_red FLOAT(22),
    domain VARCHAR2(128),
    dimension VARCHAR2(128),
    layer VARCHAR2(128),
    run_time DATE
)

CREATE TABLE dev_metric_dimension_mapping (
    metric VARCHAR2(128),
    dimension VARCHAR2(128),
    run_time DATE
)

CREATE TABLE dev_qa_timeliness (
    event_date DATE,
    is_available NUMBER(38),
    run_time DATE,
    table_name VARCHAR2(256)
)
