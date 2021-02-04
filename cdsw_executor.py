import datetime
import os
import subprocess

PROJECT_PATH = "/home/cdsw/project_dmp"
LOCAL_LOG_PATH = os.path.join(PROJECT_PATH, "logs")
HDFS_LOG_PATH = "hdfs:///data/landing/gx_pnt/mck_dmp_pipeline/logs"

os.chdir(PROJECT_PATH)

JOB_TITLE = "training-master-table"


def is_first_sunday(datetime):
    return datetime.weekday() == 6 and datetime.day <= 7


run_by_tag_or_name = {
    "de_pipeline_aggregation": [
        ("tag=agg_airtime_loan",),
        ("tag=agg_customer_los",),
        ("tag=agg_customer_profile",),
        ("tag=agg_handset",),
        # ("tag=agg_handset_internal",), # source table is not productionised
        ("tag=agg_internet_app_usage",),
        ("tag=agg_internet_usage",),
        ("tag=agg_mytsel",),
        ("tag=agg_network",),
        ("tag=agg_payu_usage",),
        ("node=non_macro_product_weekly",),
        # ("node=product_create_macroproduct_mapping",),
        # ("node=aggregate_product_to_weekly",),
        ("tag=agg_recharge",),
        ("tag=agg_revenue",),
        ("tag=agg_text_messaging",),
        ("tag=agg_voice_calls",),
        ("tag=agg_mobility",),
    ],
    "de_pipeline_primary": [
        ("pipeline=de_pipeline_primary", "runner=thread", "max-workers=10")
    ],
    "de_pipeline_feature": [
        ("tag=fea_airtime_loan",),
        ("tag=fea_customer_los",),
        ("tag=fea_customer_profile",),
        ("tag=fea_handset",),
        # ("tag=fea_handset_internal",), # source table is not productionised
        ("tag=fea_internet_app_usage",),
        ("tag=fea_internet_usage",),
        ("tag=fea_mytsel",),
        (
            "node=fea_network_msisdn,fea_network_tutela,fea_map_msisdn_to_tutela_network",
        ),
        ("tag=fea_payu_usage",),
        ("tag=fea_product",),
        ("tag=fea_recharge",),
        ("tag=fea_revenue",),
        ("tag=fea_text_messaging",),
        ("tag=fea_voice_calling",),
        ("tag=fea_mobility",),
    ],
    "de_pipeline_master": [
        ("tag=de_msisdn_master_pipeline", "runner=thread", "max-workers=10")
    ],
    "unpacking": [("tag=unpacking_pipeline", "runner=thread")],
    "de_qa_aggregation": [
        ("tag=de_qa_aggregation",),
        ("tag=de_qa_aggregation_threshold",),
    ],
    "de_qa_master": [
        ("node=qa_sample_msisdn",),
        ("tag=de_qa_master",),
        ("tag=de_qa_master_threshold",),
    ],
}

failed = False

kedro_env = os.getenv("KEDRO_ENV")
pipeline = os.getenv("PIPELINE")
tag = os.getenv("TAG")
first_sunday = os.getenv("FIRST_SUNDAY")

number_of_weeks = os.getenv("NUMBER_OF_WEEKS")
first_weekstart = os.getenv("FIRST_WEEKSTART")
last_weekstart = os.getenv("LAST_WEEKSTART")

number_of_months = os.getenv("NUMBER_OF_MONTHS")
first_month = os.getenv("FIRST_MONTH")
last_month = os.getenv("LAST_MONTH")

if number_of_weeks:
    number_of_weeks = int(number_of_weeks)

if number_of_months:
    number_of_months = int(number_of_months)

if first_sunday == "True" and not is_first_sunday(datetime.datetime.today()):
    raise Exception("Not first sunday. Stopping Pipeline")


def get_first_last_weekstart_params(number_of_weeks, first_weekstart, last_weekstart):
    if not first_weekstart or not last_weekstart:
        number_of_weeks -= 1
        now = datetime.datetime.now()
        last_weekstart = now.date() - datetime.timedelta(days=now.weekday())
        first_weekstart = last_weekstart - datetime.timedelta(weeks=number_of_weeks)
    return f"first_weekstart:{first_weekstart}", f"last_weekstart:{last_weekstart}"


def get_first_last_month_params(number_of_months, first_month, last_month):
    if not first_month or not last_month:
        now = datetime.datetime.now()
        if int(now.strftime("%d")) >= 8:
            number_of_months = number_of_months + 1
            last = 2
        else:
            number_of_months = number_of_months + 2
            last = 3
        first_month = datetime.date(
            now.year + ((now.month - number_of_months) // 12),
            (now.month - number_of_months) % 12 + 1,
            1,
        )
        last_month = datetime.date(
            now.year + ((now.month - last) // 12), (now.month - last) % 12 + 1, 1,
        )
    return f"first_month:{first_month}", f"last_month:{last_month}"


def copy_log_to_hdfs(log_file_path):
    subprocess.check_call(
        ["hdfs", "dfs", "-copyFromLocal", "-f", log_file_path, HDFS_LOG_PATH]
    )


def get_time_for_log():
    return datetime.datetime.today().strftime("%Y-%m-%d_T%H_%M_%S")


first_last_weekstart_params = get_first_last_weekstart_params(
    number_of_weeks, first_weekstart, last_weekstart
)
first_last_month_params = get_first_last_month_params(
    number_of_months, first_month, last_month
)
log_dataset_pipeline_params = "log_dataset_pipeline.enable_log:True"
log_node_runtime_prams = "log_node_runtime:True"

params = ",".join(
    [
        *first_last_weekstart_params,
        *first_last_month_params,
        log_dataset_pipeline_params,
        log_node_runtime_prams,
    ]
)

if pipeline != None and tag == None:
    log_path = os.path.join(LOCAL_LOG_PATH, f"{pipeline}__{get_time_for_log()}.log")

    # In case of Aggregation and Feature Layer run other domain even if one domain fails
    if pipeline in run_by_tag_or_name.keys():
        for item in run_by_tag_or_name[pipeline]:
            with open(log_path, "a") as log_file:
                try:
                    subprocess.check_call(
                        [
                            "kedro",
                            "run",
                            f"--env={kedro_env}",
                            f"--params={params}",
                            f"--desc={JOB_TITLE}",
                            *[f"--{args}" for args in item],
                        ],
                        stdout=log_file,
                        stderr=log_file,
                    )
                except subprocess.CalledProcessError:
                    failed = True
            copy_log_to_hdfs(log_path)

    else:
        with open(log_path, "a") as log_file:
            try:
                subprocess.check_call(
                    [
                        "kedro",
                        "run",
                        f"--env={kedro_env}",
                        f"--params={params}",
                        f"--pipeline={pipeline}",
                        f"--desc={JOB_TITLE}",
                    ],
                    stdout=log_file,
                    stderr=log_file,
                )
            except:
                failed = True
        copy_log_to_hdfs(log_path)

elif pipeline == None and tag != None:
    log_path = os.path.join(LOCAL_LOG_PATH, f"{tag}__{get_time_for_log()}.log")
    with open(log_path, "a") as log_file:
        try:
            subprocess.check_call(
                [
                    "kedro",
                    "run",
                    f"--env={kedro_env}",
                    f"--params={params}",
                    f"--tag={tag}",
                    f"--desc={JOB_TITLE}",
                ],
                stdout=log_file,
                stderr=log_file,
            )
        except:
            failed = True
    copy_log_to_hdfs(log_path)
else:
    raise Exception("Need either pipeline or tag to execute job")

if failed:
    raise Exception(f"{pipeline}{tag} failed.")
