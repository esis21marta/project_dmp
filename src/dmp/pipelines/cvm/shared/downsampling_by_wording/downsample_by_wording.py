# Copyright 2018-present QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.


import pandas as pd
import pyspark.sql.functions as fn


def prepare_campaigns_table(
    campaign_aggregated_table, t_stg_cms_trackingstream, cols_to_select
):

    campaign_aggregated_table = campaign_aggregated_table.where(
        campaign_aggregated_table["prop_owner"].isin(["HQ", "REG10", "DLS"])
    ).select(cols_to_select["campaigns"]["cmp"])

    t_stg_cms_trackingstream = (
        t_stg_cms_trackingstream.where(
            fn.col("streamtype").isin(["ELIGIBLE", "ELIGIBLE-1"])
        )
        .withColumn("msisdn", fn.col("identifier"))
        .select(["msisdn"] + cols_to_select["campaigns"]["ts"] + ["producedon"])
    )

    trackingstream_wording = t_stg_cms_trackingstream.join(
        campaign_aggregated_table, ["campaignid"], "inner"
    )

    trackingstream_wording = trackingstream_wording.withColumn(
        "week_start",
        fn.date_format(
            fn.next_day(
                fn.date_sub(
                    fn.to_date(
                        trackingstream_wording.producedon.substr(1, 8), "yyyyMMdd"
                    ),
                    14,
                ),
                "Mon",
            ),
            "yyyy-MM-dd",
        ),
    )

    return trackingstream_wording


def create_downsampling_table(trackingstream_wording, downsmpl_params):

    sample_size_per_wording = downsmpl_params["sample_size_per_wording"]
    max_control_share = downsmpl_params["max_control_share"]

    # calculation counts of ctr, cnt and transfering it to Python
    df1 = trackingstream_wording.withColumn(
        "controlgroup", fn.when(fn.col("iscontrolgroup") == "true", 1).otherwise(0)
    )
    df_counts = df1.groupBy(["wording"]).agg(
        fn.sum("controlgroup").alias("ctr"), fn.count("iscontrolgroup").alias("cnt")
    )
    pd_df_counts = df_counts.toPandas()

    # summarizing to get sampling probs
    pd_df_counts["max_smpl_size"] = sample_size_per_wording
    pd_df_counts["max_ctr_size"] = sample_size_per_wording * max_control_share
    pd_df_counts["trt"] = pd_df_counts["cnt"] - pd_df_counts["ctr"]

    # calculating sampling probs
    pd_df_counts["ctr_smpl_size"] = pd_df_counts.apply(
        lambda r: r["max_ctr_size"] if r["max_ctr_size"] <= r["ctr"] else r["ctr"],
        axis=1,
    )
    pd_df_counts["trt_smpl_size"] = pd_df_counts.apply(
        lambda r: r["trt"]
        if r["trt"] <= r["max_smpl_size"] - r["ctr_smpl_size"]
        else r["max_smpl_size"] - r["ctr_smpl_size"],
        axis=1,
    )
    ctr_sizes = pd_df_counts[["wording", "ctr", "ctr_smpl_size"]]
    trt_sizes = pd_df_counts[["wording", "trt", "trt_smpl_size"]]

    # removing not necessary rows
    ctr_sizes = ctr_sizes[ctr_sizes.ctr > 0]
    trt_sizes = trt_sizes[trt_sizes.trt > 0]

    # preparing to create sampling dictionary
    ctr_sizes["iscontrolgroup"] = "true"
    trt_sizes["iscontrolgroup"] = "false"

    ctr_sizes["sampling_prob"] = ctr_sizes.apply(
        lambda r: 0 if r["ctr"] == 0 else r["ctr_smpl_size"] / r["ctr"], axis=1
    )
    trt_sizes["sampling_prob"] = trt_sizes.apply(
        lambda r: 0 if r["trt"] == 0 else r["trt_smpl_size"] / r["trt"], axis=1
    )

    common_cols = ["wording", "iscontrolgroup", "sampling_prob"]
    downsampling_table = pd.concat([ctr_sizes[common_cols], trt_sizes[common_cols]])

    return downsampling_table


def downsample_by_wording(campaign_aggregated_table, downsampling_table):

    # preparing stratas dictionary
    downsampling_table["strata"] = downsampling_table.apply(
        lambda r: r["iscontrolgroup"] + "_" + str(r["wording"]), axis=1
    )
    samp_dict = dict(
        zip(
            downsampling_table["strata"].to_list(),
            downsampling_table["sampling_prob"].to_list(),
        )
    )

    ###Sampling
    # create strata column in full table
    campaign_aggregated_table = campaign_aggregated_table.withColumn(
        "strata", fn.concat(fn.col("iscontrolgroup"), fn.lit("_"), fn.col("wording"))
    )

    # actual sampling
    campaign_aggregated_table_downsmpl = campaign_aggregated_table.stat.sampleBy(
        "strata", samp_dict, 123
    )
    campaign_aggregated_table_downsmpl = campaign_aggregated_table_downsmpl.drop(
        "strata"
    )
    campaign_aggregated_table_downsmpl = campaign_aggregated_table_downsmpl.repartition(
        1000
    )

    return campaign_aggregated_table_downsmpl
