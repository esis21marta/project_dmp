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

from pyspark.sql import Window
from pyspark.sql import functions as f


def cmp_and_wording(t_stg_cms_definition_contact):
    """
    Gets a mapping between campaign id and (one of!) the possible wordings

    Also returns the channel used in each campaign id
    :param t_stg_cms_definition_contact:
    :return:
    """

    wrd = t_stg_cms_definition_contact
    wrd = wrd.withColumn(
        "type_order",
        f.when(wrd.type == "ELIGIBLE", 1).otherwise(
            f.when(wrd.type == "ELIGIBLE-1", 2).otherwise(
                f.when(wrd.type == "REMINDER-1", 3).otherwise(
                    f.when(wrd.type == "REMINDER-2", 4).otherwise(
                        f.when(wrd.type == "ACCEPTED-1", 5).otherwise(6)
                    )
                )
            )
        ),
    )

    wrd = (
        wrd.filter(wrd.wording != "")
        .filter(
            f.col("type").isin(
                {"ELIGIBLE", "ELIGIBLE-1", "REMINDER-1", "REMINDER-2", "ACCEPTED-1"}
            )
        )
        .withColumn(
            "rn",
            f.row_number().over(
                Window.partitionBy("campid").orderBy(
                    f.col("type_order").asc(), f.col("file_date").desc()
                )
            ),
        )
    )

    # Selecting only the first phrasing of the campaign: the same campaign id has many phrasings
    wrd = wrd.filter(wrd.rn == 1).select("campid", "wording", "channel")

    wrd = wrd.repartition(1)
    return wrd


# TODO I do not know what is in this table. Should check and decide a better function name
def deduplicate_cmp_names(t_stg_cms_definition_identification):

    df = t_stg_cms_definition_identification
    df = df.withColumn(
        "rn",
        f.row_number().over(
            Window.partitionBy("campid").orderBy(f.col("file_date").desc())
        ),
    )
    unique_cmp_names = df.filter(df.rn == 1)
    unique_cmp_names = unique_cmp_names.repartition(1000)
    return unique_cmp_names


def aggregate_cms_ts(t_stg_cms_trackingstream):

    cms_trackingstream = t_stg_cms_trackingstream.filter(
        t_stg_cms_trackingstream.prop_owner != ""
    )
    cms_trackingstream = cms_trackingstream.select(
        ["campaignid", "prop_owner", "prop_productgroup", "prop_objective", "prop_type"]
    )
    cms_ts_aggr = cms_trackingstream.dropDuplicates()
    cms_ts_aggr = cms_ts_aggr.repartition(
        1
    )  # this is fairly small table after aggregation
    return cms_ts_aggr


def joining_wordings_stages_and_trackingstream(cms_ts_aggr, wrd, unique_cmp_names):
    """
    Joins the tables output by the previous functions and
    :param cms_ts_aggr:
    :param wrd:
    :param di:
    :return:
    """
    agr = cms_ts_aggr

    agr = agr.join(
        unique_cmp_names, agr.campaignid == unique_cmp_names.campid, how="left"
    )
    agr = agr.join(wrd, agr.campaignid == wrd.campid, how="left")

    agr = agr.select(
        [
            "campaignid",
            "stage",
            "name",
            "wording",
            "prop_owner",
            "prop_productgroup",
            "prop_objective",
            "prop_type",
        ]
    )

    # # Changing the wording of some campaigns
    # mistaken_name = {"C190928ABOCB515-A", "C190928ABOCB515-B", "C190928ABOCB515-C", "C190928ABOCB515-D",
    #                  "C190928ABOCB516-A", "C190928ABOCB516-B", "C190928ABOCB517"}
    # correct_campaign_name = 'nasza kochana jedyna kampania'
    # agr = (agr
    #        .select(f.when(f.col('campaignid').isin(mistaken_name), correct_campaign_name)
    #                .otherwise(f.col('wording')).alias('wording'),
    #                'campid', 'channel')
    #        )

    agr = agr.repartition(1)
    return agr
