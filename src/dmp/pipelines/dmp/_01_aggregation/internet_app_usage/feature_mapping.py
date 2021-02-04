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

import pyspark
import pyspark.sql.functions as f
import pyspark.sql.types as t


def feature_mapping(
    df_content_mapping: pyspark.sql.DataFrame,
    df_segment_mapping: pyspark.sql.DataFrame,
    df_category_mapping: pyspark.sql.DataFrame,
    df_fintech_mapping: pyspark.sql.DataFrame,
    df_app_mapping: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Creates mapping between BCP Usage to features.

    Args:
        df_content_mapping: Mapping between Application Name & Component Name from BCP Usage data to iab_tier,
            iab_content_channel, iab_content_media_format & iab_content_type.
        df_segment_mapping: Mapping between Application Name, iab_tier, iab_content_channel, iab_content_media_format
            & iab_content_type from df_content_mapping to segment.
        df_category_mapping:
            Mapping between segment from df_segment_mapping to feature category.
        df_fintech_mapping:
            Mapping between Fin-tech Applications to feature category
        df_app_mapping:
            Mapping between Specific Applications to feature category

    Returns:
        Final Mapping between Application Name & Component Name from BCP Usage data to feature category.
    """

    # Joining Content Mapping & Segment Mapping
    df_content_segment_mapping = df_content_mapping.join(
        f.broadcast(df_segment_mapping),
        [
            (df_content_mapping["iab_tier1"] == df_segment_mapping["iab_tier1"])
            & (
                (df_content_mapping["iab_tier2"] == df_segment_mapping["iab_tier2"])
                | df_segment_mapping["iab_tier2"].isNull()
            )
            & (
                (
                    df_content_mapping["iab_content_channel"]
                    == df_segment_mapping["iab_content_channel"]
                )
                | df_segment_mapping["iab_content_channel"].isNull()
            )
            & (
                (
                    df_content_mapping["iab_content_type"]
                    == df_segment_mapping["iab_content_type"]
                )
                | df_segment_mapping["iab_content_type"].isNull()
            )
            & (
                (
                    df_content_mapping["iab_content_media_format"]
                    == df_segment_mapping["iab_content_media_format"]
                )
                | df_segment_mapping["iab_content_media_format"].isNull()
            )
            & (
                (
                    df_content_mapping["accessed_app"]
                    == df_segment_mapping["accessed_app"]
                )
                | df_segment_mapping["accessed_app"].isNull()
            )
        ],
        how="inner",
    ).select(
        df_segment_mapping["segment"],
        df_content_mapping["accessed_app"],
        df_content_mapping["component"],
    )

    # Joining Category Mapping
    df_bcp_feature_mapping = df_content_segment_mapping.join(
        f.broadcast(df_category_mapping), ["segment"], how="inner"
    ).select(
        df_content_segment_mapping["accessed_app"],
        df_content_segment_mapping["component"],
        df_category_mapping["category"],
    )

    # Adding Fintech Mapping
    df_fintech_mapping = df_fintech_mapping.withColumn(
        "component", f.lit(None).cast(t.StringType())
    ).select("accessed_app", "component", "category")
    df_bcp_feature_mapping = df_bcp_feature_mapping.union(df_fintech_mapping)

    # Adding Fintech Mapping
    df_app_mapping = df_app_mapping.withColumn(
        "component", f.lit(None).cast(t.StringType())
    ).select("accessed_app", "component", "category")
    df_bcp_feature_mapping = df_bcp_feature_mapping.union(df_app_mapping)

    df_bcp_feature_mapping = df_bcp_feature_mapping.withColumn(
        "category", f.trim(f.lower(f.col("category")))
    ).distinct()

    return df_bcp_feature_mapping
