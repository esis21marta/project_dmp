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

from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_home_customer_profiling import (
    mobility_home_customer_profiling_features,
)
from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_home_poi import (
    mobility_home_poi_features,
)
from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_home_stay_freq import (
    mobility_home_stay_freq_features,
)
from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_home_unique_count import (
    mobility_home_stay_unique_count_features,
)
from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_home_work_same import (
    mobility_home_work_same,
)
from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_poi_freq import (
    mobility_poi_freq_features,
)
from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_staypoint_customer_profiling import (
    mobility_staypoint_customer_profiling_features,
)
from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_staypoint_freq import (
    mobility_staypoint_freq_features,
)
from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_staypoint_poi import (
    mobility_staypoint_poi_features,
)
from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_staypoint_unique_count import (
    mobility_staypoint_unique_count_features,
)
from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_work_customer_profiling import (
    mobility_work_customer_profiling_features,
)
from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_work_poi import (
    mobility_work_poi_features,
)
from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_work_stay_freq import (
    mobility_work_stay_freq_features,
)
from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_work_unique_count import (
    mobility_work_stay_unique_count_features,
)
from src.dmp.pipelines.dmp._04_features.mobility.nodes.master_feature import (
    mobility_master_features,
)


def create_pipeline():
    pipeline = Pipeline(
        [
            node(
                mobility_home_poi_features,
                inputs=["l4_home_stay_filtered", "l2_external_poi"],
                outputs="l5_home_stay",
                name="mobility_home_poi_features",
            ),
            node(
                mobility_work_poi_features,
                inputs=["l4_work_stay_filtered", "l2_external_poi"],
                outputs="l5_work_stay",
                name="mobility_work_poi_features",
            ),
            node(
                mobility_staypoint_poi_features,
                inputs=["l4_staypoint_filtered", "l2_external_poi"],
                outputs="l5_staypoint",
                name="mobility_staypoint_poi_features",
            ),
            node(
                mobility_home_customer_profiling_features,
                inputs=[
                    "l4_home_stay_filtered",
                    "l2_external_dryad_gdp",
                    "l2_external_dryad_gdp_per_capita",
                    "l2_external_dryad_hdi",
                    "l2_external_fb_pop",
                    "l2_external_ghs_urbancity",
                    "l2_external_gar_demographics",
                    "l1_external_bps",
                ],
                outputs="l5_home_stay_customer_profile",
                name="mobility_home_customer_profiling_features",
            ),
            node(
                mobility_work_customer_profiling_features,
                inputs=[
                    "l4_work_stay_filtered",
                    "l2_external_dryad_gdp",
                    "l2_external_dryad_gdp_per_capita",
                    "l2_external_dryad_hdi",
                    "l2_external_fb_pop",
                    "l2_external_ghs_urbancity",
                    "l2_external_gar_demographics",
                    "l1_external_bps",
                ],
                outputs="l5_work_stay_customer_profile",
                name="mobility_work_customer_profiling_features",
            ),
            node(
                mobility_staypoint_customer_profiling_features,
                inputs=[
                    "l4_staypoint_filtered",
                    "l2_external_dryad_gdp",
                    "l2_external_dryad_gdp_per_capita",
                    "l2_external_dryad_hdi",
                    "l2_external_fb_pop",
                    "l2_external_ghs_urbancity",
                    "l2_external_gar_demographics",
                    "l1_external_bps",
                ],
                outputs="l5_staypoint_customer_profile",
                name="mobility_staypoint_customer_profiling_features",
            ),
            node(
                mobility_home_work_same,
                inputs=["l4_home_stay_filtered", "l4_work_stay_filtered"],
                outputs="l5_work_home_same",
                name="mobility_home_work_same",
            ),
            node(
                mobility_home_stay_unique_count_features,
                inputs=["l4_home_stay_filtered"],
                outputs="l5_home_stay_unique_count",
                name="mobility_home_stay_unique_count_features",
            ),
            node(
                mobility_work_stay_unique_count_features,
                inputs=["l4_work_stay_filtered"],
                outputs="l5_work_stay_unique_count",
                name="mobility_work_stay_unique_count_features",
            ),
            node(
                mobility_staypoint_unique_count_features,
                inputs=["l4_staypoint_filtered"],
                outputs="l5_staypoint_unique_count",
                name="mobility_staypoint_unique_count_features",
            ),
            node(
                mobility_home_stay_freq_features,
                inputs=["l4_home_stay_filtered"],
                outputs="l5_home_stay_freq",
                name="mobility_home_stay_freq_features",
            ),
            node(
                mobility_work_stay_freq_features,
                inputs=["l4_work_stay_filtered"],
                outputs="l5_work_stay_freq",
                name="mobility_work_stay_freq_features",
            ),
            node(
                mobility_staypoint_freq_features,
                inputs=["l4_staypoint_filtered"],
                outputs="l5_staypoint_freq",
                name="mobility_staypoint_freq_features",
            ),
            node(
                mobility_poi_freq_features,
                inputs=["l4_staypoint_filtered", "l2_external_poi"],
                outputs="l5_poi_freq",
                name="mobility_poi_freq_features",
            ),
            node(
                mobility_master_features,
                inputs=[
                    "l5_home_stay_customer_profile",
                    "l5_home_stay",
                    "l5_home_stay_freq",
                    "l5_home_stay_unique_count",
                    "l5_work_home_same",
                    "l5_poi_freq",
                    "l5_staypoint_customer_profile",
                    "l5_staypoint_freq",
                    "l5_staypoint",
                    "l5_staypoint_unique_count",
                    "l5_work_stay_customer_profile",
                    "l5_work_stay",
                    "l5_work_stay_freq",
                    "l5_work_stay_unique_count",
                    "params:sla_date",
                ],
                outputs="l5_fea_mobility",
                name="mobility_master_features",
            ),
        ],
        tags=[
            "fea_mobility",
            "de_mobility_pipeline",
            "de_feature_pipeline",
            "de_pipeline",
        ],
    )

    return pipeline
