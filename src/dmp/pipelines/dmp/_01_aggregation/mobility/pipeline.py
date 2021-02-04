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

from src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.covid_agg import (
    mobility_covid_agg,
)
from src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.customer_profile_dryad_gdp_agg import (
    mobility_customer_profile_gdp_agg,
)
from src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.customer_profile_dryad_gdp_per_capita_agg import (
    mobility_customer_profile_gdp_per_capita_agg,
)
from src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.customer_profile_dryad_hdi_agg import (
    mobility_customer_profile_dryad_hdi,
)
from src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.customer_profile_fb_pop_agg import (
    mobility_customer_profile_fb_pop_agg,
)
from src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.customer_profile_gar_demographics_agg import (
    mobility_customer_profile_gar_demographics,
)
from src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.customer_profile_urbancity_agg import (
    mobility_customer_profile_urbancity_agg,
)
from src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.home_agg import (
    create_home_agg_monthly,
)
from src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.poi_agg import (
    mobility_poi_agg,
)
from src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.stay_point_agg import (
    create_staypoint_agg_monthly,
)
from src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.work_agg import (
    create_work_agg_monthly,
)


def create_pipeline():
    pipeline = Pipeline(
        [
            node(
                create_home_agg_monthly,
                inputs=["l1_home_stay", "params:month_dt_dict"],
                outputs=None,  # l1_home_stay
                name="mobility_home_stay_agg",
            ),
            node(
                create_work_agg_monthly,
                inputs=["l1_work_stay", "params:month_dt_dict"],
                outputs=None,  # "l2_work_stay"
                name="mobility_work_stay_agg",
            ),
            node(
                mobility_poi_agg,
                inputs=[
                    "l1_external_big_poi",
                    "l1_external_osm_pois",
                    "params:category1_dict",
                    "params:category2_dict",
                ],
                outputs="l2_external_poi",
                name="mobility_poi_agg",
            ),
            node(
                create_staypoint_agg_monthly,
                inputs=["l1_staypoint", "params:month_dt_dict"],
                outputs=None,  # "l2_staypoint"
                name="mobility_stay_point_agg",
            ),
            node(
                mobility_customer_profile_urbancity_agg,
                inputs=["l1_external_ghs_urbancity",],
                outputs="l2_external_ghs_urbancity",
                name="mobility_customer_profile_urbancity_agg",
            ),
            node(
                mobility_customer_profile_gdp_agg,
                inputs=["l1_external_dryad_gdp",],
                outputs="l2_external_dryad_gdp",
                name="mobility_customer_profile_gdp_agg",
            ),
            node(
                mobility_customer_profile_fb_pop_agg,
                inputs=["l1_external_fb_pop",],
                outputs="l2_external_fb_pop",
                name="mobility_customer_profile_fb_pop_agg",
            ),
            node(
                mobility_customer_profile_gdp_per_capita_agg,
                inputs=["l1_external_dryad_gdp_per_capita",],
                outputs="l2_external_dryad_gdp_per_capita",
                name="mobility_customer_profile_gdp_per_capita_agg",
            ),
            node(
                mobility_customer_profile_dryad_hdi,
                inputs=["l1_external_dryad_hdi",],
                outputs="l2_external_dryad_hdi",
                name="mobility_customer_profile_dryad_hdi",
            ),
            node(
                mobility_customer_profile_gar_demographics,
                inputs=["l1_external_gar_demographics",],
                outputs="l2_external_gar_demographics",
                name="mobility_customer_profile_gar_demographics",
            ),
            node(
                mobility_covid_agg,
                inputs=["l1_external_covid", "l1_external_village_cordinate"],
                outputs="l2_external_covid",
                name="mobility_covid_agg",
            ),
        ],
        tags=[
            "agg_mobility",
            # "de_mobility_pipeline",
            "de_aggregation_pipeline",
            # "de_pipeline",
        ],
    )

    return pipeline
