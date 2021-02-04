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

from typing import Any, Dict

import pyspark

from src.dmp.pipelines.dmp._02_primary.msisdn_list.check_new_msisdn_count import (
    check_new_msisdn_count,
)
from src.dmp.pipelines.dmp._02_primary.msisdn_list.create_scoring_msisdns import (
    create_scoring_msisdns,
)
from src.dmp.pipelines.dmp._02_primary.msisdn_list.packing_partner_msisdns import (
    packing_partner_msisdns,
)


def create_msisdn_list(
    df_prepaid_data: pyspark.sql.DataFrame,
    df_postpaid_data: pyspark.sql.DataFrame,
    partner_msisdns_old: pyspark.sql.DataFrame,
    parameters: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    pipeline = parameters["pipeline"]
    if "scoring" == pipeline:
        df_msisdn = create_scoring_msisdns(
            df_prepaid_data=df_prepaid_data, df_postpaid_data=df_postpaid_data
        )
    else:
        packing_unpacking = parameters["packing_unpacking"]
        files_to_pack = packing_unpacking["files_to_pack"]
        packing_msisdn_count_limit = packing_unpacking["packing_msisdn_count_limit"]
        email_notif_setup = parameters["email_notif_setup"]
        df_msisdn = packing_partner_msisdns(files_to_pack=files_to_pack)

        # Validate MSISDN
        msisdn_to_move = check_new_msisdn_count(
            df_msisdn, partner_msisdns_old, files_to_pack, email_notif_setup
        )
        if msisdn_to_move > packing_msisdn_count_limit:
            raise Exception(
                "MSISDN packing limit crossed. Check telegram notification for more details."
            )
    return df_msisdn
