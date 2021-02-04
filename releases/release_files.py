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

"""project-thanatos
"""

import configparser

_config = configparser.ConfigParser()
_config.read_file(open(r"releases/release.conf"))

RELEASE_BRANCH = _config.get("release_config", "release_branch")

HDFS_BASE_PATH = {
    "dev": "hdfs:///data/landing/gx_pnt",
    "stage": "hdfs:///data/landing/dmp_staging/release-20201120",
    "prod": "hdfs:///data/landing/dmp_production/release-20201120",
}

FILES_TO_MOVE = {
    # Sample
    # "catalog_name": {
    #     "first_weekstart": "2020-07-06",        # (Optional) first weekstart if we want to copy only selected partitions
    #     "last_weekstart": "2020-10-26",         # (Optional) last weekstart if we want to copy only selected partitions
    # },
    "partner_msisdns": {},
    # Customer LOS
    "l1_customer_los_weekly": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    # Customer Profile
    "l2_customer_profile_weekly": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    "l2_customer_profile_sellthru_weekly": {
        "first_weekstart": "2020-05-18",
        "last_weekstart": "2020-11-09",
    },
    "l2_customer_profile_nsb_weekly": {
        "first_weekstart": "2020-05-18",
        "last_weekstart": "2020-11-09",
    },
    "l2_customer_profile_points_redeemed_weekly": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    "l2_outlet_weekly": {
        "first_weekstart": "2020-05-18",
        "last_weekstart": "2020-11-09",
    },
    "l2_customer_expiry_weekly": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    # Handset
    # "l1_handset_lookup_data": {}, # No need to copy full table will be generated while running
    "l2_handset_weekly_aggregated": {
        "first_weekstart": "2020-05-18",
        "last_weekstart": "2020-11-09",
    },
    # Internet App Usage
    "l1_content_mapping": {},
    "l1_segment_mapping": {},
    "l1_category_mapping": {},
    "l1_fintech_mapping": {},
    "l1_app_mapping": {},
    "l1_internet_app_feature_mapping": {},
    "l2_internet_app_usage_weekly": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    "l2_internet_connection_weekly": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    "l2_internet_app_usage_competitor_monthly": {},  # mo_id
    # Internet Usage
    "l1_internet_usage_weekly": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    # Network
    "l1_network_msisdn_weekly_aggregated": {
        "first_weekstart": "2020-05-18",
        "last_weekstart": "2020-11-09",
    },
    "l2_network_tutela_location_weekly_aggregated": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    # PayU Usage
    "l2_payu_usage_weekly_aggregation": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    # Recharge
    "l1_rech_weekly": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    "l2_account_balance_weekly": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    "l1_chg_pkg_prchse_weekly": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    "l1_digi_rech_weekly": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    "l1_mytsel_rech_weekly": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    # Revenue
    "l1_airtime_loan_id": {},
    "l2_revenue_weekly_data": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    "l2_revenue_alt_weekly_data": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    "l2_revenue_mytsel_weekly": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    # SMS
    "l1_commercial_text_mapping": {},
    "l1_broken_bnumber": {},
    "l2_text_messaging_weekly": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    "l2_commercial_text_messaging_weekly": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    "l2_kredivo_sms_recipients": {},
    # Voice
    "l2_voice_calling_weekly_aggregated": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    # mytsel
    "l2_mytsel_weekly": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
    # airtime loan
    "l2_airtime_loan_weekly": {
        "first_weekstart": "2020-08-17",
        "last_weekstart": "2020-11-09",
    },
}
