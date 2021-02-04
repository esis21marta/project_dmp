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

python upload.py -f "data/BI2.0 Data Dictionary.xlsx" -s "UPCC-Daily" -o 2 -c "New Attribute Name, Data Type, Business Rule (If Any)" --data_set "abt.usage_upcc_abt_dd" --data_source "ABT"
######################################################


python upload.py -f "data/BI2.0 Data Dictionary.xlsx" -s "UPCC-Monthly" -c "New Attribute Name, Data Type, Business Rule (If Any)" --data_set "abt.usage_upcc_abt_mm" --data_source "ABT"
######################################################


python upload.py -f "data/BI2.0 Data Dictionary.xlsx" -s "UPCC Daily Detail" -c "New Attribute Name, Data Type, Business Rule (If Any)" --data_set "abt.usage_upcc_det_abt_dd" --data_source "ABT"
######################################################


python upload.py -f "data/BI2.0 Data Dictionary.xlsx" -s "CDR Daily" -o 1 -c "New Attribute Name, Data Type, Business Rule (If Any)" --data_set "abt.usage_chg_payu_abt_dd" --data_source "ABT"
######################################################


python upload.py -f "data/BI2.0 Data Dictionary.xlsx" -s "CDR Package Daily" -o 1 -c "New Attribute Name, Data Type, Business Rule (If Any)" --data_set "abt.usage_chg_pkg_abt_dd" --data_source "ABT"
######################################################


python upload.py -f "data/BI2.0 Data Dictionary.xlsx" -s "CDR Package Purchase" -o 1 -c "New Attribute Name, Data Type, Business Rule (If Any)" --data_set "abt.usage_chg_pkg_prchse_abt_dd" --data_source "ABT"
######################################################


python upload.py -f "data/BI2.0 Data Dictionary.xlsx" -s "CDR Package Purchase Detail" -o 1 c "New Attribute Name, Data Type, Business Rule (If Any)" --data_set "abt.usage_chg_pkg_prchse_det_abt_dd" --data_source "ABT"
######################################################


python upload.py -f "data/BI2.0 Data Dictionary.xlsx" -s "Recharge Top Denomination" -o 2 -c "New Attribute Name, Data Type, Business Rule (If Any)" --data_set "abt.rech_top_denomination_abt_mm" --data_source "ABT"
######################################################


python upload.py -f "data/BI2.0 Data Dictionary.xlsx" -s "Recharge" -o 2 -c "New Attribute Name, Data Type, Business Rule (If Any)" --data_set "abt.rech_abt_mm" --data_source "ABT"
######################################################


python upload.py -f "data/BI2.0 Data Dictionary.xlsx" -s "Recharge per Channel" -o 3 -c "New Attribute Name, Data Type, Business Rule (If Any)" --data_set "abt.rech_channel_abt_mm" --data_source "ABT"
######################################################


python upload.py -f "data/BI2.0 Data Dictionary.xlsx" -s "Recharge Last Topup" -o 3 -c "New Attribute Name, Data Type, Business Rule (If Any)" --data_set "abt.rech_last_topup_abt_mm" --data_source "ABT"
######################################################


python upload.py -f "data/BI2.0 Data Dictionary.xlsx" -s "Top Up Daily (Recharge)" -o 2 -c "New Attribute Name, Data Type, Business Rule (If Any)" --data_set "abt.rech_daily_abt_dd" --data_source "ABT"
######################################################


python upload.py -f "data/BI2.0 Data Dictionary.xlsx" -s "MSS-Daily" -o 1 -c "New Attribute Name, Data Type, Business Rule (If Any)" --data_set "abt.usage_mss_abt_dd" --data_source "ABT"
######################################################


python upload.py -f "data/BI2.0 Data Dictionary.xlsx" -s "MSS-Monthly" -o 1 -c "New Attribute Name, Data Type, Business Rule (If Any)" --data_set "abt.usage_mss_abt_mm" --data_source "ABT"
######################################################


python upload.py -f "data/BI2.0 Data Dictionary.xlsx" -s "Campaign" -o 1 -c "New Attribute Name, Data Type, Business Rule (If Any)" --data_set "abt.campaign_abt_dd" --data_source "ABT"
######################################################
