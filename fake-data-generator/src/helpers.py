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

import decimal
import random
import string
from datetime import datetime, timedelta


def gen_random_string(size):
    chars = string.ascii_lowercase + string.digits
    size = random.randrange(5, size + 1)
    return "".join([random.choice(chars) for _ in range(size)])


def gen_random_int(bigint=False, maxm=-1):
    if bigint:
        return random.randint(65536, 99999)
    elif maxm == -1:
        return random.randrange(65535)
    return random.randrange(1, maxm + 1)


def gen_random_calltype():
    calltypes = ["04_sms_out", "05_sms_in", "01_call_out", "02_call_in", "03_call_fwd"]
    return calltypes[random.randrange(0, 5)]


def gen_random_id():
    # Random ID To Be Between 1-50, So That There Are More Rows For A User
    # This Would Allow Better Tracking Of User Activity
    return random.randrange(1, 51)


def gen_random_binary_int():
    random.randint(0, 2)


def gen_random_day_of_week():
    day_of_week = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]
    return random.choice(day_of_week)


def gen_random_date(start, end, date_only=False):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    _date = start + timedelta(seconds=random_second)
    if date_only:
        return _date.strftime("%Y-%m-%d")
    return _date.strftime("%Y-%m-%d %H:%M:%S")


d1 = datetime.strptime("1/1/2019 1:30 PM", "%m/%d/%Y %I:%M %p")
d2 = datetime.strptime("2/1/2019 4:50 AM", "%m/%d/%Y %I:%M %p")


def gen_random_number_ndigs(size):
    numbers = "123456789"
    random_number = "".join([random.choice(numbers) for _ in range(size)])
    return int(random_number)


def gen_random_decimal(i, d):
    i = i - d
    return decimal.Decimal(
        "%d.%d" % (gen_random_number_ndigs(i), gen_random_number_ndigs(d))
    )


def gen_date_yyyymm():
    year = str(random.choice([2018, 2019, 2016, 2015]))
    month = random.randrange(1, 13)
    if month < 10:
        month = "0" + str(month)
    return year + str(month)


def gen_random_time_hh_mm_ss():
    hour = random.randrange(1, 25)
    minute = random.randrange(0, 60)
    sec = random.randrange(0, 60)
    _hour_str = str(hour) if hour > 9 else "0" + str(hour)
    _minute_str = str(minute) if hour > 9 else "0" + str(minute)
    _sec_str = str(sec) if hour > 9 else "0" + str(sec)
    return "{}:{}:{}".format(_hour_str, _minute_str, _sec_str)
