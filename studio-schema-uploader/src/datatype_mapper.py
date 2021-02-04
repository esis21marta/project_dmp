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


def map_datatype(_type):
    if "int" in _type.lower() or "smallint" in _type.lower():
        return "INT"
    elif "char" in _type.lower() or "varchar" in _type.lower():
        return "VARCHAR"
    elif "timestamp" in _type.lower():
        return "TIMESTAMP"
    elif "date" in _type.lower():
        return "DATE"
    elif "time" in _type.lower():
        return "TIME"
    elif (
        ("float" in _type.lower())
        or ("double" in _type.lower())
        or ("decimal" in _type.lower())
        or ("real" in _type.lower())
        or ("decimal" in _type.lower())
        or ("numeric" in _type.lower())
    ):
        return "DOUBLE"
    elif "date" in _type.lower():
        return "DATE"
    elif (
        ("bool" in _type.lower())
        or ("boolean" in _type.lower())
        or ("binary" in _type.lower())
    ):
        return "BOOLEAN"
