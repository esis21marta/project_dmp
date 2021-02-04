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

"""
Functions for determining sample sizes.
"""

from math import ceil
from typing import Tuple

from scipy.stats import norm

from .util import check_args


def sample_t_test(
    delta: float, mean: float, sd0: float, sd1: float, **kwargs
) -> Tuple[int, int]:
    """
    Return sample sizes of control and treatment groups based on 2-sided,
    2-sample, equal variance t-test.
    Args:
        delta: Minimum percentage difference between control and treatment groups outcomes,
         which is expected to be captured
        mean: Expected mean value of the outcome for control group.
         Typical proxy might be population mean before applying treatment
        sd0: Expected standard deviation value of the outcome for control group.
         Typical proxy might be population standard deviation before applying treatment
        sd1: Expected standard deviation value of the outcome for treatment group.
         Typical proxy might be population standard deviation before applying treatment
          with additional factor based on expected effect size.
        **kwargs: {`power`: Expected power of the test. Typically set at 0.8,
                    `sig_level`: Expected type I error rate. Typically set at 0.05,
                    `samples_ratio`: Ratio between sample sizes of treatment and control groups,
                                    Ideally set to 1 for group sizes minimization.
                    If c0 and c1 are specified parameter will not be taken in consideration.
                    Instead optimal ratio will be set.
                    `c0`: Cost of targeting people in control group,
                    `c1`: Cost of targeting people in target group}
    Returns:
        Sample sizes for control and treatment group.
    """

    check_args(kwargs.keys(), {"power", "sig_level", "samples_ratio", "c0", "c1"})
    c0, c1, power, samples_ratio, sig_level = _get_t_test_params(kwargs)

    if (c0 is not None) & (c1 is not None):
        samples_ratio = (c0 / c1) ** (1 / 2) * sd1 / sd0

    n_control = (
        (1 + 1 / samples_ratio)
        / 4
        * ((norm.ppf(1 - sig_level / 2) + norm.ppf(power)) / (delta * mean)) ** 2
        * (((sd0 ** 2) / (sd0 / (sd0 + sd1))) + ((sd1 ** 2) / (sd1 / (sd0 + sd1))))
    )

    return ceil(n_control), ceil(n_control * samples_ratio)


def _get_t_test_params(kwargs):
    """
    The function extracts parameter value if passed else default value is returned.
    Args:
        kwargs: keyword arguments passed to sample_t_test function.
    Returns:
        c0, c1, power, samples_ratio, sig_level
    """
    power, sig_level = kwargs.get("power", 0.8), kwargs.get("sig_level", 0.05)
    samples_ratio = kwargs.get("samples_ratio", 1)
    c0, c1 = kwargs.get("c0", None), kwargs.get("c1", None)
    return c0, c1, power, samples_ratio, sig_level


def sample_binary(delta: float, p: float, **kwargs):
    """
       Return sample sizes of control and treatment groups based normal
       approximation of binary distribution.
       Args:
           delta: Minimum absolute difference in share ot outcome equal to 1
           between control and treatment groups outcomes, which is expected to be captured
           p: Expected share of outcomes equal to 1 for control group.
           Typical proxy might be population mean before applying treatment.
           **kwargs: {`power`: Expected power of the test. Typically set at 0.8,
                    `sig_level`: Expected type I error rate. Typically set at 0.05,
                    `samples_ratio`: Ratio between sample sizes of treatment and control groups,
                                    Ideally set to 1 for group sizes minimization.
                    If c0 and c1 are specified parameter will not be taken in consideration.
                    Instead optimal ratio will be set.
                    `c0`: Cost of targeting people in control group,
                    `c1`: Cost of targeting people in target group}
       Returns:
           Sample sizes for control and treatment group.
    """

    check_args(kwargs.keys(), {"power", "sig_level", "samples_ratio", "c0", "c1"})
    c0, c1, power, samples_ratio, sig_level = _get_t_test_params(kwargs)

    return sample_t_test(
        delta=delta / p,
        mean=p,
        sd0=p * (1 - p),
        sd1=(p + delta) * (1 - p + delta),
        power=power,
        sig_level=sig_level,
        samples_ratio=samples_ratio,
        c0=c0,
        c1=c1,
    )
