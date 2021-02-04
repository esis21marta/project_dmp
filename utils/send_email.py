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

import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List


def send_email(
    host: str,
    port: int,
    sender: str,
    recipients: List,
    subject: str = None,
    body: str = None,
    body_type: str = "plain",
    attachments: List[Dict[str, Any]] = None,
):
    """
    Helper to sending an email

    Args:
        host: SMTP relay host.
        port: SMTP relay port.
        sender: Sender.
        recipients: List of recipients.
        subject: Subject of email.
        body: Body of email.
        body_type: Type of mail body [HTML, TXT etc]
        attachments: Attachments of email.

    """
    s = smtplib.SMTP(host=host, port=port)
    s.starttls()

    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    if subject:
        msg["Subject"] = subject
    if body:
        msg.attach(MIMEText(body, body_type))
    if attachments:
        for a in attachments:
            part = MIMEApplication(a["body"], Name=a["name"])
            part["Content-Disposition"] = f'attachment; filename="{a["name"]}"'
            msg.attach(part)

    s.sendmail(sender, recipients, msg.as_string())
