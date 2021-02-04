import shlex
import subprocess
from pathlib import Path

import pyspark
from kedro.io import DataSetError


def run_bash_command(command: str) -> bytes:
    """
    Run bash command
    :param command: bash command
    :return: standard output of command run
    """
    process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE)
    stdout = process.communicate()[0]
    return stdout


def send_file_to_telegram(file_path: str, caption: str = None) -> None:
    """
    Send file to telegram bot
    :param file_path: path of file which needs to be delivered to telegram
    :param caption: Header of telegram message
    """
    if caption:
        command = f'telegram-send --file {file_path} --caption "{caption}"'
    else:
        command = f"telegram-send --file {file_path}"
    stdout = run_bash_command(command=command)


def send_text_to_telegram(text: str) -> None:
    """
    Send message to telegram bot
    :param text: Text message
    """
    command = f'telegram-send --format markdown "{text}"'
    stdout = run_bash_command(command=command)


def send_html_to_telegram(html_string: str) -> None:
    """
    Send HTML Message to telegram bot
    :param html_string: HTML string
    """
    command = f'telegram-send --format html "{html_string}"'
    stdout = run_bash_command(command=command)


def send_image_to_telegram(image_path: str, caption: str = None) -> None:
    """
    Sends image to telegram bot
    :param image_path: Path of image file
    :param caption: Header of telegram message
    """
    if caption:
        command = f'telegram-send --image {image_path} --caption "{caption}"'
    else:
        command = f"telegram-send --image {image_path}"
    stdout = run_bash_command(command=command)


def send_data_frame_to_telegram(
    df: pyspark.sql.DataFrame, html_file_name: str, caption: str = None
) -> None:
    """
    Sends PySpark DataFrame to the telegram bot
    :param df: PySpark DataFrame
    :param html_file_name: HTML File Name for the DF to send
    :param caption: Header of telegram message
    """
    column_list = df.columns
    if "msisdn" in column_list or list(
        filter(lambda k: k.startswith("fea_"), column_list)
    ):
        raise DataSetError(
            "df contains MSISDN or Feature column, any confidential data can't send be sent to telegram"
        )
    html = df.toPandas().to_html(index=False, border=5, bold_rows=True)
    if caption:
        html = f"<b><i>{caption}</i></b>{html}"
    html_file_path = str(Path.cwd() / "data" / "telegram-bot" / html_file_name)
    f = open(html_file_path, "w")
    f.write(html)
    f.close()
    send_file_to_telegram(file_path=html_file_path, caption=caption)
