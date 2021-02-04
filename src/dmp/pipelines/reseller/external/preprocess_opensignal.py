import logging
import time

import pandas as pd

log = logging.getLogger(__name__)


def preprocess_opensignal_excel(input_excel_filename, output_csv_filename):
    log.info(f"Reading input excel file: {input_excel_filename}")
    data = pd.read_excel(input_excel_filename, sheet_name="Kab")

    # drop duplicates
    output_df = data.drop_duplicates()

    timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
    log.info(f"Saving output to csv file: {output_csv_filename}_{timestamp}.csv")
    output_df.to_csv(f"{output_csv_filename}_{timestamp}.csv", index=False)
    log.info("Complete!")
