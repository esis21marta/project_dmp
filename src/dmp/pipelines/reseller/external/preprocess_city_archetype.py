import logging
import time

import pandas as pd

log = logging.getLogger(__name__)


def preprocess_city_archetypes_excel(input_excel_filename, output_csv_filename):
    log.info(f"Reading input excel file: {input_excel_filename}")
    data = pd.read_excel(input_excel_filename)

    # lowercase columns
    data.columns = [col.lower() for col in data.columns]

    # lowercase values
    data = data.applymap(lambda ele: ele.lower())

    # get area number
    data["area"] = data["area"].apply(lambda area: area[-1])

    # drop duplicates
    output_df = data.drop_duplicates()

    timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
    log.info(f"Saving output to csv file: {output_csv_filename}_{timestamp}.csv")
    output_df.to_csv(f"{output_csv_filename}_{timestamp}.csv", index=False)
    log.info("Complete!")
