import logging
import time

import pandas as pd

log = logging.getLogger(__name__)


def preprocess_fb_share_excel(input_excel_filename, output_csv_filename):
    log.info(f"Reading input excel file: {input_excel_filename}")
    data = pd.read_excel(input_excel_filename, sheet_name=None)

    # create empty dataframe with desired fields
    columns = [
        "date",
        "city",
        "share_tsel",
        "share_xl",
        "share_isat",
        "share_three",
        "share_smartfren",
    ]
    output_df = pd.DataFrame(columns=columns)

    sheet_cols_mapping = {
        "name": "city",
        "TSEL": "share_tsel",
        "XL": "share_xl",
        "ISAT": "share_isat",
        "THREE": "share_three",
        "SMARTFREN": "share_smartfren",
    }

    for sheet, df in data.items():
        if sheet != "FB Share vs Competitors":
            df.columns = [col.upper() if col != "name" else col for col in df.columns]
            filtered_df = df.loc[:, sheet_cols_mapping.keys()]
            filtered_df.columns = sheet_cols_mapping.values()
            filtered_df["date"] = pd.to_datetime(sheet, format="%d%m%y")
            output_df = pd.concat(
                [output_df, filtered_df], ignore_index=True, sort=False
            )

    output_df.fillna(0.0, inplace=True)

    # drop duplicates
    output_df = output_df.drop_duplicates()

    timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
    log.info(f"Saving output to csv file: {output_csv_filename}_{timestamp}.csv")
    output_df.to_csv(f"{output_csv_filename}_{timestamp}.csv", index=False)
    log.info("Complete!")
