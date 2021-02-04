import logging
import time

import pandas as pd

log = logging.getLogger(__name__)


def preprocess_4g_bts_excel(input_excel_filename, output_csv_filename):
    log.info(f"Reading input excel file: {input_excel_filename}")
    data = pd.read_excel(input_excel_filename)

    players = ["tsel", "xl", "isat", "three", "smartfren"]
    value_cols = [f"value_{player}" for player in players] + [
        f"share_{player}" for player in players
    ]
    cols = ["area", "region", "district", "year", "month"] + value_cols

    # create output dataframe
    output_df = pd.DataFrame(columns=cols)

    # filter out useless data
    data = data.iloc[1:, :13]

    # fillna
    data.fillna(0.0, inplace=True)

    for idx, row in data.iterrows():
        # for jan 2019
        value_row = [row[i] for i in range(3, 8)]
        total = sum(value_row)
        if total != 0.0:
            percent_row = [round(value * 100 / total, 2) for value in value_row]
            parsed_row = (
                [row[0][-1], row[1].lower(), row["KABUPATEN"].lower(), 2019, 1]
                + value_row
                + percent_row
            )
            output_df.loc[len(output_df)] = parsed_row

        # for oct 2019
        value_row = [row[i] for i in range(8, 13)]
        total = sum(value_row)
        if total != 0.0:
            percent_row = [round(value * 100 / total, 2) for value in value_row]
            parsed_row = (
                [row[0][-1], row[1].lower(), row["KABUPATEN"].lower(), 2019, 10]
                + value_row
                + percent_row
            )
            output_df.loc[len(output_df)] = parsed_row

    # drop duplicates
    output_df = output_df.drop_duplicates()

    timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
    log.info(f"Saving output to csv file: {output_csv_filename}_{timestamp}.csv")
    output_df.to_csv(f"{output_csv_filename}_{timestamp}.csv", index=False)
    log.info("Complete!")
