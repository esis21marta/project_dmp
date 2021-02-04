import logging
import time

import pandas as pd

log = logging.getLogger(__name__)


def process_fb_pop(input_dict, output_csv_filename):
    log.info(f"Loading input datasets")
    # load datasets
    datasets = [
        pd.read_csv(csv_filepath).rename(columns={"population": renamed_column})
        for csv_filepath, renamed_column in input_dict.items()
    ]

    log.info(f"Preprocessing datasets")
    # round lat/lon to 7 dp
    for dataset in datasets:
        dataset[["latitude", "longitude"]] = dataset[["latitude", "longitude"]].round(
            decimals=7
        )
        # dataset = dataset.set_index(['latitude', 'longitude'])
    datasets = [dataset.set_index(["latitude", "longitude"]) for dataset in datasets]

    log.info(f"Concatenating datasets")
    # concatenate list of datasets
    output_dataset = pd.concat(datasets, axis=1)

    log.info(f"Removing nulls from concatenated dataset")
    # filter non-null rows
    output_dataset = output_dataset[~output_dataset.isnull().any(axis=1)].reset_index()

    # drop duplicates
    output_dataset = output_dataset.drop_duplicates()

    timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
    log.info(f"Saving output to {output_csv_filename}_{timestamp}.csv")
    output_dataset.to_csv(f"{output_csv_filename}_{timestamp}.csv", index=False)
    log.info(f"Complete!")
