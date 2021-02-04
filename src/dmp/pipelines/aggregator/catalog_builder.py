import logging
import os
from typing import List

from kedro.extras.datasets.spark import SparkDataSet
from kedro.io import DataCatalog
from kedro.io.core import DataSetError

from src.dmp.pipelines.core.entity import FileRecord

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatasetBuilder:
    """
    The class is used to build and load datasets programmatically.
    It uses kedro `DataCatalog` API.
    """

    def __init__(self):
        self.catalog = DataCatalog()

    def add(self, key: str, dataset):
        self.catalog.add(data_set_name=key, data_set=dataset)

    def build_with_file_records(self, records: List[FileRecord] = None):
        """
        The function loads the data given a list of FileRecord with file paths
        Args:
            records (str, List[FileRecord]): A list of records with file paths
        """
        datasets_dict = {}
        for index, record in zip(range(1, len(records) + 1), records):
            filepath = (
                os.path.join(record.file_path, record.file_name)
                if record.file_name
                else record.file_path
            )

            save_args = {"mode": "overwrite"}
            record_name = record.file_name if record.file_name else f"file_{index}"
            dataset = SparkDataSet(filepath=filepath, save_args=save_args)
            self.add(record_name, dataset)
            datasets_dict[record_name] = record.id

        success_on_loading = []
        success_record_ids = []
        rejected_record_ids = []
        for key in self.catalog.list():
            try:
                success_on_loading.append(self.catalog.load(key))
                success_record_ids.append(datasets_dict[key])
            except DataSetError as e:
                logger.error(
                    f"Error found while loading dataset {key} with reason: {e}"
                )
                rejected_record_ids.append(datasets_dict[key])
        return success_on_loading, success_record_ids, rejected_record_ids

    def build(self):
        return self._load()

    def _load(self):
        self._loaded_ds = []
        for key in self.catalog.list():
            try:
                self._loaded_ds.append(self.catalog.load(key))
            except DataSetError as e:
                logger.error(
                    f"Error found while loading dataset {key} with reason: {e}"
                )
        return self._loaded_ds
