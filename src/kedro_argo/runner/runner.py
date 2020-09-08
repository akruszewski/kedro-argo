from pathlib import Path
from typing import Union

from kedro.extras.datasets.pickle import PickleDataSet
from kedro.runner import AbstractRunner, SequentialRunner


class ArgoRunner(SequentialRunner):
    """ArgoRunner is Kedro runner class.

    It uses as default dataset uses PickleDataSet.
    """

    def __init__(self, tmp_path: Union[str, Path], is_async: bool = False):
        """Instantiates the runner classs.

        Args:
            is_async: If True, the node inputs and outputs are loaded and saved
            asynchronously with threads. Defaults to False.

        """
        super().__init__(is_async=is_async)
        self._tmp_path = tmp_path

    def create_default_data_set(self, ds_name: str) -> AbstractRunner:
        """Factory method for creating the default data set for the runner.

        Args:
            ds_name: Name of the missing data set

        Returns:
            An instance of an implementation of AbstractDataSet to be used
            for all unregistered data sets.
        """
        return PickleDataSet(filepath=f"{self._tmp_path}/{ds_name}.pkl")
