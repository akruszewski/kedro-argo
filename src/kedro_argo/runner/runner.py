from pathlib import Path
from typing import Any, Dict, Union

from kedro.extras.datasets.pickle import PickleDataSet
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
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

    def run(
        self, pipeline: Pipeline, catalog: DataCatalog, run_id: str = None
    ) -> Dict[str, Any]:
        """Run the ``Pipeline`` using the ``DataSet``s provided by ``catalog``
        and save results back to the same objects.

        Args:
            pipeline: The ``Pipeline`` to run.
            catalog: The ``DataCatalog`` from which to fetch data.
            run_id: The id of the run.

        Raises:
            ValueError: Raised when ``Pipeline`` inputs cannot be satisfied.

        Returns:
            Any node outputs that cannot be processed by the ``DataCatalog``.
            These are returned in a dictionary, where the keys are defined
            by the node outputs.

        """

        catalog = catalog.shallow_copy()

        unsatisfied = pipeline.inputs() - set(catalog.list())
        # If ephemeral dataset exists in _tmp_path add it to catalog,
        # otherwise raise ValueError.
        for ds_name in unsatisfied:
            if (Path(self._tmp_path) / f"{ds_name}.pkl").exists():
                catalog.add(ds_name, self.create_default_data_set(ds_name))
            else:
                raise ValueError(
                    "Pipeline input(s) {} not found in the "
                    "DataCatalog".format(unsatisfied)
                )

        free_outputs = pipeline.outputs() - set(catalog.list())
        unregistered_ds = pipeline.data_sets() - set(catalog.list())
        for ds_name in unregistered_ds:
            catalog.add(ds_name, self.create_default_data_set(ds_name))

        if self._is_async:
            self._logger.info(
                "Asynchronous mode is enabled for loading and saving data"
            )
        self._run(pipeline, catalog, run_id)

        self._logger.info("Pipeline execution completed successfully.")

        return {ds_name: catalog.load(ds_name) for ds_name in free_outputs}


