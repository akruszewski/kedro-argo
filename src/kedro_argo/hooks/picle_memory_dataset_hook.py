from typing import Any, Dict

from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog, MemoryDataSet
from kedro.extras.datasets.pickle import PickleDataSet
from kedro.pipeline.node import Node


class PicleMemoryDataSetHook:
    """Replace all MemoryDataSets with PicleDataSet"""

    @hook_impl
    def after_catalog_created(
        self,
        catalog: DataCatalog,
        conf_catalog: Dict[str, Any],
        conf_creds: Dict[str, Any],
        feed_dict: Dict[str, Any],
        save_version: str,
        load_versions: Dict[str, str],
        run_id: str,
    ) -> None:
        """Hooks to be invoked after a data catalog is created.
        It receives the ``catalog`` as well as
        all the arguments for ``KedroContext._create_catalog``.

        Args:
            catalog: The catalog that was created.
            conf_catalog: The config from which the catalog was created.
            conf_creds: The credentials conf from which the catalog was created.
            feed_dict: The feed_dict that was added to the catalog after creation.
            save_version: The save_version used in ``save`` operations
                for all datasets in the catalog.
            load_versions: The load_versions used in ``load`` operations
                for each dataset in the catalog.
            run_id: The id of the run for which the catalog is loaded.
        """
        for name, dataset in catalog._data_sets.items():
            if name.startswith("params:") or name == "parameters":
                continue
            if isinstance(dataset, MemoryDataSet):
                catalog._data_sets[name] = PickleDataSet(filepath=f"data/tmp/{name}.pkl")

    @hook_impl
    def before_node_run(
        self,
        node: Node,
        catalog: DataCatalog,
        inputs: Dict[str, Any],
        is_async: bool,
        run_id: str,
    ) -> None:
        """Hook to be invoked before a node runs.
        The arguments received are the same as those used by ``kedro.runner.run_node``

        Args:
            node: The ``Node`` to run.
            catalog: A ``DataCatalog`` containing the node's inputs and outputs.
            inputs: The dictionary of inputs dataset.
                The keys are dataset names and the values are the actual loaded input data,
                not the dataset instance.
            is_async: Whether the node was run in ``async`` mode.
            run_id: The id of the run.
        """
        for name, dataset in catalog._data_sets.items():
            if name.startswith("params:") or name == "parameters":
                continue
            if isinstance(dataset, MemoryDataSet):
                catalog._data_sets[name] = PickleDataSet(filepath=f"data/tmp/{name}.pkl")

    @hook_impl
    def after_node_run(  # pylint: disable=too-many-arguments
        self,
        node: Node,
        catalog: DataCatalog,
        inputs: Dict[str, Any],
        outputs: Dict[str, Any],
        is_async: bool,
        run_id: str,
    ) -> None:
        """Hook to be invoked after a node runs.
        The arguments received are the same as those used by ``kedro.runner.run_node``
        as well as the ``outputs`` of the node run.

        Args:
            node: The ``Node`` that ran.
            catalog: A ``DataCatalog`` containing the node's inputs and outputs.
            inputs: The dictionary of inputs dataset.
                The keys are dataset names and the values are the actual loaded input data,
                not the dataset instance.
            outputs: The dictionary of outputs dataset.
                The keys are dataset names and the values are the actual computed output data,
                not the dataset instance.
            is_async: Whether the node was run in ``async`` mode.
            run_id: The id of the run.
        """
        for name, dataset in catalog._data_sets.items():
            if name.startswith("params:") or name == "parameters":
                continue
            if isinstance(dataset, MemoryDataSet):
                catalog._data_sets[name] = PickleDataSet(filepath=f"data/tmp/{name}.pkl")
