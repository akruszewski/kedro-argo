import itertools
from glob import glob
from pathlib import Path
from typing import Any, Dict, Union

from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog, MemoryDataSet
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from kedro.extras.datasets.pickle import PickleDataSet


class PicleMemoryDataSetHook:
    """Replace all MemoryDataSets with PicleDataSet"""

    def __init__(self, tmp_path: Union[str, Path] = Path("data/tmp")):
        self._data_sets: Dict[str, Any] = {}
        self._tmp_path = Path(tmp_path)

    @hook_impl
    def before_pipeline_run(
       self, run_params: Dict[str, Any], pipeline: Pipeline, catalog: DataCatalog
    ) -> None:
        """Hook to be invoked before a pipeline runs.

        Args:
           run_params: The params used to run the pipeline.
               Should be identical to the data logged by Journal with the following schema::
               {
                   "run_id": str
                   "project_path": str,
                                                                                                                                                                      "env": str,
                   "kedro_version": str,
                   "tags": Optional[List[str]],
                   "from_nodes": Optional[List[str]],
                                                                                                                                                                                                                                                          "to_nodes": Optional[List[str]],
                   "node_names": Optional[List[str]],
                   "from_inputs": Optional[List[str]],
                   "load_versions": Optional[List[str]],
                                                                                                                                                                                                                                                                                                                                              "pipeline_name": str,
                   "extra_params": Optional[Dict[str, Any]]
               }
        pipeline: The ``Pipeline`` that will be run.
        catalog: The ``DataCatalog`` to be used during the run.
        """
        filtered_datasets = filter(
            lambda x: not (x.startswith("params:") or x == "parameters"),
            itertools.chain.from_iterable(map(
                lambda x: x.inputs, pipeline.nodes
            ))
        )
        
        for dataset_name in filtered_datasets:
            if catalog._data_sets.get(dataset_name) is None:
                self._data_sets[dataset_name] = catalog._data_sets[dataset_name] = PickleDataSet(
                    filepath=(self._tmp_path / f"{dataset_name}.pkl").as_posix()
                )

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
        for name, dataset in self._data_sets.items():
            if outputs.get(name) is not None:
                catalog._data_sets[name] = PickleDataSet(
                    filepath=(self._tmp_path / f"{name}.pkl").as_posix()
                )
                catalog._data_sets[name].save(outputs.get(name))
