import itertools
from glob import glob
from pathlib import Path
from typing import Any, Dict

from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog, MemoryDataSet
from kedro.pipeline import Pipeline
from kedro.extras.datasets.pickle import PickleDataSet


class PicleMemoryDataSetHook:
    """Replace all MemoryDataSets with PicleDataSet"""

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
                catalog._data_sets[dataset_name] = PickleDataSet(
                    filepath=f"data/tmp/{dataset_name}.pkl"
                )
