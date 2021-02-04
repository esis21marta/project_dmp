from typing import Any, Dict

import pai
from kedro.io import AbstractDataSet


class PAIArtifact(AbstractDataSet):
    """
    This allows us to load models (or other artifacts) from PAI using an
    entry in the data catalogue.

    Example:

        If we create an artifact like this:
        ```
        def create_artifact_x():
            x = {'name': 'x-artifact', 'value': 1}
            pai.log_artifacts({'x-in-pai': x})

        def create_artifact_y():
            y = {'name': 'y-artifact', 'value': 1}
            pai.log_artifacts({'y-in-pai': y})
        ```

        We can load it like this:
        ```
        x-from-pai:
            type: src.dmp.io.pai_artifact.PAIArtifact
            artifact_name: 'x-in-pai'
            run_id: 'some UUID'
        y-from-pai:
            type: src.dmp.io.pai_artifact.PAIArtifact
            artifact_name: 'y-in-pai'
            run_id: 'same UUID'
        ```

        I **strongly** suggest that you take steps to ensure  you do not
        mix up different `run_id` values. For example,  in model factory
        each model has a name key, which we match against what we expect
        the use case to be.  An easy mistake to make would be to mix the
        use cases up. For the above example, we might write:

        ```
        def use_artifacts(x_artifact, y_artifact):
            assert x_artifact['name'] == 'x-artifact'
            assert y_artifact['name'] == 'y-artifact'
        ```
    """

    def __init__(self, artifact_name, run_id):
        self.run_id = run_id
        self.artifact_name = artifact_name

    def _load(self) -> Any:
        pai.load_artifact(self.run_id, self.artifact_name)

    def _save(self, data: Any) -> None:
        raise NotImplementedError("Can only load from PAI")

    def _describe(self) -> Dict[str, Any]:
        return {"run_id": self.run_id, "artifact_name": self.artifact_name}
