from fabricMedallion.architectureDeployments.update_notebook import (
    update_notebook
)

from sempy.fabric import FabricRestClient
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, Future


def update_notebooks(
        notebooks: list[tuple[str, str]],
        workspace_id: str,
        new_cells_generator: Callable[[str, list[str]], list[str]],
        ) -> list[Future]:

    """
    Updates a set of notebooks with the provided cells.

    Parameters
    -------
    notebooks: list[tuple[str, str]]
        a list of the name and the GUID of each notebook.
        [(name_1, GUID_1), ..., (name_n, GUID_n)]
    workspace_id: str
        the workspace id where the notebooks are located.
    cells_top: Callable
        a Callable that takes the name of the notebook and returns
        a list of cells to be placed at the top
    cells_botton: Callable
        a Callable that takes the name of the notebook and returns
        a list of cells to be placed at the bottom

    Returns
    -------
    futures:
        a list with the promises of the ThreadPoolExecutor.

    """
    client = FabricRestClient()
    with ThreadPoolExecutor(max_workers=128) as executor:
        futures = [
            executor.submit(
                update_notebook,
                client,
                name,
                notebook_id,
                workspace_id,
                new_cells_generator,
            )
                for name, notebook_id in notebooks
        ]

    return futures