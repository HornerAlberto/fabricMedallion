from fabricMedallion.architectureDeployments.create_notebook_definition import (
    create_notebook_definition
)

from sempy.fabric import FabricRestClient
import json
from base64 import b64encode, b64decode
from collections.abc import Callable
from time import sleep
import re

def update_notebook(
        client: FabricRestClient,
        name: str,
        notebook_id: str,
        workspace_id: str,
        new_cells_generator: Callable[[str, list[str]], list[str]],
        ) -> None:

    # get the current definition for the given notebook
    # https://learn.microsoft.com/en-us/rest/api/fabric/notebook/items/get-notebook-definition

    call = (
            "https://api.fabric.microsoft.com"
            f"/v1/workspaces/{workspace_id}"
            f"/notebooks/{notebook_id}"
            "/getDefinition"
        )
    response = client.post(
        call
    )
    operation_url = response.headers["Location"]
    response = client.get(operation_url)
    while response.json()["status"] == "Running":
        sleep(1)
        response = client.get(operation_url)
    response = client.get(operation_url + "/result")
    data = response.json()
    for part in data["definition"]["parts"]:
        match part:
            case {"path": "notebook-content.py", "payload": payload, **other}:
                payload = payload
    payload = b64decode(payload).decode("utf-8")
    current_cells = [l for l in payload.split('\n')[1:] if not (
                        (l[:7] == "# META ")
                        or (l[:31] == "# METADATA ********************")
                        or (l == "")
        )
    ]
    current_cells = [
        l for l in re.split(r'# CELL \*+\n', ("\n").join(current_cells))
            if l != ""
    ]

    # Update the notebook definition
    # https://learn.microsoft.com/en-us/rest/api/fabric/notebook/items/update-notebook-definition

    code_cells = new_cells_generator(name, current_cells)
    call = (
            "https://api.fabric.microsoft.com"
            f"/v1/workspaces/{workspace_id}"
            f"/notebooks/{notebook_id}"
            "/updateDefinition"
        )
    payload = create_notebook_definition(code_cells)
    payload = b64encode(
            json.dumps(payload).encode("utf-8")
        ).decode("utf-8")
    body = {
        "displayName": name,
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.py",
                    "payload": payload,
                    "payloadType": "InlineBase64"
                },
            ]
        }
    }
    return client.post(call, json=body)