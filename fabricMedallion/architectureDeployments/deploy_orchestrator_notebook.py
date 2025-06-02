from fabricMedallion.architectureDeployments.create_notebook_definition import (
    create_notebook_definition
)

from sempy.fabric import FabricRestClient
import json
from base64 import b64encode


def deploy_orchestrator_notebook(
        activities: list[tuple[str, str]],
        workspace_id: str,
        timeoutPerCellInSeconds: int = 5*60,
        data_domain: str = None
        ):
    client = FabricRestClient()
    dag = {
        "activities": [
            {
                "name": activity_name,
                "path": notebook_path,
                "timeoutPerCellInSeconds": timeoutPerCellInSeconds
            }
            for activity_name, notebook_path in activities
        ]
    }
    code_cell_1 = "DAG = " + json.dumps(dag, indent=4)
    code_cell_2 = "notebookutils.notebook.runMultiple(DAG)"
    code_cell_3 = "notebookutils.session.stop()"
    payload = create_notebook_definition([
        code_cell_1,
        code_cell_2,
        code_cell_3,
    ])
    payload = b64encode(
        json.dumps(payload).encode("utf-8")
    ).decode("utf-8")
    if data_domain:
        display_name = f"Notebook_Orchestrator_{data_domain}_Silver"
    else:
        display_name = "Notebook_Orchestrator_Silver"

    body = {
        "displayName": display_name,
        "description": "Deployed Notebook",
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
    call = (f"https://api.fabric.microsoft.com/v1/"
            f"workspaces/{workspace_id}/notebooks")
    print(f"Deploying {display_name}...")
    return client.post(call, json=body)