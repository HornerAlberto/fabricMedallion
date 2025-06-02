from fabricMedallion.datamanagement.script_as_select import (
    script_as_select
)
from fabricMedallion.datamanagement.standardize_column import (
    standardize_column
)
from fabricMedallion.architectureDeployments.create_notebook_definition import (
    create_notebook_definition
)
from fabricMedallion.architectureDeployments.deploy_orchestrator_notebook import (
    deploy_orchestrator_notebook
)

from sempy.fabric import FabricRestClient
from pyspark.sql import SparkSession
import json
from base64 import b64encode

def deploy_silver_notebooks(
        tables: list[tuple[str, str]],
        workspace_id: str,
        spark: SparkSession,
        include_orchestrator: bool = True,
        data_domain: str = None
):
    """
    Deploys a set of notebooks for the given tables.

    Parameters
    -------
    tables: list[tuple[str, str]]
        a list of the path and the name of each bronze table as found in
        the Bronze lakehouse.
        [(path_1, table_name_1), ..., (path_n, table_name_n)]
    workspace_id: str
        the workspace id where the notebooks will be deployed.

    Returns
    -------
    None

    """
    client = FabricRestClient()
    activities = []

    for path, table_name in tables:
        code_cell_1 = """df = spark.read.format("delta").load(
            "{path}"
            "{table_name}"
        )
        """.format(
            path=path.split(table_name)[0],
            table_name=table_name)
        df = spark.read.format("delta").load(path)
        code_cell_2 = script_as_select(
            df,
            printed=False,
            cols_map=standardize_column
        )
        code_cell_3 = """df.write.format("delta").mode("overwrite").save(
            "Tables/{table_name}"
        )
        """.format(
            path=path.split(table_name)[0],
            table_name=table_name)
        code_cell_4 = """notebookutils.notebook.exit("Execution Completed")"""
        payload = create_notebook_definition([
            code_cell_1,
            code_cell_2,
            code_cell_3,
            code_cell_4,
        ])
        payload = b64encode(
            json.dumps(payload).encode("utf-8")
        ).decode("utf-8")
        if data_domain:
            display_name = f"{data_domain}_{table_name}_Bronze_To_Silver"
        else:
            display_name = f"{table_name}_Bronze_To_Silver"
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
        activities.append((display_name, display_name))
        print(f"Deploying notebook for {table_name}...")
        client.post(call, json=body)
    if include_orchestrator:
        deploy_orchestrator_notebook(
            activities=activities,
            workspace_id=workspace_id,
            data_domain=data_domain
        )
    print("Done!")
    return None