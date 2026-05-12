# Databricks notebook source
# Gold Layer: Run dbt models via dbt Python API
# Reads from silver Delta tables, writes gold Delta tables to dbw_cms_medallion_dev.gold
#
# NOTE: This notebook is kept for reference only and is NOT used by the Databricks Workflow.
# Installing dbt-databricks inside a notebook upgrades protobuf, breaking Databricks internals.
# The workflow runs run_gold_models.py instead, which executes the same SQL via spark.sql().

# COMMAND ----------

%pip install dbt-databricks -q

# COMMAND ----------

import os
import yaml
from dbt.cli.main import dbtRunner, dbtRunnerResult

# COMMAND ----------

workspace_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
project_dir    = f"/Workspace/Users/{workspace_user}/dbt_project/cms_medallion"
profiles_dir   = f"/Workspace/Users/{workspace_user}/dbt_project"

# Verify project directory exists
assert os.path.isdir(project_dir), f"dbt project not found at {project_dir}"
print(f"Project files: {os.listdir(project_dir)}")

# Write profiles.yml — PAT pulled from secret scope at runtime
pat    = dbutils.secrets.get(scope="cms", key="databricks_pat")
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host   = context.apiUrl().get().replace("https://", "")

profiles = {
    "cms_medallion": {
        "outputs": {
            "dev": {
                "type":      "databricks",
                "host":      host,
                "http_path": "sql/protocolv1/o/<workspace-id>/<warehouse-id>",
                "token":     pat,
                "catalog":   "dbw_cms_medallion_dev",
                "schema":    "gold",
                "threads":   4,
            }
        },
        "target": "dev",
    }
}

os.makedirs(profiles_dir, exist_ok=True)
with open(f"{profiles_dir}/profiles.yml", "w") as f:
    yaml.dump(profiles, f)

print(f"profiles.yml written. Running dbt at: {project_dir}")

# COMMAND ----------

dbt = dbtRunner()
res: dbtRunnerResult = dbt.invoke([
    "run",
    "--project-dir", project_dir,
    "--profiles-dir", profiles_dir,
    "--log-level", "info",
])

if res.exception:
    raise Exception(f"dbt failed with exception: {res.exception}")

if res.result is None:
    raise Exception("dbt returned no results — check logs above for initialization errors")

for r in res.result.results:
    status = "OK" if r.status.value in ("success", "pass") else "FAIL"
    print(f"  [{status}] {r.node.name}")

if not res.success:
    raise Exception("dbt run failed — check logs above")

print("\ndbt run complete.")
