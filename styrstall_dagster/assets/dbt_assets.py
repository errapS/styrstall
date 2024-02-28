from pathlib import Path
import os, json
from dagster_dbt import DbtCliResource, dbt_assets
from dagster import AssetExecutionContext, DailyPartitionsDefinition

dbt_project_dir = Path(__file__).joinpath("..", "..","..","styrstall_dbt").resolve()
dbt = DbtCliResource(project_dir=os.fspath(dbt_project_dir),profiles_dir=os.fspath(dbt_project_dir))

if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_parse_invocation = dbt.cli(["parse"]).wait()
    dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")
else:
    dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")

@dbt_assets(
        manifest=dbt_manifest_path,
        exclude="obt_trips"
)
def all_dbt_assets(context: AssetExecutionContext):
    yield from dbt.cli(["build"], context=context).stream()

@dbt_assets(
        manifest=dbt_manifest_path,
        partitions_def=DailyPartitionsDefinition(start_date="2024-01-01"),
        select="obt_trips"
)
def partitioned_dbt_assets(context: AssetExecutionContext):
    start, end = context.partition_time_window

    dbt_vars = {
        "min_date": start.isoformat(),
        "max_date": end.isoformat()
    }
    print('dbdbdbdbdb  ', dbt_vars)
    dbt_build_args = ["build", "--vars", json.dumps(dbt_vars)]

    yield from dbt.cli(dbt_build_args, context=context).stream()

