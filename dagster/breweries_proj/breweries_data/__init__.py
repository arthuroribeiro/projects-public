from .assets import (
    bronze_raw_breweries,
    silver_dim_breweries,
    gold_dim_breweries,
)
from .io_managers import DeltaLakeIOManager, TextIOManager
from dagster import Definitions, define_asset_job, AssetKey, ScheduleDefinition
import os

# Job
breweries_job = define_asset_job(
    name="breweries_job",
    selection=[
        AssetKey(["bronze", "raw_breweries"]),
        AssetKey(["silver", "dim_breweries"]),
        AssetKey(["gold", "vw_breweries_agg"]),
    ],
)

# Schedule
breweries_schedule = ScheduleDefinition(job=breweries_job, cron_schedule="0 10 * * *")

# Definitions
def_root_uri = os.environ["STORAGE_PATH"]
defs = Definitions(
    jobs=[breweries_job],
    schedules=[breweries_schedule],
    assets=[bronze_raw_breweries, silver_dim_breweries, gold_dim_breweries],
    resources={
        "io_manager_delta": DeltaLakeIOManager(root_uri=def_root_uri),
        "io_manager_text": TextIOManager(root_uri=def_root_uri),
    },
)
