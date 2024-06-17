from .assets import (
    bronze_raw_breweries,
    silver_dim_breweries,
    gold_dim_breweries,
)
from .io_managers import DeltaLakeIOManager, TextIOManager
from dagster import Definitions
import os

# Definitions
def_root_uri = os.environ["STORAGE_PATH"]
defs = Definitions(
    assets=[bronze_raw_breweries, silver_dim_breweries, gold_dim_breweries],
    resources={
        "io_manager_delta": DeltaLakeIOManager(root_uri=def_root_uri),
        "io_manager_text": TextIOManager(root_uri=def_root_uri),
    },
)
