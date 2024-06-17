from dagster import (
    asset,
    Config,
    AssetExecutionContext,
    AssetIn,
    MetadataValue,
    RetryPolicy,
    Backoff,
)
from pydantic import Field
import requests
import requests
import json
import math
import pandas as pd


class BreweriesConfig(Config):
    int_per_page: int = Field(
        default=200,
        description="Number of breweries to return each call. Max: 200",
    )
    # str_by_ids: str = Field(default=None, description="Comma-separated list of brewery IDs.")
    # str_by_name: str = Field(default=None, description="Filter breweries by name. Note: You can use underscores or url encoding for spaces.")
    # str_by_type: str = Field(default=None, description="Filter by type of brewery. Note: You can use underscores or url encoding for spaces.")
    # str_by_city: str = Field(default=None, description="Filter breweries by city. Note: You can use underscores or url encoding for spaces.")


def build_url(config_api: BreweriesConfig) -> str:
    """
    Generate URL to API call.

    Args:
        api_args (BreweriesConfig): Config to call API.

    Returns:
        str: URL.
    """
    str_base_url = "https://api.openbrewerydb.org/v1/breweries?"
    dict_rename = {
        "int_per_page": "per_page",
        "str_by_ids": "by_ids",
        "str_by_name": "by_name",
        "str_by_type": "by_type",
        "str_by_city": "by_city",
    }
    list_args = []
    for key, value in config_api.model_dump().items():
        # Checking spaces and null values
        if value is None:
            continue
        elif " " in str(value):
            raise Exception("Parameters with space, you should use underscores or url encoding for spaces.")  # fmt: skip
        list_args.append(f"{dict_rename[key]}={value}")
    str_url = str_base_url + "&".join(list_args)

    return str_url


@asset(
    group_name="bronze",
    key_prefix=["bronze"],
    name="raw_breweries",
    metadata={"source_api": MetadataValue.url("https://openbrewerydb.org/")},
    tags={"dagster/storage_kind": "JSON"},
    io_manager_key="io_manager_text",
    retry_policy=RetryPolicy(max_retries=3, delay=30, backoff=Backoff.LINEAR),  # 30s
)
def bronze_raw_breweries(
    context: AssetExecutionContext,
    config: BreweriesConfig,
) -> str:
    """List of breweries in the raw format"""
    # Variables
    int_per_page = config.int_per_page
    str_url_base = build_url(config)
    context.log.info(f"URL Base: {str_url_base}")

    # Getting metadata information to know number of pages
    str_meta = str_url_base.replace("?", "/meta?")
    context.log.info(f"URL Meta: {str_meta}")
    json_return_meta = requests.get(str_meta)
    if json_return_meta.status_code != 200:
        raise Exception(f"API Result: (Code: {json_return_meta.status_code}) {json_return_meta.text}")  # fmt: skip
    num_total = int(json.loads(json_return_meta.text)["total"])
    num_pages = math.ceil(num_total / int_per_page)
    context.log.info(f"API Metadata: {num_total} Records in {num_pages} Pages")

    # Generating warning if result is 0
    if num_total == 0:
        context.log.warning("This request returned 0 records, review your parameters.")

    # Getting breweries list
    list_result = []
    for num_page in range(1, num_pages + 1):
        str_url_list = f"{str_url_base}&page={num_page}"
        json_return_list = requests.get(str_url_list)

        # Checking result
        if json_return_meta.status_code != 200:
            raise Exception(f"API Result: ({json_return_meta.status_code}) {json_return_meta.text}")  # fmt: skip

        # Getting data
        list_result = list_result + json.loads(json_return_list.text)
        context.log.info(f"API Getting Data: {num_page}/{num_pages}")

    # Saving output
    return json.dumps(list_result)


@asset(
    ins={
        "bronze_raw_breweries": AssetIn(
            key=["bronze", "raw_breweries"],
            input_manager_key="io_manager_text",
        )
    },
    group_name="silver",
    key_prefix=["silver"],
    name="dim_breweries",
    metadata={"partition_expr": "country"},
    tags={"dagster/storage_kind": "Delta"},
    io_manager_key="io_manager_delta",
)
def silver_dim_breweries(
    context: AssetExecutionContext,
    bronze_raw_breweries: str,
) -> pd.DataFrame:
    """List of breweries in the Delta format with the right types"""
    # Reading JSON file
    silver_breweries = pd.DataFrame(json.loads(bronze_raw_breweries), dtype="string")

    # Setting the right data types
    silver_breweries = silver_breweries.astype(
        {
            "id": "string",
            "name": "string",
            "brewery_type": "string",
            "address_1": "string",
            "address_2": "string",
            "address_3": "string",
            "city": "string",
            "state_province": "string",
            "postal_code": "string",
            "country": "string",
            "longitude": "double",
            "latitude": "double",
            "phone": "string",
            "website_url": "string",
            "state": "string",
            "street": "string",
        }
    )

    return silver_breweries


@asset(
    ins={"silver_dim_breweries": AssetIn(key=["silver", "dim_breweries"])},
    group_name="gold",
    key_prefix=["gold"],
    name="vw_breweries_agg",
    tags={"dagster/storage_kind": "Delta"},
    io_manager_key="io_manager_delta",
)
def gold_dim_breweries(
    context: AssetExecutionContext, silver_dim_breweries: pd.DataFrame
) -> pd.DataFrame:
    """Aggregated view with the quantity of breweries per type and location"""
    # Grouping values and renaming columns
    gold_breweries = silver_dim_breweries.groupby(["country", "brewery_type"], as_index=False)["id"].count()  # fmt: skip
    gold_breweries = gold_breweries.rename(
        columns={
            "brewery_type": "type",
            "id": "qty_breweries",
        }
    )

    return gold_breweries
