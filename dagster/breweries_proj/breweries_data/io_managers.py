from dagster import (
    ConfigurableIOManager,
    OutputContext,
    TableSchema,
    TableColumn,
    MetadataValue,
    InputContext,
)
from deltalake import write_deltalake, DeltaTable
from pydantic import Field
import os
from pandas import DataFrame


class DeltaLakeIOManager(ConfigurableIOManager):
    """
    Class to manage Delta files.

    Args:
        root_uri (str): Base path to save the output.
        default_schema (str, optional): File Layer (schema). Defaults to "public".
    """

    root_uri: str
    default_schema: str = "public"

    def _get_path(self, context) -> str:
        """
        Generate path to the output in the specific layer, if the asset has a prefix.

        Args:
            context (OutputContext): Dagster context.

        Returns:
            str: Path generated.
        """
        str_name_path = context.asset_key.path
        if len(str_name_path) == 1:
            str_schema = self.default_schema
        else:
            str_schema = str_name_path[0]
        str_full_path = os.path.join(self.root_uri, str_schema, str_name_path[-1])

        return str_full_path

    def handle_output(self, context: OutputContext, obj):
        """
        Writes object on Delta format.

        Args:
            context (OutputContext): Dagster context.
            obj (Dataframe): Pandas Dataframe.
        """
        # Writing Output
        str_path = self._get_path(context)
        write_deltalake(
            table_or_uri=str_path,
            data=obj,
            mode="overwrite",
            partition_by=context.definition_metadata.get("partition_expr"),
        )

        # Adding metadata
        list_cols = [
            TableColumn(name=key.name, type=key.type.type)
            for key in DeltaTable(str_path).schema().fields
        ]
        context.add_output_metadata(
            {
                "path": MetadataValue.path(str_path),
                "columns": MetadataValue.table_schema(TableSchema(list_cols)),
                "preview": MetadataValue.md(obj.head(5).to_markdown(index=0)),
            }
        )

    def load_input(self, context: InputContext) -> DataFrame:
        """
        Reads the Delta file.

        Args:
            context (InputContext): Dagster context.

        Returns:
            DataFrame: Delta in Pandas Dataframe.
        """
        return DeltaTable(self._get_path(context)).to_pandas()


class TextIOManager(ConfigurableIOManager):
    """
    Class to manage text files.

    Args:
        root_uri (str): Base path to save the output.
        output_extension (str, optional): Filename extension. Defaults to "json".
    """

    root_uri: str
    output_extension: str = Field(default="json")

    def _get_path(self, context) -> str:
        """
        Generate path to the output in the bronze layer.

        Args:
            context (OutputContext): Dagster context.

        Returns:
            str: Path generated.
        """
        str_dir = os.path.join(self.root_uri, "bronze")
        os.makedirs(str_dir, exist_ok=True)
        str_filename = f"{context.asset_key.path[-1]}.{self.output_extension}"
        str_path = os.path.join(str_dir, str_filename)

        return str_path

    def handle_output(self, context: OutputContext, str_raw: str):
        """
        Save result as text file in the "root_uri" path.

        Args:
            context (OutputContext): Dagster context.
            str_raw (str): Result to be saved in string format.
            str_extension (str): Filename extension.
        """
        str_path = self._get_path(context)
        with open(str_path, "w") as file:
            file.write(str_raw)
        context.add_output_metadata(
            {
                "path": MetadataValue.path(str_path),
                "preview": f"{str_raw[:50]}...",
            }
        )

    def load_input(self, context: InputContext) -> str:
        """
        Reads the text file.

        Args:
            context (InputContext): Dagster context.

        Returns:
            str: File content.
        """
        str_path = self._get_path(context)
        str_raw = ""
        with open(str_path, "r") as file:
            str_raw = file.read()
        return str_raw
