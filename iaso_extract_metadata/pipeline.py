"""Template for newly generated pipelines."""

import re
import unicodedata
from datetime import datetime
from pathlib import Path

import polars as pl
from openhexa.sdk import (
    Dataset,
    IASOConnection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.toolbox.iaso import IASO, dataframe


@pipeline("__pipeline_id__", name="Extract IASO form metadata")
@parameter("iaso_connection", name="IASO connection", type=IASOConnection, required=True)
@parameter("form_id", name="Form ID", type=int, required=True)
@parameter(
    "db_table_name",
    name="Database table name",
    type=str,
    required=False,
    help="Target database table name for metadata (default: metadata_<form_name>)",
)
@parameter(
    "save_mode",
    name="Saving mode",
    type=str,
    required=True,
    choices=["append", "replace"],
    default="replace",
    help="Select mode if table exists",
)
@parameter(
    "dataset",
    name="Output dataset",
    type=Dataset,
    required=False,
    help="Dataset to store metadata",
)
def iaso_extract_metadata(
    iaso_connection: IASOConnection,
    form_id: int,
    db_table_name: str,
    save_mode: str,
    dataset: Dataset,
):
    """Main pipeline to extract IASO form metadata.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """

    iaso = authenticate_iaso(iaso_connection)
    form_name = get_form_name(iaso, form_id)
    metadata = fetch_form_metadata(iaso, form_id)

    table_name = db_table_name or f"metadata_{form_name}"
    export_to_database(metadata, table_name, save_mode)

    if dataset:
        export_to_dataset(metadata, dataset)

    current_run.log_info("Pipeline execution successful ✅")


def authenticate_iaso(conn: IASOConnection) -> IASO:
    """
    Authenticates and returns an IASO object.

    Args:
        conn (IASOConnection): IASO connection details.

    Returns:
        IASO: An authenticated IASO object.
    """
    try:
        iaso = IASO(conn.url, conn.username, conn.password)
        current_run.log_info("IASO authentication successful")
        return iaso
    except Exception as e:
        current_run.log_error(f"Authentication failed: {str(e)}")
        raise


def fetch_form_metadata(iaso: IASO, form_id: int) -> pl.DataFrame:
    """
    Fetches metadata for a form.

    Args:
        iaso (IASO): An authenticated IASO object.
        form_id (int): The ID of the form to fetch metadata

    Returns:
        pl.DataFrame: Metadata for the form.
    """
    questions, choices = dataframe.get_form_metadata(iaso, form_id)

    questions_df = pl.DataFrame(
        {
            "name": [v["name"] for v in questions.values()],
            "type": [v["type"] for v in questions.values()],
            "label": [v["label"] for v in questions.values()],
            "list_name": [v["list_name"] for v in questions.values()],
            "calculate": [v["calculate"] for v in questions.values()],
        }
    )
    choices_data = [
        (key, [row["name"] for row in choices_list], [row["label"] for row in choices_list])
        for key, choices_list in choices.items()
    ]

    choices_df = pl.DataFrame(
        choices_data, schema=["name", "choices_name", "choices_label"]
    ).with_columns(pl.col("choices_name").cast(pl.List(int)))

    return questions_df.join(choices_df, on="name", how="left")


def get_form_name(iaso: IASO, form_id: int) -> str:
    """
    Retrieve and sanitize form name.

    Args:
        iaso (IASO): An authenticated IASO object.
        form_id (int): The ID of the form to check.

    Returns:
        str: Form name.

    Raises:
        ValueError: If the form does not exist.
    """
    try:
        response = iaso.api_client.get(f"/api/forms/{form_id}", params={"fields": {"name"}})
        return clean_string(response.json().get("name"))
    except Exception as e:
        current_run.log_error(f"Form fetch failed: {str(e)}")
        raise ValueError("Invalid form ID")


def export_to_database(data: pl.DataFrame, table_name: str, mode: str):
    """
    Export metadata to a database table.

    Args:
        data (pl.DataFrame): Metadata to export.
        table_name (str): Name of the database table.
        mode (str): Save mode for the table.
    """

    current_run.log_info("Exporting form metadata to database")
    try:
        # To export data to database to cast some columns to string
        data = data.with_columns(
            pl.col("choices_name").cast(pl.String),
            pl.col("choices_label").cast(pl.String),
        )
        data.write_database(
            table_name=table_name,
            connection=workspace.database_url,
            if_table_exists=mode,
        )
        current_run.add_database_output(table_name)
        current_run.log_info(f"Metadata saved to database table {table_name}")
    except Exception as e:
        current_run.log_error(f"Database export failed: {str(e)}")
        raise


def export_to_dataset(data: pl.DataFrame, dataset: Dataset, form_name: str):
    """
    Export metadata to a dataset.

    Args:
        data (pl.DataFrame): Metadata to export.
        dataset (Dataset): Dataset to export to.
        form_name (str): Name of the form.
    """

    current_run.log_info(f"Exporting form metadata to dataset {dataset.name}")
    timestamp = datetime.now().strftime("%Y%m%d_%H:%M")
    output_dir = Path(workspace.files_path, "iaso-pipelines", "extract-metadata")
    output_dir.mkdir(exist_ok=True, parents=True)

    try:
        # Write metadata to csv
        file_path = output_dir / f"metadata_{form_name}_{timestamp}.csv"
        data.write_csv(file_path)

        # create/retreieve dataset version
        version_name = f"metadata_{form_name}"
        version = next((v for v in dataset.versions if v.name == version_name), None)
        version = version or dataset.create_version(version_name)

        # add file to dataset version
        version.add_file(file_path)

        current_run.log_info(f"Metadata saved to dataset {dataset.name} in {version.name} version")
    except Exception as e:
        current_run.log_error(f"Dataset export failed: {str(e)}")
        raise
    finally:
        # Clean tempory files
        if file_path.exists():
            file_path.unlink()
        
        output_dir.rmdir()
        Path(workspace.files_path, "iaso-pipelines").rmdir()


def clean_string(data) -> str:
    """
    Cleans the input string by removing unwanted characters and formatting it

    Args:
        data (str): The input string to be cleaned.
    Returns:
        str: The cleaned string.
    """

    data = unicodedata.normalize("NFD", data)
    data = "".join(c for c in data if not unicodedata.combining(c))
    # Precompile regex patterns for performance
    data = re.sub(r"[^\w\s-]", "", data).strip().replace(" ", "_").lower()
    return data


if __name__ == "__main__":
    iaso_extract_metadata()
