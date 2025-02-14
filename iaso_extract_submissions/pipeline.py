"""Template for newly generated pipelines."""

import re
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
from openhexa.sdk import (
    Dataset,
    IASOConnection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.toolbox.iaso import IASO


@pipeline("__pipeline_id__", name="Extract IASO form submissions")
@parameter("iaso_connection", name="IASO connection", type=IASOConnection, required=True)
@parameter("form_id", name="Form ID", type=int, required=True)
@parameter(
    "choices_to_labels",
    name="Replace choice names with labels?",
    type=bool,
    default=False,
    required=True,
)
@parameter(
    "db_table_name",
    name="Database table name",
    type=str,
    required=False,
    help="Target database table name for submissions (default: submissions_form_name)",
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
    help="Dataset to store submissions data",
)
def iaso_extract_submissions(
    iaso_connection: IASOConnection,
    form_id: int,
    choices_to_labels: bool,
    db_table_name: str,
    save_mode: str,
    dataset: Dataset,
):
    """Pipeline orchestration function for extracting and processing form submissions."""
    
    iaso = authenticate_iaso(iaso_connection)

    form_name = get_form_name(iaso, form_id)

    submissions = fetch_form_submissions(iaso, form_id)

    if choices_to_labels:
        submissions = replace_choice_labels(iaso, submissions, form_id)

    submissions = deduplicate_columns(submissions)

    table_name = db_table_name or f"submissions_{form_name}"
    export_to_database(submissions, table_name, save_mode)

    if dataset:
        export_to_dataset(dataset, submissions, form_name)
    
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


def fetch_form_submissions(iaso: IASO, form_id: int) -> pl.DataFrame:
    """
    Retrieves form submissions as a Polars DataFrame from the IASO API.

    Args:
        iaso (IASO): Authenticated IASO object.
        form_id (int): Form ID for which submissions are fetched.

    Returns:
        pl.DataFrame: DataFrame containing the form submissions.

    Raises:
        Exception: If an error occurs while fetching the form submissions.
    """

    try:
        current_run.log_info("Fetch form submissions data")
        params = {
            "csv": True,
            "form_ids": form_id,
        }
        response = iaso.api_client.get("/api/instances/", params=params)

        if not response.content:
            current_run.log_error("No submissions found")
            raise
        try:
            submissions = pl.read_csv(response.content)
        except Exception as e:
            current_run.log_error(f"Error reading submissions form data: {e}")
            raise
        return submissions
    except Exception as e:
        current_run.log_error(f"Error fetching form submissions: {e}")
        raise


def replace_choice_labels(iaso: IASO, submissions: pl.DataFrame, form_id: int) -> pl.DataFrame:
    """
    Replaces choice names with their corresponding labels in the DataFrame.

    Args:
        iaso (IASO): Authenticated IASO object.
        submissions (pl.DataFrame): DataFrame containing form submissions.
        form_id (int): Form ID to retrieve metadata for.

    Returns:
        pl.DataFrame: DataFrame with choice names replaced by labels.
    """

    df_choices = fetch_form_choices(iaso, form_id)

    if df_choices.is_empty():
        current_run.log_info("No choices label found in form metadata")
        return submissions

    try:
        choice_maps = {
            col: dict(zip(df_choices["name"], df_choices["label"]))
            for col in df_choices["list_name"].unique()
        }

        submissions.with_columns(
            [
                pl.col(col).map_dict(mapping).alias(col)
                for col, mapping in choice_maps.items()
                if col in submissions.columns
            ]
        )

    except Exception as e:
        current_run.log_error(f"Error replacing choice names with labels: {e}")
        raise ValueError("Error replacing choice names with labels")

    current_run.log_info("Choice names have been successfully replaced with labels")

    return submissions


def deduplicate_columns(submissions: pl.DataFrame) -> pl.DataFrame:
    """
    Renames duplicate columns in the DataFrame by appending a unique suffix.

    Args:
        submissions (pl.DataFrame): DataFrame with potential duplicate columns.

    Returns:
        pl.DataFrame: DataFrame with unique column names.
    """
    cleaned_columns = [clean_string(col) for col in submissions.columns]
    duplicates_columns = {
        k: list(range(1, cleaned_columns.count(k) + 1))
        for k in cleaned_columns
        if cleaned_columns.count(k) != 1
    }
    for col in cleaned_columns:
        if col in duplicates_columns:
            cleaned_columns[cleaned_columns.index(col)] = f"{col}_{duplicates_columns[col][0]}"
            duplicates_columns[col].remove(duplicates_columns[col][0])

    submissions.columns = cleaned_columns
    return submissions


def export_to_database(submissions: pl.DataFrame, table_name: str, mode: str) -> None:
    """
    Saves form submissions to a database if requested.

    Args:
        save_to_db (bool): Flag indicating whether to save to the database.
        save_mode (str): Mode to use when saving the table (replace or append).
        submissions (pl.DataFrame): DataFrame containing the form submissions.
        form_name (str): Name of the form.
    """
    current_run.log_info("Exporting form submissions to database")
    submissions.write_database(
        table_name=table_name,
        connection=workspace.database_url,
        if_table_exists=mode,
    )
    current_run.add_database_output(table_name)
    current_run.log_info(
        f"Form submissions saved to database {len(submissions)} rows to {table_name}"
    )


def export_to_dataset(dataset: Dataset, submissions: pl.DataFrame, form_name: str):
    """
    Saves form submissions to the specified dataset.

    Args:
        output_dataset (Dataset): The dataset to store the submissions.
        submissions (pl.DataFrame): DataFrame containing the submissions.
        form_name (str): Name of the form.
    """

    current_run.log_info(f"Export form submissions to dataset {dataset.name}")

    timestamp = datetime.now().strftime("%Y%m%d_%H:%M")
    output_dir = Path(workspace.files_path, "iaso-pipelines", "extract-submissions")
    output_dir.mkdir(exist_ok=True, parents=True)

    try:
        # Write to temporary file
        temp_file = output_dir / f"submissions_{form_name}_{timestamp}.csv"
        submissions.write_csv(temp_file)

        # Create/retrieve dataset version
        version_name = f"submissions_{form_name}"
        version = next((v for v in dataset.versions if v.name == version_name), None)
        version = version or dataset.create_version(version_name)

        # Add and cleannup
        version.add_file(temp_file)
        temp_file.unlink()

        current_run.log_info(
            f"Form submissions successfully saved to {dataset.name} dataset in {version.name} version"
        )
    finally:
        output_dir.rmdir()


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


def fetch_form_choices(iaso: IASO, form_id: int) -> pl.DataFrame:
    """
    Retrieves form metadata from IASO API.

    Args:
        iaso (IASO): Authenticated IASO object.
        form_id (int): Form ID to retrieve metadata.

    Returns:
        pl.DataFrame: Form metadata DataFrame.
    """
    try:
        res = iaso.api_client.get(f"/api/forms/{form_id}", params={"fields": "latest_form_version"})
        xls_url = res.json().get("latest_form_version", {}).get("xls_file", "")

        if not xls_url:
            return pl.DataFrame()

        return pl.from_pandas(
            pd.read_excel(xls_url, sheet_name="choices")
            .dropna(subset=["list_name", "label"])
            .reset_index(drop=True)
        )
    except Exception as e:
        current_run.log_warning(f"Choice metadata incomplete: {str(e)}")
        return pl.DataFrame()


if __name__ == "__main__":
    iaso_extract_submissions()
