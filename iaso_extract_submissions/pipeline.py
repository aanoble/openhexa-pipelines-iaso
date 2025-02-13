"""Template for newly generated pipelines."""

import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Tuple
from urllib.parse import urlparse

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
    "save_to_database",
    name="Save to database",
    type=bool,
    required=True,
    default=True,
    help="Save the submissions form data to database",
)
@parameter(
    "save_mode",
    name="Saving mode",
    type=str,
    required=True,
    help="Select saving mode if the form table already exists in the database",
    choices=["append", "replace"],
    default="replace",
)
@parameter(
    "dataset",
    name="Output dataset",
    type=Dataset,
    required=False,
    help="Dataset to store the form submissions data",
)
def iaso_extract_submissions(
    iaso_connection: IASOConnection,
    form_id: int,
    choices_to_labels: bool,
    save_to_database: bool,
    save_mode: str,
    dataset: Dataset,
):
    """Pipeline orchestration function for extracting and processing form submissions."""

    iaso = authenticate_iaso(iaso_connection)

    form_id, form_name = validate_form_existence(iaso, form_id)

    df_submissions = fetch_form_submissions(iaso, form_id)

    if choices_to_labels:
        df_submissions = map_choice_names_to_labels(iaso, df_submissions, form_id)

    df_submissions = handle_duplicate_columns(df_submissions)

    save_submissions_to_database(save_to_database, save_mode, df_submissions, form_name)

    save_submissions_to_dataset(dataset, df_submissions, form_name)


def validate_form_existence(iaso: IASO, form_id: int) -> Tuple[int, str]:
    """
    Validates if the form with the given ID exists in IASO.

    Args:
        iaso (IASO): An authenticated IASO object.
        form_id (int): The ID of the form to check.

    Returns:
        Tuple[int, str]: Form ID and name.

    Raises:
        ValueError: If the form does not exist.
    """
    try:
        response = iaso.api_client.get(f"/api/forms/{form_id}", params={"fields": {"name"}})
        form_data = response.json()
        form_name = clean_string(form_data.get("name"))
        return form_id, form_name
    except Exception:
        current_run.log_error("Form ID does not exist in IASO Instance")
        raise


@iaso_extract_submissions.task
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
        params = {
            "csv": True,
            "form_ids": form_id,
        }
        response = iaso.api_client.get("/api/instances/", params=params)

        if not response.content:
            current_run.log_error("No submissions found for the form")
            raise
        try:
            df_submissions = pl.read_csv(response.content)
        except Exception as e:
            current_run.log_error(f"Error reading submissions form data: {e}")
            raise
        return df_submissions
    except Exception as e:
        current_run.log_error(f"Error fetching form submissions: {e}")
        raise


@iaso_extract_submissions.task
def map_choice_names_to_labels(
    iaso: IASO, df_submissions: pl.DataFrame, form_id: int
) -> pl.DataFrame:
    """
    Replaces choice names with their corresponding labels in the DataFrame.

    Args:
        iaso (IASO): Authenticated IASO object.
        df_submissions (pl.DataFrame): DataFrame containing form submissions.
        form_id (int): Form ID to retrieve metadata for.

    Returns:
        pl.DataFrame: DataFrame with choice names replaced by labels.
    """

    df_choices = get_form_metadata(iaso, form_id)

    if df_choices.empty:
        current_run.log_info("No choices label found in form metadata")
        return df_submissions

    try:
        for col in df_submissions.columns:
            if col in df_choices["list_name"].unique():
                dict_choices = (
                    df_choices.loc[df_choices["list_name"] == col]
                    .set_index("name")["label"]
                    .to_dict()
                )
                df_submissions = df_submissions.with_columns(
                    pl.col(col).map_dict(dict_choices).alias(col)
                )
    except Exception as e:
        current_run.log_error(f"Error replacing choice names with labels: {e}")
        raise ValueError("Error replacing choice names with labels")

    current_run.log_info("Choice names replaced with labels successfully")

    return df_submissions


@iaso_extract_submissions.task
def handle_duplicate_columns(df_submissions: pl.DataFrame) -> pl.DataFrame:
    """
    Renames duplicate columns in the DataFrame by appending a unique suffix.

    Args:
        df_submissions (pl.DataFrame): DataFrame with potential duplicate columns.

    Returns:
        pl.DataFrame: DataFrame with unique column names.
    """
    cleaned_columns = [clean_string(col) for col in df_submissions.columns]
    duplicates_columns = {
        k: list(range(1, cleaned_columns.count(k) + 1))
        for k in cleaned_columns
        if cleaned_columns.count(k) != 1
    }
    for col in cleaned_columns:
        if col in duplicates_columns:
            cleaned_columns[cleaned_columns.index(col)] = f"{col}_{duplicates_columns[col][0]}"
            duplicates_columns[col].remove(duplicates_columns[col][0])

    df_submissions.columns = cleaned_columns
    return df_submissions


@iaso_extract_submissions.task
def save_submissions_to_database(
    save_to_database: bool, save_mode: str, df_submissions: pl.DataFrame, form_name: str
):
    """
    Saves form submissions to a database if requested.

    Args:
        save_to_db (bool): Flag indicating whether to save to the database.
        save_mode (str): Mode to use when saving the table (replace or append).
        df_submissions (pl.DataFrame): DataFrame containing the form submissions.
        form_name (str): Name of the form.
    """

    if save_to_database:
        current_run.log_info("Exporting form submissions to database")
        # dbengine = create_engine(workspace.database_url)
        df_submissions.write_database(
            table_name=f"submissions_{form_name}",
            connection=workspace.database_url,
            if_table_exists=save_mode,
        )
        current_run.log_info(f"Form submissions saved to database as table submissions_{form_name}")


@iaso_extract_submissions.task
def save_submissions_to_dataset(dataset: Dataset, df_submissions: pl.DataFrame, form_name: str):
    """
    Saves form submissions to the specified dataset.

    Args:
        output_dataset (Dataset): The dataset to store the submissions.
        df_submissions (pl.DataFrame): DataFrame containing the submissions.
        form_name (str): Name of the form.
    """

    if dataset:
        current_run.log_info("Exporting form submissions to dataset {dataset.name}")

        timestamp = datetime.now().strftime("%Y%m%d_%H:%M")
        output_dir = Path(workspace.files_path, "iaso-pipelines", "extract-submissions")
        output_dir.mkdir(exist_ok=True, parents=True)

        submissions_file = f"{output_dir}/submissions_{form_name}_{timestamp}.csv"

        df_submissions.write_csv(submissions_file)

        _version = f"submissions_{form_name}_{timestamp}"
        version = dataset.create_version(_version)
        version.add_file(submissions_file)

        submissions_file.unlink()
        output_dir.rmdir()

        current_run.log_info(f"Form submissions saved to dataset {dataset.name} successfully")


def authenticate_iaso(iaso_connection: IASOConnection) -> IASO:
    """
    Authenticates and returns an IASO object.

    Args:
        iaso_connection (IASOConnection): IASO connection details.

    Returns:
        IASO: An authenticated IASO object.
    """
    try:
        parsed = urlparse(iaso_connection.url)
        if not parsed.scheme or not parsed.netloc:
            current_run.log_error("Invalid URL format for IASO connection")
            raise
        iaso = IASO(
            f"{parsed.scheme}://{parsed.netloc}",
            iaso_connection.username,
            iaso_connection.password,
        )
        return iaso
    except Exception as e:
        current_run.log_error(f"Error while authenticating IASO: {e}")
        raise


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

    data = re.sub(r"[^\w\s-]", "", data).strip()
    return data.replace(" ", "_").lower()


def get_form_metadata(iaso: IASO, form_id: int):
    """
    Retrieves form metadata from IASO API.

    Args:
        iaso (IASO): Authenticated IASO object.
        form_id (int): Form ID to retrieve metadata.

    Returns:
        pd.DataFrame: Form metadata DataFrame.
    """
    try:
        response = iaso.api_client.get(
            f"/api/forms/{form_id}", params={"fields": {"latest_form_version"}}
        )
        response = response.json()
        url = response.get("latest_form_version", {}).get("xls_file")

        df_choices = pd.read_excel(url, sheet_name="choices")

        try:
            df_choices = df_choices.loc[
                (df_choices["list_name"].notna()) & (df_choices["label"].notna())
            ]
        except Exception:
            current_run.log_warning("Columns 'list_name' and 'label' not found in choices sheet")
        return df_choices

    except Exception as e:
        current_run.log_error(f"Error getting form metadata choices: {e}")
        raise ValueError("Error getting form metadata")


if __name__ == "__main__":
    iaso_extract_submissions()
