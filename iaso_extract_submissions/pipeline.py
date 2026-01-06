"""Pipeline for extracting and processing form submissions from IASO."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Literal

import polars as pl
from openhexa.sdk import (
    IASOConnection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.sdk.datasets.dataset import Dataset
from openhexa.sdk.pipelines.parameter import IASOWidget  # type: ignore
from openhexa.toolbox.iaso import IASO, dataframe
from utils import clean_string, in_dataset_version


@pipeline("iaso_extract_submissions")
@parameter("iaso_connection", name="IASO connection", type=IASOConnection, required=True)  # type: ignore
@parameter(
    "form_id",
    name="Form ID",
    type=int,
    widget=IASOWidget.IASO_FORMS,
    connection="iaso_connection",  # type: ignore
    required=True,
)
@parameter(
    "last_updated",
    name="Last Updated Date",
    type=str,  # type: ignore
    required=False,
    help="ISO formatted date (YYYY-MM-DD) for incremental data extraction",
)
@parameter(
    "choices_to_labels",
    name="Convert Choices to Labels",
    type=bool,  # type: ignore
    default=True,
    required=False,
    help="Replace choice codes with labels",
)
@parameter(
    code="output_file_name",
    type=str,  # type: ignore
    name="Path and base name of the output file (without extension)",
    help=(
        "Path and base name of the output file (without extension) in the workspace files directory"
        "(default if ou_id is defined: "
        "`iaso-pipelines/extract-submissions/<form_name>.<output_format>`"
    ),
    required=False,
)
@parameter(
    code="output_format",
    type=str,  # type: ignore
    name="File format to use for exporting the data",
    required=False,
    default=".parquet",
    choices=[
        ".csv",
        ".parquet",
        ".xlsx",
    ],
)
@parameter(
    "db_table_name",
    name="Database table name",
    type=str,  # type: ignore
    required=False,
    help="Target database table name",
)
@parameter(
    "save_mode",
    name="Save Mode",
    type=str,  # type: ignore
    required=False,
    choices=["append", "replace"],
    help="Database write behavior for existing tables",
)
@parameter(
    "dataset",
    name="Output Dataset",
    type=Dataset,  # type: ignore
    required=False,
    help="Target OpenHEXA dataset for storing submissions file",
)
def iaso_extract_submissions(
    iaso_connection: IASOConnection,
    form_id: int,
    last_updated: str | None,
    choices_to_labels: bool | None,
    output_file_name: str | None,
    output_format: str | None,
    db_table_name: str | None,
    save_mode: str,
    dataset: Dataset | None,
):
    """Pipeline orchestration function for extracting and processing form submissions."""
    current_run.log_info("Starting form submissions extraction pipeline")

    iaso = authenticate_iaso(iaso_connection)

    form_name = get_form_name(iaso, form_id)
    cutoff_date = parse_cutoff_date(last_updated)

    submissions = fetch_submissions(iaso, form_id, cutoff_date)
    submissions = process_choices(submissions, choices_to_labels, iaso, form_id)
    submissions = deduplicate_columns(submissions)

    output_file_path = export_to_file(
        submissions, form_name, output_file_name, output_format, db_table_name, dataset
    )

    export_to_database(submissions, db_table_name, save_mode)

    export_to_dataset(file_path=output_file_path, dataset=dataset)


def authenticate_iaso(conn: IASOConnection) -> IASO:
    """Authenticates and returns an IASO object.

    Args:
        conn (IASOConnection): IASO connection details.

    Returns:
        IASO: An authenticated IASO object.
    """
    try:
        iaso = IASO(conn.url, conn.username, conn.password)
        current_run.log_info("IASO authentication successful")
        return iaso
    except Exception as exc:
        error_msg = f"IASO authentication failed: {exc}"
        current_run.log_error(error_msg)
        raise


# @iaso_extract_submissions.task
def get_form_name(iaso: IASO, form_id: int) -> str:
    """Retrieve and sanitize form name.

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
        current_run.log_error(f"Form fetch failed: {e}")
        raise


# @iaso_extract_submissions.task
def parse_cutoff_date(date_str: str | None) -> str | None:
    """Validate and parse ISO date string.

    Args:
        date_str: Input date string in YYYY-MM-DD format

    Returns:
        Validated date string or None

    Raises:
        ValueError: For invalid date formats
    """
    if not date_str:
        return None

    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        current_run.log_error("Invalid date format - must be YYYY-MM-DD")
        raise ValueError("Invalid date format") from exc


# @iaso_extract_submissions.task
def fetch_submissions(
    iaso: IASO,
    form_id: int,
    cutoff_date: str | None,
) -> pl.DataFrame:
    """Retrieve form submissions from IASO API.

    Args:
        iaso: Authenticated IASO client
        form_id: Target form identifier
        cutoff_date: Optional date filter

    Returns:
        DataFrame containing form submissions
    """
    try:
        current_run.log_info(f"Fetching submissions for form ID {form_id}")
        return dataframe.extract_submissions(iaso, form_id, cutoff_date)
    except Exception as exc:
        current_run.log_error(f"Submission retrieval failed: {exc}")
        raise


# @iaso_extract_submissions.task
def process_choices(
    submissions: pl.DataFrame, convert: bool | None, iaso_client: IASO, form_id: int
) -> pl.DataFrame:
    """Convert choice codes to human-readable labels if requested.

    Args:
        submissions: Raw submissions DataFrame
        convert: Conversion flag
        iaso_client: Authenticated IASO client
        form_id: Target form identifier

    Returns:
        Processed DataFrame with labels if requested
    """
    if not convert:
        return submissions

    try:
        form_metadata = dataframe.get_form_metadata(iaso_client, form_id)
        return dataframe.replace_labels(
            submissions=submissions,
            form_metadata=form_metadata,  # type: ignore
        )
    except Exception as exc:
        current_run.log_error(f"Choice conversion failed: {exc}")
        raise


@iaso_extract_submissions.task
def deduplicate_columns(submissions: pl.DataFrame) -> pl.DataFrame:
    """Renames duplicate columns in the DataFrame by appending a unique suffix.

    Args:
        submissions: DataFrame with potential duplicate columns.

    Returns:
        pl.DataFrame: DataFrame with unique column names.
    """
    cleaned_columns = [clean_string(col) for col in submissions.columns]
    duplicates_columns = {
        k: list(range(1, cleaned_columns.count(k) + 1))
        for k in cleaned_columns
        if cleaned_columns.count(k) != 1
    }
    for col in cleaned_columns[:]:  # Iterate over a copy of the list
        if col in duplicates_columns:
            index = cleaned_columns.index(col)
            cleaned_columns[index] = f"{col}_{duplicates_columns[col][0]}"
            duplicates_columns[col].remove(duplicates_columns[col][0])

    submissions.columns = cleaned_columns
    return _process_submissions(submissions)


@iaso_extract_submissions.task
def export_to_file(
    submissions: pl.DataFrame,
    form_name: str,
    output_file_name: str | None,
    output_format: str,
    db_table_name: str | None = None,
    dataset: Dataset | None = None,
) -> Path | None:
    """Export submissions data to specified file format.

    Args:
        submissions: DataFrame containing the form submissions.
        form_name: Name of the form to use in the output file name.
        output_file_name: Optional custom output file name. If not provided, defaults to
            `iaso-pipelines/extract-submissions/form_<form_name>.<output_format>`.
        output_format: File format extension for the output file.
        db_table_name: Optional database table name.
        dataset: Optional dataset to store the output file.

    Returns:
        Path to the exported file or None if no file was created.
    """
    if db_table_name and not (output_file_name or dataset):
        return None

    output_file_path = _generate_output_file_path(
        form_name=form_name, output_file_name=output_file_name, output_format=output_format
    )

    current_run.log_info(f"Exporting submissions fiile to: `{output_file_path}`")
    if output_format == ".csv":
        submissions.write_csv(output_file_path)
    elif output_format == ".parquet":
        submissions.write_parquet(output_file_path)
    else:
        submissions.to_pandas().to_excel(output_file_path, index=False)

    current_run.add_file_output(output_file_path.as_posix())
    current_run.log_info(f"Form submissions exported to file `{output_file_path}`")

    return output_file_path


@iaso_extract_submissions.task
def export_to_database(
    submissions: pl.DataFrame, table_name: str, mode: Literal["replace", "append"]
) -> None:
    """Saves form submissions to a database.

    Args:
        submissions: DataFrame containing the form submissions.
        table_name: Name of the database table where submissions will be saved.
        mode: Mode to use when saving the table (replace or append).
    """
    if table_name is None:
        return

    if _validate_schema(submissions, table_name):
        mode = mode or "replace"
        submissions.write_database(
            table_name=table_name,
            connection=workspace.database_url,
            if_table_exists=mode,
        )
        current_run.add_database_output(table_name)
        current_run.log_info(
            f"Form submissions saved to database {len(submissions)} rows into `{table_name}`"
        )


@iaso_extract_submissions.task
def export_to_dataset(file_path: Path, dataset: Dataset | None) -> None:
    """Saves form submissions to the specified dataset.

    Args:
        file_path (Path): The path to the file containing the submissions data.
        dataset (Dataset): The dataset where the submissions will be stored.
    """
    if dataset is None:
        return

    latest_version = dataset.latest_version
    if bool(latest_version) and in_dataset_version(file_path, latest_version):
        current_run.log_info(
            f"Form submissions file `{file_path.name}` already exists in dataset version "
            f"`{latest_version.name}` and no changes have been detected"
        )
        return

    version_number = int(latest_version.name.lstrip("v")) + 1 if latest_version else 1
    version = dataset.create_version(f"v{version_number}")
    version.add_file(file_path, file_path.name)
    current_run.log_info(
        f"Form submissions file `{file_path.name}` successfully added to {dataset.name} "
        f"dataset in version `{version.name}`"
    )


def _process_submissions(submissions: pl.DataFrame) -> pl.DataFrame:
    """Process and clean the submissions DataFrame.

    Returns:
        pl.DataFrame: The cleaned and processed submissions DataFrame.
    """
    list_cols = submissions.select(pl.col(pl.List(pl.Utf8))).columns

    binary_exprs = []
    for col in list_cols:
        unique_cats = submissions[col].drop_nulls().explode().drop_nulls().unique().to_list()

        for cat in unique_cats:
            expr = (
                pl.col(col)
                .list.contains(cat)
                .fill_null(False)
                .cast(pl.Int8)
                .alias(f"{col}_{clean_string(cat)}")
            )
            binary_exprs.append(expr)

    if binary_exprs:
        submissions = submissions.with_columns(binary_exprs)

    submissions = submissions.drop(list_cols).select(pl.exclude("instanceid"), pl.col("instanceid"))

    return submissions.select(sorted(submissions.columns)).sort(submissions.columns)


def _validate_schema(submissions: pl.DataFrame, table_name: str) -> bool:
    """Validate the schema of the submissions DataFrame against the database table.

    Args:
        submissions: The submissions DataFrame to validate.
        table_name: Name of the database table to validate against.

    Returns:
        bool: True if schema is valid
    """
    db_table_columns = (
        pl.read_database_uri(
            query=(
                f"select column_name from information_schema.columns "
                f"where table_name='{table_name}'"
            ),
            uri=workspace.database_url,
        )
        .select("column_name")
        .to_series()
        .to_list()
    )
    if (
        db_table_columns
        and set(submissions.columns).issubset(set(db_table_columns))
        and len(submissions.columns) > len(db_table_columns)
    ):
        msg = (
            f"Schema mismatch: New columns {set(submissions.columns) - set(db_table_columns)} "
            "not present in existing table"
        )
        current_run.log_critical(msg)
        return False
    return True


def _generate_output_file_path(
    form_name: str, output_file_name: str | None, output_format: str
) -> Path:
    """Generate the output file path based on provided parameters.

    Args:
        form_name: Name of the form to include in the file name.
        output_file_name: Optional custom output file name.
        output_format: File format extension for the output file.

    Returns:
        Path to the output file.
    """
    if output_file_name:
        output_file_path = Path(output_file_name)

        if not output_file_path.suffix:
            output_file_path = output_file_path.with_suffix(output_format)

        if output_file_path.suffix not in [".csv", ".parquet", ".xlsx"]:
            current_run.log_error(
                f"Unsupported output format: {output_file_path.suffix}. "
                "Supported formats are: .csv, .parquet, .xlsx"
            )
            raise ValueError(f"Unsupported output format: {output_file_path.suffix}")

        if not output_file_path.is_absolute():
            output_file_path = Path(workspace.files_path, output_file_path)

        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        return output_file_path

    output_dir = Path(workspace.files_path, "iaso-pipelines", "extract-submissions")
    output_dir.mkdir(exist_ok=True, parents=True)

    base_name = f"{clean_string(form_name)}"
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M")
    file_name = f"{base_name}_{timestamp}{output_format}"

    return output_dir / file_name


if __name__ == "__main__":
    iaso_extract_submissions()
