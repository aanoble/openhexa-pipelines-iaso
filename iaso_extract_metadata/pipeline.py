"""Template for newly generated pipelines."""

import re
import unicodedata
from datetime import datetime
from pathlib import Path

import polars as pl
import xlsxwriter
from openhexa.sdk import (
    Dataset,
    IASOConnection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.toolbox.iaso import IASO, dataframe


@pipeline("iaso_extract_metadata")
@parameter("iaso_connection", name="IASO connection", type=IASOConnection, required=True)
@parameter("form_id", name="Form ID", type=int, required=True)
@parameter(
    code="output_file_name",
    type=str,
    name="Path and base name of the output file (without extension)",
    help=(
        "Path and base name of the output file (without extension) in the workspace files directory"
        "(default if ou_id is defined: "
        "`iaso-pipelines/extract-metadata/md_<form_name>.<output_format>`"
    ),
    required=False,
)
@parameter(
    code="output_format",
    type=str,
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
    type=str,
    required=False,
    help="Target database table name for metadata",
)
@parameter(
    "save_mode",
    name="Saving mode",
    type=str,
    required=False,
    choices=["append", "replace"],
    help="Select mode if table exists",
)
@parameter(
    "dataset",
    name="Output dataset",
    type=Dataset,
    required=False,
    help="Dataset to store metadata file",
)
def iaso_extract_metadata(
    iaso_connection: IASOConnection,
    form_id: int,
    output_file_name: str,
    output_format: str,
    db_table_name: str,
    save_mode: str,
    dataset: Dataset,
):
    """Main pipeline to extract IASO form metadata.

    Pipeline functions should only call tasks and should never perform
    IO operations or expensive computations.
    """
    iaso = authenticate_iaso(iaso_connection)
    form_name = get_form_name(iaso, form_id)

    questions, choices = fetch_form_metadata(iaso, form_id)

    output_file_path = export_to_file(
        questions,
        choices,
        form_name,
        output_file_name,
        output_format,
    )

    if db_table_name:
        export_to_database(questions, choices, db_table_name, save_mode)

    if dataset:
        export_to_dataset(output_file_path, dataset)

    current_run.log_info("Pipeline execution successful ✅")


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
    except Exception as e:
        current_run.log_error(f"Authentication failed: {e}")
        raise


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
        raise ValueError("Invalid form ID") from e


def fetch_form_metadata(iaso: IASO, form_id: int) -> pl.DataFrame:
    """Fetches metadata for a form.

    Args:
        iaso (IASO): An authenticated IASO object.
        form_id (int): The ID of the form to fetch metadata

    Returns:
        pl.DataFrame: Metadata for the form.
    """
    form_metadata = dataframe.get_form_metadata(iaso, form_id)

    valid_versions = {k: v for k, v in form_metadata.items() if isinstance(k, int)}
    if valid_versions:
        latest_dt = max(valid_versions)
        latest_version = valid_versions[latest_dt]

    else:
        latest_dt = next(iter(form_metadata))
        latest_version = form_metadata[latest_dt]

    questions = latest_version.get("questions", {})
    choices = latest_version.get("choices", {})

    questions = pl.DataFrame(
        {
            "name": [v["name"] for v in questions.values()],
            "type": [v["type"] for v in questions.values()],
            "label": [v["label"] for v in questions.values()],
            "calculate": [v["calculate"] for v in questions.values()],
        }
    ).sort("label")

    choices = pl.DataFrame(
        [
            (key, choice["name"], choice["label"])
            for key, choices_list in choices.items()
            for choice in choices_list
        ],
        schema=["name", "choice_value", "choice_label"],
    )

    return questions, choices


def export_to_file(
    questions: pl.DataFrame,
    choices: pl.DataFrame,
    form_name: str,
    output_file_name: str,
    output_format: str,
) -> Path:
    """Export metadata to a file in the specified format.

    Args:
        questions (pl.DataFrame): Metadata questions to export.
        choices (pl.DataFrame): Metadata choices to export.
        form_name (str): Name of the form.
        output_file_name (str): Base name of the output file.
        output_format (str): File format to use for exporting the data.

    Returns:
       Path: Path to the exported file.
    """
    output_file_path = _generate_output_file_path(
        form_name=form_name, output_file_name=output_file_name, output_format=output_format
    )

    current_run.log_info(f"Exporting form metadata to file `{output_file_path}`")
    if output_format == ".xlsx":
        with xlsxwriter.Workbook(output_file_path) as workbook:
            questions.write_excel(workbook, workbook.add_worksheet("Questions"))
            choices.write_excel(workbook, workbook.add_worksheet("Choices"))

    elif output_format == ".parquet":
        questions.join(choices, on="name", how="left").sort("label").write_parquet(output_file_path)

    elif output_format == ".csv":
        questions.join(choices, on="name", how="left").sort("label").write_csv(output_file_path)

    current_run.add_file_output(output_file_path.as_posix())
    current_run.log_info(f"Metadata saved to file `{output_file_path}`")
    return output_file_path


def export_to_database(questions: pl.DataFrame, choices: pl.DataFrame, table_name: str, mode: str):
    """Export metadata to a database table.

    Args:
        questions (pl.DataFrame): Metadata questions.
        choices (pl.DataFrame): Metadata choices.
        table_name (str): Name of the database table.
        mode (str): Save mode for the table.
    """
    current_run.log_info("Exporting form metadata to database")
    try:
        metadata = questions.join(choices, on="name", how="left").sort("label")
        mode = mode or "replace"

        metadata.write_database(
            table_name=table_name,
            connection=workspace.database_url,
            if_table_exists=mode,
        )
        current_run.add_database_output(table_name)
        current_run.log_info(f"Metadata saved to database table {table_name}")
    except Exception as e:
        current_run.log_error(f"Database export failed: {e}")
        raise


def export_to_dataset(file_path: Path, dataset: Dataset):
    """Export metadata to a dataset.

    Args:
        file_path (Path): Path to the output file.
        dataset (Dataset): Dataset to export to.

    Raises:
        RuntimeError: If dataset export fails
    """
    try:
        stem = Path(file_path).stem
        match = re.match(r"^(.*)_\d{4}-\d{2}-\d{2}_\d{2}:\d{2}$", stem)
        file_name = match.group(1) if match else clean_string(stem)

        version = next((v for v in dataset.versions if v.name == file_name), None)
        version = version or dataset.create_version(file_name)

        try:
            version.add_file(file_path, file_path.name)
        except ValueError as err:
            if err.args and "already exists" in err.args[0]:
                msg_critical = (
                    f"File `{file_path.name}` already exists in dataset version `{version.name}`. "
                )
                current_run.log_critical(msg_critical)

                file_name = file_path.with_name(
                    f"{file_path.name}_{datetime.now().strftime('%Y-%m-%d_%H:%M')}{file_path.suffix}"
                ).name

                current_run.log_info(f"Renaming file to `{file_name}` to avoid conflict")

                version.add_file(file_path, file_name)

        current_run.log_info(f"Metadata saved to dataset {dataset.name} in {version.name} version")
    except Exception as e:
        current_run.log_error(f"Dataset export failed: {e}")
        raise


def clean_string(data: str) -> str:
    """Cleans the input string by removing unwanted characters and formatting it.

    Args:
        data (str): The input string to be cleaned.

    Returns:
        str: The cleaned string.
    """
    data = unicodedata.normalize("NFD", data)
    data = "".join(c for c in data if not unicodedata.combining(c))
    # Precompile regex patterns for performance
    return re.sub(r"[^\w\s-]", "", data).strip().replace(" ", "_").lower()


def _generate_output_file_path(form_name: str, output_file_name: str, output_format: str) -> Path:
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

    output_dir = Path(workspace.files_path, "iaso-pipelines", "extract-metadata")
    output_dir.mkdir(exist_ok=True, parents=True)

    base_name = f"md_{clean_string(form_name)}"
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M")
    file_name = f"{base_name}_{timestamp}{output_format}"

    return output_dir / file_name


if __name__ == "__main__":
    iaso_extract_metadata()
