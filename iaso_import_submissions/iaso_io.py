from pathlib import Path

import polars as pl
from openhexa.sdk import current_run


def read_submissions_file(file_path: Path) -> pl.DataFrame:
    """Read and validate submissions file.

    Args:
        file_path (Path): Path to the submissions file.

    Returns:
        pl.DataFrame: DataFrame containing the validated submissions data.

    Raises:
        FileNotFoundError: If the file doesn't exist
        ValueError: If file format is unsupported or file is empty
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File {file_path} does not exist")

    if file_path.stat().st_size == 0:
        raise ValueError(f"File {file_path} is empty")

    current_run.log_info(f"📁 Reading submissions file: {file_path}")

    try:
        file_readers = {
            ".csv": lambda fp: pl.read_csv(
                fp,
                infer_schema_length=10000,
                ignore_errors=False,
                truncate_ragged_lines=True,
            ),
            ".parquet": lambda fp: pl.read_parquet(fp, use_pyarrow=True),
            ".xlsx": lambda fp: pl.read_excel(fp, engine="calamine"),
            ".xls": lambda fp: pl.read_excel(fp, engine="calamine"),
        }

        suffix = file_path.suffix.lower()
        reader = file_readers.get(suffix)

        if not reader:
            supported_formats = ", ".join(file_readers.keys())
            raise ValueError(
                f"Unsupported file format: '{suffix}'. Supported formats: {supported_formats}"
            )

        df = reader(file_path)

        if df.is_empty():
            raise ValueError(f"File {file_path} is empty")

        current_run.log_info(
            f"✅ File read successfully - {len(df)} records, {len(df.columns)} columns"
        )

        return df

    except Exception as e:
        current_run.log_error(f"Unexpected error reading file: {e}")
        raise
