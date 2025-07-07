"""Pipeline for extracting and exporting organizational units data from IASO."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from datetime import datetime
from io import StringIO
from pathlib import Path

import geopandas as gpd
import polars as pl
import topojson as tp
from openhexa.sdk import (
    IASOConnection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.sdk.datasets.dataset import Dataset, DatasetVersion
from openhexa.toolbox.iaso import IASO, dataframe
from shapely.geometry import MultiPolygon, Point, Polygon
from sqlalchemy import create_engine

# Precompile regex pattern for string cleaning
CLEAN_PATTERN = re.compile(r"[^\w\s-]")


@pipeline("iaso_extract_orgunits")
@parameter(
    "iaso_connection",
    name="IASO connection",
    type=IASOConnection,
    required=True,
    help="Authenticated connection to IASO platform",
)
@parameter(
    "ou_type_id",
    name="Organization Unit Type ID",
    type=int,
    required=False,
    help="Specific organization unit type identifier to extract",
)
@parameter(
    code="output_file_name",
    type=str,
    name="Path and base name of the output file (without extension)",
    help=(
        "Path and base name of the output file (without extension) in the workspace files directory"
        "(default if ou_type_id is defined: "
        "`iaso-pipelines/extract-orgunits/ou_<ou_type_name>.<output_format>`"
    ),
    required=False,
)
@parameter(
    code="output_format",
    type=str,
    name="File format to use for exporting the data",
    required=False,
    default=".gpkg",
    choices=[
        ".csv",
        ".gpkg",
        ".geojson",
        ".parquet",
        ".shp",
        ".topojson",
        ".xlsx",
    ],
)
@parameter(
    "db_table_name",
    name="Database table name",
    type=str,
    required=False,
    help="Target table name for organization units storage",
)
@parameter(
    "save_mode",
    name="Saving mode",
    type=str,
    required=False,
    choices=["append", "replace"],
    help="Database write behavior for existing tables",
)
@parameter(
    "dataset",
    name="Output Dataset",
    required=False,
    type=Dataset,
    help="Target dataset for orgunits data file export",
)
def iaso_extract_orgunits(
    iaso_connection: IASOConnection,
    ou_type_id: int | None,
    output_file_name: str | None,
    output_format: str | None,
    db_table_name: str | None,
    save_mode: str,
    dataset: Dataset | None,
) -> None:
    """Extract and export organizational units data from IASO platform.

    Args:
        iaso_connection: Authenticated IASO connection parameters
        ou_type_id: Optional specific organization unit type identifier
        output_file_name: Base name for output file in workspace files directory
        output_format: File format for exporting data
        db_table_name: Target database table name for storage
        save_mode: Database write mode for existing tables
        dataset: Optional dataset for geopackage export
    """
    current_run.log_info("Starting IASO organizational units extraction pipeline")

    iaso_client = authenticate_iaso(iaso_connection)

    org_units_df = fetch_org_units(iaso_client, ou_type_id)
    org_units_df = org_units_df.select(sorted(org_units_df.columns)).sort(org_units_df.columns)

    output_file_path = export_to_file(
        org_units_df=org_units_df,
        ou_type_id=ou_type_id,
        output_file_name=output_file_name,
        output_format=output_format,
    )
    current_run.log_info(f"Data exported to file: `{output_file_path}`")

    if db_table_name:
        export_to_database(org_units_df=org_units_df, table_name=db_table_name, save_mode=save_mode)

    if dataset:
        export_to_dataset(file_path=output_file_path, dataset=dataset)

    current_run.log_info("Pipeline executed successfully ✅")


# @iaso_extract_orgunits.task
def authenticate_iaso(connection: IASOConnection) -> IASO:
    """Establish authenticated connection to IASO API.

    Args:
        connection: IASO connection parameters

    Returns:
        Authenticated IASO client instance

    Raises:
        RuntimeError: If authentication fails
    """
    try:
        iaso = IASO(connection.url, connection.username, connection.password)
        current_run.log_info("IASO authentication successful")
        return iaso
    except Exception as err:
        error_msg = f"IASO authentication failed: {err}"
        current_run.log_error(error_msg)
        raise RuntimeError(error_msg) from err


# @iaso_extract_orgunits.task
def fetch_org_units(iaso_client: IASO, ou_type_id: int | None) -> pl.DataFrame:
    """Retrieve organizational units data from IASO.

    Args:
        iaso_client: Authenticated IASO client
        ou_type_id: Optional specific organization unit type identifier

    Returns:
        DataFrame containing organizational units data

    Raises:
        ValueError: If specified org unit ID is not found
    """
    # current_run.log_info(f"{iaso_client.api_client.server_url}")
    try:
        if ou_type_id:
            response = iaso_client.api_client.get("/api/orgunittypes")
            org_type_df = pl.DataFrame(response.json()["orgUnitTypes"]).filter(
                pl.col("id") == ou_type_id
            )

            if org_type_df.is_empty():
                raise ValueError(f"No organization type found for ID {ou_type_id}")

            return get_organisation_units(iaso_client=iaso_client, ou_type_id=ou_type_id)

        return get_organisation_units(iaso_client)

    except Exception as err:
        current_run.log_error(f"Failed to fetch OrgUnit from IASO API: {err}")
        raise


def get_organisation_units(iaso_client: IASO, ou_type_id: int | None = None) -> pl.DataFrame:
    """Retrieve organizational units data from IASO.

    Args:
        iaso_client: Authenticated IASO client
        ou_type_id: Optional specific organization unit type ID

    Returns:
        DataFrame containing organizational units data

    Raises:
        ValueError: If specified org unit ID is not found
    """
    try:
        if ou_type_id:
            response = iaso_client.api_client.get(
                url="api/orgunits", params={"csv": True, "orgUnitTypeId": ou_type_id}, stream=True
            )
            response.raise_for_status()

            df_ou = pl.read_csv(StringIO(response.content.decode("utf8")))

        else:
            response = iaso_client.api_client.get(
                "/api/orgunits", params={"csv": True}, stream=True
            )
            response.raise_for_status()

            df_ou = pl.read_csv(StringIO(response.content.decode("utf8")))

        df_ou = df_ou.select(
            pl.col("ID").alias("id"),
            pl.col("Nom").alias("name"),
            pl.col("Type").alias("org_unit_type"),
            pl.col("Latitude").alias("latitude"),
            pl.col("Longitude").alias("longitude"),
            pl.col("Date d'ouverture").str.to_date("%Y-%m-%d").alias("opening_date"),
            pl.col("Date de fermeture").str.to_date("%Y-%m-%d").alias("closing_date"),
            pl.col("Date de création").str.to_datetime("%Y-%m-%d %H:%M").alias("created_at"),
            pl.col("Date de modification").str.to_datetime("%Y-%m-%d %H:%M").alias("updated_at"),
            pl.col("Source").alias("source"),
            pl.col("Validé").alias("validation_status"),
            pl.col("Référence externe").alias("source_ref"),
            *[
                pl.col(f"Ref Ext parent {lvl}").alias(f"level_{lvl}_ref")
                for lvl in range(1, 10)
                if f"Ref Ext parent {lvl}" in df_ou.columns
            ],
            *[
                pl.col(f"parent {lvl}").alias(f"level_{lvl}_name")
                for lvl in range(1, 10)
                if f"parent {lvl}" in df_ou.columns
            ],
        )
        geoms = dataframe._get_org_units_geometries(iaso_client)
        return df_ou.with_columns(
            pl.col("id")
            .map_elements(lambda x: geoms.get(x, None), return_dtype=pl.String)
            .alias("geometry")
        )

    except Exception as err:
        current_run.log_error(f"Failed to fetch OrgUnit from IASO API: {err}")
        raise


def export_to_file(
    output_format: str,
    org_units_df: pl.DataFrame,
    ou_type_id: int | None,
    output_file_name: str | None,
) -> Path:
    """Export organizational units data to specified file format.

    Args:
        output_format: File format extension for the output file.
        org_units_df: DataFrame containing organizational units data.
        ou_type_id: Optional specific organization unit type identifier.
        output_file_name: Optional custom output file name.

    Returns:
        Path: The path to the exported file.
    """
    output_file_path = _generate_output_file_path(
        output_format=output_format,
        org_units_df=org_units_df,
        ou_type_id=ou_type_id,
        output_file_name=output_file_name,
    )

    current_run.log_info(f"Exporting to file: `{output_file_path}`")

    if output_format in {".gpkg", ".geojson", ".shp", ".topojson"}:
        geo_df = _prepare_geodataframe(org_units_df)
        geo_df = geo_df.sort_values(by=list(geo_df.columns)).reset_index(drop=True)

        if output_format == ".shp":
            for col in geo_df.select_dtypes(
                include=["datetime", "datetimetz", "object", "datetime64[ns]"]
            ).columns:
                geo_df[col] = geo_df[col].astype(str)

            for col in geo_df.columns:
                if geo_df[col].dtype == "bool":
                    geo_df[col] = geo_df[col].astype(int)

                elif isinstance(geo_df[col].iloc[0], (list, dict)):
                    geo_df[col] = geo_df[col].astype(str)

        if output_format == ".topojson":
            for col in geo_df.select_dtypes(
                include=["datetime", "datetimetz", "object", "datetime64[ns]"]
            ).columns:
                geo_df[col] = geo_df[col].astype(str)

            features = json.loads(geo_df.to_json(na="null"))
            topo = tp.Topology(features, prequantize=False, topology=True)

            with Path(output_file_path).open("w", encoding="utf-8") as f:
                json.dump(topo.to_dict(), f)

        else:
            geo_df.to_file(output_file_path, driver=_get_driver(output_format), encoding="utf-8")

    else:
        if output_format == ".csv":
            org_units_df.write_csv(output_file_path)

        elif output_format == ".parquet":
            org_units_df.write_parquet(output_file_path)

        elif output_format == ".xlsx":
            org_units_df.to_pandas().to_excel(output_file_path, index=False)

    current_run.add_file_output(output_file_path.as_posix())
    if output_format == ".shp":
        for suffix in [".shx", ".dbf", ".prj", ".cpg"]:
            current_run.add_file_output(output_file_path.with_suffix(suffix).as_posix())

    return output_file_path


# @iaso_extract_orgunits.task
def export_to_database(
    org_units_df: pl.DataFrame,
    table_name: str | None,
    save_mode: str,
) -> gpd.GeoDataFrame:
    """Export organizational units data to spatial database.

    Args:
        org_units_df: Organizational units data
        table_name: Optional Target database table name
        save_mode: Database write mode

    Raises:
        RuntimeError: If database export fails
    """
    current_run.log_info("Exporting to database table")

    try:
        geo_df = _prepare_geodataframe(org_units_df)

        engine = create_engine(workspace.database_url)
        save_mode = save_mode or "replace"
        geo_df.to_postgis(table_name, engine, if_exists=save_mode, index=False)

        current_run.add_database_output(table_name)
        current_run.log_info(f"Successfully exported {len(geo_df)} units to `{table_name}`")

    except Exception as err:
        current_run.log_error(f"Database export failed: {err}")
        raise RuntimeError("Database export operation failed") from err


# @iaso_extract_orgunits.task
def export_to_dataset(file_path: Path, dataset: Dataset | None) -> None:
    """Export organizational units data to geopackage dataset.

    Args:
        file_path: Path to the exported file to be added to the dataset
        dataset: Target dataset for export
    """
    latest_version = dataset.latest_version
    if bool(latest_version) and in_dataset_version(file_path, latest_version):
        current_run.log_info(
            f"Organizational units file `{file_path.name}` already exists in dataset version "
            f"`{latest_version.name}` and no changes have been detected"
        )
        return

    version_number = int(latest_version.name.lstrip("v")) + 1 if latest_version else 1
    version = dataset.create_version(f"v{version_number}")
    
    if file_path.suffix != ".shp":
        version.add_file(file_path, file_path.name)
    else:
        for suffix in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
            version.add_file(file_path.with_suffix(suffix), file_path.with_suffix(suffix).name)

    current_run.log_info(
        f"Organizational units file `{file_path.name}` successfully added to {dataset.name} "
        f"dataset in version `{version.name}`"
    )


def _generate_output_file_path(
    output_format: str,
    org_units_df: pl.DataFrame,
    ou_type_id: int | None,
    output_file_name: str | None,
) -> Path:
    """Generate the default output file path for exported organizational units data.

    Args:
        output_format: File format extension for the output file.
        org_units_df: DataFrame containing organizational units data.
        ou_type_id: Optional specific organization unit type identifier.
        output_file_name: Optional custom output file name.

    Returns:
        Path to the output file as a string.
    """
    if output_file_name:
        output_file_path = Path(output_file_name)

        if not output_file_path.suffix:
            output_file_path = output_file_path.with_suffix(output_format)

        if output_file_path.suffix not in {
            ".csv",
            ".gpkg",
            ".geojson",
            ".parquet",
            ".shp",
            ".topojson",
            ".xlsx",
        }:
            current_run.log_error(
                f"Invalid output file format: {output_file_path.suffix}. "
                f"Supported formats are: .csv, .gpkg, .geojson, .parquet, .shp, .topojson, .xlsx"
            )
            raise ValueError(
                f"Invalid output file format: {output_file_path.suffix}. "
                f"Supported formats are: .csv, .gpkg, .geojson, .parquet, .shp, .topojson, .xlsx"
            )

        if not output_file_path.is_absolute():
            output_file_path = Path(workspace.files_path) / output_file_path

        output_file_path.parent.mkdir(parents=True, exist_ok=True)

        return output_file_path

    output_dir = Path(workspace.files_path, "iaso-pipelines", "extract-orgunits")
    output_dir.mkdir(parents=True, exist_ok=True)

    base_name = (
        "orgunits"
        if ou_type_id is None
        else f"ou_{clean_string(org_units_df['org_unit_type'].unique()[0])}"
    )
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M")
    file_name = f"{base_name}_{timestamp}{output_format}"

    return output_dir / file_name


def _prepare_geodataframe(df: pl.DataFrame) -> gpd.GeoDataFrame:
    """Convert Polars DataFrame to GeoDataFrame with proper geometry."""  # noqa: DOC201
    return (
        df.with_columns(
            pl.col("geometry").map_elements(
                convert_to_geometry,
                return_dtype=pl.Object,
            )
        )
        .to_pandas()
        .pipe(lambda df: gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326"))
    )


def convert_to_geometry(geometry_str: str) -> Point | MultiPolygon | None:
    """Convert GeoJSON string to Shapely geometry object.

    Args:
        geometry_str: GeoJSON-formatted geometry string

    Returns:
        Shapely geometry object or None for invalid inputs
    """
    try:
        geom_data = json.loads(geometry_str)
        coords = geom_data["coordinates"]

        if geom_data["type"] == "Point":
            return Point(coords[0], coords[1])

        if geom_data["type"] == "MultiPolygon":
            polygons = [Polygon(polygon) for polygon in coords[0]]
            return MultiPolygon(polygons)
    except (json.JSONDecodeError, KeyError, TypeError):
        return None


def clean_string(input_str: str) -> str:
    """Normalize and sanitize string for safe file/table names.

    Args:
        input_str: Original input string

    Returns:
        Normalized string with special characters removed
    """
    normalized = unicodedata.normalize("NFD", input_str)
    cleaned = "".join(c for c in normalized if not unicodedata.combining(c))
    sanitized = CLEAN_PATTERN.sub("", cleaned)
    return sanitized.strip().replace(" ", "_").lower()


def _get_driver(output_format: str) -> str:
    """Return the appropriate driver string for a given output file format.

    Args:
        output_format: File format extension (e.g., '.gpkg', '.shp').

    Returns:
        The corresponding driver string for the specified format.
    """
    return {
        ".gpkg": "GPKG",
        ".shp": "ESRI Shapefile",
        ".geojson": "GeoJSON",
        ".topojson": "TopoJSON",
    }[output_format]


def sha256_of_file(file_path: Path) -> str:
    """Calculate the SHA-256 hash of a file.

    Args:
        file_path (Path): Path to the file.

    Returns:
        str: SHA-256 hash of the file content.
    """
    hasher = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def in_dataset_version(file_path: Path, dataset_version: DatasetVersion) -> bool:
    """Check if a file is in the specified dataset version.

    Args:
        file_path (Path): Path to the file.
        dataset_version (DatasetVersion): The dataset version to check against.

    Returns:
        bool: True if the file is in the dataset version, False otherwise.
    """
    file_hash = sha256_of_file(file_path)
    for file in dataset_version.files:
        remote_hash = hashlib.sha256()
        remote_hash.update(file.read())
        if file_hash == remote_hash.hexdigest():
            return True
    return False


if __name__ == "__main__":
    iaso_extract_orgunits()
