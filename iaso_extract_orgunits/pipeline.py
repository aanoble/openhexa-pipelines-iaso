"""Pipeline for extracting and exporting organizational units data from IASO."""

from __future__ import annotations

import json
import re
import unicodedata
from datetime import datetime
from pathlib import Path

import geopandas as gpd
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
    "ou_id",
    name="Organization Unit ID",
    type=int,
    required=False,
    help="Specific organization unit identifier to extract",
)
@parameter(
    "db_table_name",
    name="Database table name",
    type=str,
    required=False,
    help=(
        "Target table name for organization units storage"
        "(default if ou_id is define: ou_<ou_type_name>, else <orgunits>)"
    ),
)
@parameter(
    "save_mode",
    name="Saving mode",
    type=str,
    required=False,
    choices=["append", "replace"],
    default="replace",
    help="Database write behavior for existing tables",
)
@parameter(
    "dataset",
    name="Output Dataset",
    required=False,
    type=Dataset,
    help="Target dataset for geopackage export",
)
def iaso_extract_orgunits(
    iaso_connection: IASOConnection,
    ou_id: int | None,
    db_table_name: str | None,
    save_mode: str,
    dataset: Dataset | None,
) -> None:
    """Extract and export organizational units data from IASO platform.

    Args:
        iaso_connection: Authenticated IASO connection parameters
        ou_id: Optional specific organization unit identifier
        db_table_name: Target database table name for storage
        save_mode: Database write mode for existing tables
        dataset: Optional dataset for geopackage export
    """
    current_run.log_info("Starting IASO organizational units extraction pipeline")
    iaso_client = authenticate_iaso(iaso_connection)
    org_units_df = fetch_org_units(iaso_client, ou_id)
    geo_df = export_to_database(org_units_df, ou_id, db_table_name, save_mode)
    export_to_dataset(geo_df, ou_id, dataset, db_table_name)


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
def fetch_org_units(iaso_client: IASO, org_unit_id: int | None) -> pl.DataFrame:
    """Retrieve organizational units data from IASO.

    Args:
        iaso_client: Authenticated IASO client
        org_unit_id: Optional specific organization unit ID

    Returns:
        DataFrame containing organizational units data

    Raises:
        ValueError: If specified org unit ID is not found
    """
    # current_run.log_info(f"{iaso_client.api_client.server_url}")
    try:
        if org_unit_id:
            response = iaso_client.api_client.get("/api/orgunittypes")
            org_type_df = pl.DataFrame(response.json()["orgUnitTypes"]).filter(
                pl.col("id") == org_unit_id
            )

            if org_type_df.is_empty():
                raise ValueError(f"No organization type found for ID {org_unit_id}")

            org_type_name = org_type_df["name"][0]
            return dataframe.get_organisation_units(iaso_client).filter(
                pl.col("org_unit_type") == org_type_name
            )

        return dataframe.get_organisation_units(iaso_client)

    except Exception as err:
        current_run.log_error(f"Failed to fetch OrgUnit from IASO API: {err}")
        raise


# @iaso_extract_orgunits.task
def export_to_database(
    org_units_df: pl.DataFrame,
    org_unit_id: int | None,
    table_name: str | None,
    save_mode: str,
) -> gpd.GeoDataFrame:
    """Export organizational units data to spatial database.

    Args:
        org_units_df: Organizational units data
        org_unit_id: Optional specific organization unit ID
        table_name: Optional Target database table name
        save_mode: Database write mode

    Returns:
        GeoDataFrame containing exported data

    Raises:
        RuntimeError: If database export fails
    """
    current_run.log_info("Exporting to database table")

    try:
        final_table = _generate_table_name(table_name, org_units_df, org_unit_id)
        geo_df = _prepare_geodataframe(org_units_df)

        engine = create_engine(workspace.database_url)
        geo_df.to_postgis(final_table, engine, if_exists=save_mode)

        current_run.add_database_output(final_table)
        current_run.log_info(f"Successfully exported {len(geo_df)} units to `{final_table}`")
        return geo_df

    except Exception as err:
        current_run.log_error(f"Database export failed: {err}")
        raise RuntimeError("Database export operation failed") from err


# @iaso_extract_orgunits.task
def export_to_dataset(
    geo_df: gpd.GeoDataFrame,
    org_unit_id: int | None,
    dataset: Dataset | None,
    table_name: str,
) -> None:
    """Export organizational units data to geopackage dataset.

    Args:
        geo_df: GeoDataFrame containing spatial data
        org_unit_id: Optional specific organization unit ID
        dataset: Target dataset for export
        table_name: Base name for geopackage file

    Raises:
        RuntimeError: If dataset export fails
    """
    if not dataset:
        current_run.log_info("Pipeline execution successful ✅")
        return

    try:
        final_name = _generate_table_name(table_name, geo_df, org_unit_id)
        output_path = _prepare_output_path(final_name)

        geo_df.to_file(output_path, driver="GPKG")
        version = _get_or_create_dataset_version(dataset, final_name)
        version.add_file(output_path)

        current_run.log_info(
            f"Exported {len(geo_df)} units to dataset `{dataset.name}` in `{version.name}` version"
        )
        current_run.log_info("Pipeline execution successful ✅")

    except Exception as err:
        current_run.log_error(f"Dataset export failed: {err}")
        raise RuntimeError("Dataset export operation failed") from err
    finally:
        _clean_temp_files(output_path)


def _generate_table_name(
    base_name: str, data: pl.DataFrame | gpd.GeoDataFrame, org_unit_id: int | None
) -> str:
    """Generate standardized table name based on inputs."""  # noqa: DOC201
    if base_name:
        return clean_string(base_name)
    if org_unit_id:
        return f"ou_{clean_string(data['org_unit_type'][0])}"
    return "orgunits"


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


def _prepare_output_path(table_name: str) -> Path:
    """Create output directory structure and return full path."""  # noqa: DOC201
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_dir = Path(workspace.files_path, "iaso-pipelines", "extract-orgunits")
    output_dir.mkdir(exist_ok=True, parents=True)
    return output_dir / f"{table_name}_{timestamp}.gpkg"


def _get_or_create_dataset_version(dataset: Dataset, name: str) -> Dataset.Version:
    """Get existing or create new dataset version."""  # noqa: DOC201
    return next((v for v in dataset.versions if v.name == name), None) or dataset.create_version(
        name
    )


def _clean_temp_files(output_path: Path) -> None:
    """Clean up temporary output files."""
    try:
        if output_path.exists():
            output_path.unlink()
        output_path.parent.rmdir()
    except OSError as err:
        current_run.log_warning(f"Temp file cleanup failed: {err}")


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


if __name__ == "__main__":
    iaso_extract_orgunits()
