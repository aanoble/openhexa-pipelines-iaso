import hashlib
import json
import re
import unicodedata
from pathlib import Path

from openhexa.sdk.datasets.dataset import DatasetVersion
from shapely.geometry import MultiPolygon, Point, Polygon

# Precompile regex pattern for string cleaning
CLEAN_PATTERN = re.compile(r"[^\w\s-]")


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


def get_driver(output_format: str) -> str:
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
