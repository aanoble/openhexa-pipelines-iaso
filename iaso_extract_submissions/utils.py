import hashlib
import re
import unicodedata
from pathlib import Path

from openhexa.sdk.datasets.dataset import DatasetVersion

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
