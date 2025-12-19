import base64
import json
from functools import lru_cache
from io import BytesIO

import pandas as pd
import polars as pl
import requests
from openhexa.sdk import IASOConnection, current_run
from openhexa.toolbox.iaso import IASO
from utils import clean_string


def authenticate_iaso(conn: IASOConnection) -> IASO:
    """Authenticates and returns an IASO object.

    Args:
        conn (IASOConnection): IASO connection details.

    Returns:
        IASO: An authenticated IASO object.
    """
    iaso = IASO(conn.url, conn.username, conn.password)
    current_run.log_info("IASO authentication successful")
    return iaso


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
    except requests.RequestException as e:
        current_run.log_error(f"Form fetch failed: {e}")
        raise


def get_app_id(iaso: IASO, project_id: int) -> str:
    """Retrieve the application ID for a given project.

    Args:
        iaso (IASO): An authenticated IASO object.
        project_id (int): The ID of the project.

    Returns:
        str: Application ID.

    Raises:
        ValueError: If the project does not exist.
    """
    try:
        resp = iaso.api_client.get(
            f"/api/projects/{project_id}",
            params={"fields": {"app_id"}},
        )
        data = resp.json()
    except requests.RequestException as e:
        current_run.log_error(f"Project fetch failed (network): {e}")
        raise
    except json.JSONDecodeError as e:
        current_run.log_error(f"Invalid JSON in project response: {e}")
        raise

    app_id = data.get("app_id")
    if not app_id:
        current_run.log_error(f"Project {project_id} has no app_id in response: {data}")
        raise ValueError(f"Project {project_id} has no app_id")

    return app_id


@lru_cache(maxsize=10)
def get_form_metadata(
    iaso: IASO, form_id: int, form_version: str | None = None, type_metadata: str = "questions"
) -> pl.DataFrame:
    """Retrieve metadata for a given form.

    Args:
        iaso (IASO): An authenticated IASO object.
        form_id (int): The ID of the form.
        form_version (str): form version id
        type_metadata (str): Type of metadata to retrieve ('questions' or 'choices').

    Returns:
        pl.DataFrame: DataFrame containing form metadata.
    """
    try:
        if form_version:
            params = {
                "form_id": str(form_id),
                "version_id": str(form_version),
                "fields": "xls_file",
            }
            res = iaso.api_client.get("/api/formversions/", params=params)
            form_versions = res.json().get("form_versions", [])
            xls_url = next((fv.get("xls_file") for fv in form_versions if fv.get("xls_file")), "")
        else:
            res = iaso.api_client.get(
                f"/api/forms/{form_id}", params={"fields": "latest_form_version"}
            )
            xls_url = res.json().get("latest_form_version", {}).get("xls_file", "")
    except requests.RequestException as e:
        current_run.log_error(f"Form metadata fetch failed (network): {e}")
        raise
    except json.JSONDecodeError as e:
        current_run.log_error(f"Invalid JSON in form metadata response: {e}")
        raise

    if not xls_url:
        return pl.DataFrame()

    if type_metadata not in ("questions", "choices"):
        raise ValueError("type_metadata must be 'questions' or 'choices'")

    try:
        resp = requests.get(xls_url, timeout=30)
        resp.raise_for_status()
        bio = BytesIO(resp.content)
        df_pd = (
            pd.read_excel(bio, dtype=str)
            if type_metadata == "questions"
            else pd.read_excel(bio, sheet_name="choices", dtype=str)
        )
        df_pd = df_pd.dropna(how="all")
        df_pl = pl.from_pandas(df_pd)

        str_cols = [name for name, dtype in df_pl.schema.items() if dtype == pl.Utf8]
        if str_cols:
            df_pl = df_pl.with_columns(
                [pl.col(c).str.replace_all(r"(^\s+)|(\s+$)", "") for c in str_cols]
            )
        return df_pl
    except requests.RequestException as ex:
        current_run.log_error(f"Failed to download xls from {xls_url}: {ex}")
        raise
    except Exception as ex:
        current_run.log_error(f"Failed to process xls file from {xls_url}: {ex}")
        raise


def validate_user_roles(iaso: IASO, app_id: str) -> bool:
    """Check if the user has the required role for the given app_id.

    Args:
        iaso (IASO): An authenticated IASO object.
        app_id (str): The application ID to check against.

    Returns:
        bool: True if the user has the required role for the given app_id, False otherwise.
    """
    try:
        resp = iaso.api_client.get("/api/profiles/me/")
        res = resp.json()
    except requests.RequestException as e:
        current_run.log_error(f"Failed to fetch profile from IASO API: {e}")
        raise

    # Normalize permissions: they can be a list or a dict-like structure.
    def _to_perm_set(obj: object) -> set[str]:
        if not obj:
            return set()
        if isinstance(obj, dict):
            # use keys if dict maps permission->truthy
            return {str(k) for k in obj}
        if isinstance(obj, (list, tuple, set)):
            return {str(x) for x in obj}
        # fallback: coerce to string
        return {str(obj)}

    perms = _to_perm_set(res.get("permissions")) | _to_perm_set(res.get("user_permissions"))
    has_form_permissions = "iaso_update_submission" in perms

    user_account = res.get("account")
    has_account = False
    if isinstance(user_account, dict):
        has_account = user_account.get("name") == app_id
    elif isinstance(user_account, (list, tuple)):
        try:
            has_account = any(
                (acc or {}).get("name") == app_id for acc in user_account if isinstance(acc, dict)
            )
        except Exception:
            has_account = False
    else:
        has_account = False

    result = bool(has_form_permissions and has_account)
    if not result:
        current_run.log_info(
            "User lacks required role or account mismatch "
            f"(app_id={app_id}): permissions={list(perms)}, "
            f"account={user_account}"
        )
    return result


def get_token_headers(iaso: IASO) -> dict[str, str]:
    """Obtain an IASO bearer token and return Authorization headers.

    Raises RuntimeError on failure.

    Returns:
        dict[str, str]: Authorization header mapping.
    """
    try:
        token_res = iaso.api_client.post(
            "/api/token/",
            json={"username": iaso.api_client.username, "password": iaso.api_client.password},
        )
        token_res.raise_for_status()
        token = token_res.json().get("access")
    except requests.RequestException as exc:
        msg = f"Failed to obtain access token (network): {exc} - response: {token_res.text}"
        current_run.log_error(msg)
        raise
    except json.JSONDecodeError as exc:
        msg = f"Failed to parse token response JSON: {exc} - response: {token_res.text}"
        current_run.log_error(msg)
        raise

    if not token:
        current_run.log_error("Token response did not contain access token")
        raise ValueError("IASO token missing in /api/token/ response")

    return {"Authorization": f"Bearer {token}"}


def fetch_form_meta(iaso: IASO, form_id: int) -> dict:
    """Fetch form metadata from IASO and return the parsed JSON.

    Raises on any network or parsing error.

    Returns:
        dict: Parsed form metadata JSON.
    """
    try:
        meta_res = iaso.api_client.get(
            f"/api/forms/{form_id}",
            params={"fields": "form_id,org_unit_type_ids,latest_form_version"},
        )
        meta_res.raise_for_status()
        return meta_res.json()
    except requests.RequestException as exc:
        current_run.log_error(f"Failed to fetch form metadata for form {form_id}: {exc}")
        raise
    except json.JSONDecodeError as exc:
        current_run.log_error(f"Failed to parse form metadata JSON for form {form_id}: {exc}")
        raise


def get_user_id_from_jwt(token: str) -> str:
    """Extract user ID from a JWT token.

    Args:
        token (str): JWT token string.

    Returns:
        str: User ID extracted from the token, or empty string if not found.
    """
    payload_b64 = token.split(".")[1]
    payload_b64 += "=" * (-len(payload_b64) % 4)
    payload = base64.urlsafe_b64decode(payload_b64)
    payload = json.loads(payload)
    return payload.get("user_id", "") or payload.get("id", "") or payload.get("sub", "")
