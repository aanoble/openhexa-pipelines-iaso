import warnings

warnings.filterwarnings("ignore")

import re
import unicodedata
from io import BytesIO

import geopandas as gpd
import polars as pl
from openhexa.sdk import workspace
from openhexa.toolbox.iaso import IASO


def clean_data(data):
    """Clean data DataFrame OrgUnits"""
    data = unicodedata.normalize("NFD", data)
    data = "".join(c for c in data if not unicodedata.combining(c))

    data = re.sub(r"[^\w\s-]", "", data).rstrip().lstrip()
    return data.replace(" ", "_").lower()


def get_org_unit_ids(iaso: IASO):
    """Extract IASO Org Unit Type Id"""

    response = iaso.api_client.get("/api/orgunittypes")

    if "orgUnitTypes" not in response.json():
        return pl.DataFrame()

    return pl.DataFrame(response.json().get("orgUnitTypes"))


def get_org_unit_geo_data(df_ou_type: pl.DataFrame, iaso: IASO, ou_id: int) -> tuple:
    """Extract Org Unit Geo Data form Id"""

    df_ou = df_ou_type.filter(pl.col("id") == ou_id).select(["short_name", "projects"])
    ou_short_name = clean_data(df_ou["short_name"][0])
    project_name = clean_data(df_ou["projects"][0][0].get("name").strip("project"))

    try:
        response = iaso.api_client.get(
            "/api/orgunits", params={"gpkg": "true", "orgUnitTypeId": ou_id}
        )
        gdf = gpd.read_file(BytesIO(response.content))
        return gdf, ou_short_name, project_name
    except Exception:
        return gpd.GeoDataFrame(), None, None
