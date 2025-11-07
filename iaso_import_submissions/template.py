import xml.etree.ElementTree as ET

import polars as pl
from utils import local_name_xml_tag


def generate_xml_template(
    df: pl.DataFrame,
    questions: pl.DataFrame,
    id_form: str,
    form_version: str,
) -> str:
    """Generate an advanced XML template for form submissions.

    Args:
        df (pl.DataFrame): DataFrame containing the submissions data.
        questions (pl.DataFrame): DataFrame containing the form questions metadata.
        id_form (str): The ID form on IASO.
        form_version (str): The form version.

    Returns:
        str: The generated XML template as a string.
    """
    begin_group = (
        questions.filter(pl.col("type") == "begin group")["name"][0]
        if not questions.filter(pl.col("type") == "begin group").is_empty()
        else None
    )

    template_parts = [
        '<data xmlns:jr="http://openrosa.org/javarosa" xmlns:orx="http://openrosa.org/xforms"',
        f'      id="{id_form}" version="{form_version}">',
        f"    <{begin_group}>" if begin_group else "",
    ]

    included_fields = [col for col in df.columns if col in questions["name"].unique().to_list()]

    for field in included_fields:
        template_parts.append(f"        <{field}>{{{{ {field} }}}}</{field}>")

    template_parts.extend(
        [
            f"    </{begin_group}>" if begin_group else "",
            "    <meta>",
            "        <instanceID>uuid:{{uuid}}</instanceID>",
            "    </meta>",
            "</data>",
        ]
    )

    return "\n".join(template_parts)


def inject_iaso_and_edituser_from_str(
    xml_str: str,
    iaso_numeric_id: int | None = None,
    edit_user_id: int | None = None,
) -> bytes:
    """Inject or update IASO instance ID and edit user ID in XML string.

    Args:
        xml_str (str): The XML string to modify.
        iaso_numeric_id (int | None, optional): IASO instance ID to inject. Defaults to None.
        edit_user_id (int | None, optional): Edit user ID to inject. Defaults to None.

    Returns:
        bytes: Modified XML string with updated attributes.
    """
    root = ET.fromstring(xml_str)

    if iaso_numeric_id is not None:
        root.set("iasoInstance", str(iaso_numeric_id))

    meta = None
    for elem in root.iter():
        if local_name_xml_tag(elem.tag).lower() == "meta":
            meta = elem
            break

    if meta is None and edit_user_id is not None:
        meta = ET.SubElement(root, "meta")

    if edit_user_id is not None and meta is not None:
        found = None
        for ch in list(meta):
            if local_name_xml_tag(ch.tag).lower() == "edituserid":
                found = ch
                break
        if found is None:
            found = ET.SubElement(meta, "editUserID")
        found.text = str(edit_user_id)

    out = ET.tostring(root, encoding="utf-8")

    if not out.strip().startswith("<?xml"):
        out = '<?xml version="1.0" encoding="utf-8"?>\n' + out
    return out
