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


def enrich_submission_xml(
    xml_str: str,
    iaso_instance: int | None = None,
    edit_user_id: int | None = None,
) -> bytes:
    """Enrich an IASO submission XML string with instance/user metadata.

    Args:
    xml_str (str): Original XML template content.
    iaso_instance (int | None): IASO instance numeric ID to embed (added as attribute).
    edit_user_id (int | None): User ID performing edit; added/updated under <meta><editUserID>.

    Returns:
    bytes: Updated XML including namespace safeguards and optional metadata elements.
    """
    root = ET.fromstring(bytes(xml_str, encoding="utf-8"))

    if iaso_instance is not None:
        root.set("iasoInstance", str(iaso_instance))

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

    # ElementTree may drop unused namespace declarations on serialization.
    # Ensure the standard OpenRosa namespaces are present on the <data> root.
    missing = []
    if b"xmlns:jr=" not in out:
        missing.append(b'xmlns:jr="http://openrosa.org/javarosa"')
    if b"xmlns:orx=" not in out:
        missing.append(b'xmlns:orx="http://openrosa.org/xforms"')

    if missing:
        insertion = b" " + b" ".join(missing)
        out = out.replace(b"<data", b"<data" + insertion, 1)

    # Prepend XML declaration if it's missing
    if not out.lstrip().startswith(b"<?xml"):
        out = b'<?xml version="1.0" encoding="utf-8"?>\n' + out

    return out
