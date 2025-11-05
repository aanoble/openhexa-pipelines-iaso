import polars as pl


def generate_xml_template(
    df_submissions: pl.DataFrame,
    questions: pl.DataFrame,
    id_form: str,
    form_version: str,
) -> str:
    """Generate an advanced XML template for form submissions.

    Args:
        df_submissions (pl.DataFrame): DataFrame containing the submissions data.
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

    included_fields = [
        col for col in df_submissions.columns if col in questions["name"].unique().to_list()
    ]

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