"""Template for newly generated pipelines."""

import uuid
from datetime import datetime
from pathlib import Path

import polars as pl
from iaso_client import (
    authenticate_iaso,
    fetch_form_meta,
    get_app_id,
    get_form_metadata,
    get_form_name,
    get_token_headers,
    validate_user_roles,
)
from iaso_io import read_submissions_file
from jinja2 import Template
from openhexa.sdk import (
    File,  # type: ignore
    IASOConnection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.sdk.pipelines.parameter import IASOWidget  # type: ignore
from openhexa.toolbox.iaso import IASO
from template import generate_xml_template
from validation import validate_data_structure, validate_field_constraints, validate_global_data

CAST_MAP = {
    "String": pl.Utf8,
    "Int64": pl.Int64,
    "Float64": pl.Float64,
    "Boolean": pl.Boolean,
}


@pipeline("iaso_import_submissions")
@parameter("iaso_connection", name="IASO connection", type=IASOConnection, required=True)  # type: ignore
@parameter(
    "project",
    name="Projects",
    type=int,
    widget=IASOWidget.IASO_PROJECTS,
    connection="iaso_connection",  # type: ignore
    required=True,
)
@parameter(
    "form_id",
    name="Form ID",
    type=int,
    widget=IASOWidget.IASO_FORMS,
    connection="iaso_connection",  # type: ignore
    required=True,
)
@parameter(
    "input_file",
    type=File,
    name="IASO form submission file",
    required=True,
)
@parameter(
    "import_strategy",
    type=str,  # type: ignore
    name="Import Strategy",
    help=(
        "Import Strategy: 'CREATE', 'UPDATE', 'CREATE_AND_UPDATE', 'DELETE' (default: 'CREATE')"
        "notice that UPDATE, CREATE_AND_UPDATE and DELETE modes require that the input "
        "file contains ID submissions columns"
    ),
    default="CREATE",
    choices=["CREATE", "UPDATE", "CREATE_AND_UPDATE", "DELETE"],
    required=False,
)
@parameter(
    "output_directory",
    type=str,  # type: ignore
    name="Output directory",
    help=(
        "Directory where the import summary will be saved."
        "(default if not specified: "
        "`iaso-pipelines/import-submissions/<form_name>`)"
    ),
    required=False,
)
@parameter(
    "strict_validation",
    type=bool,  # type: ignore
    name="Strict validation of form submissions and file structure.",
    default=False,
    help=(
        "If enabled, the pipeline will perform strict validation of the form submissions "
        "and file structure, raising errors for any discrepancies found."
    ),
    required=False,
)
def iaso_import_submissions(
    iaso_connection: IASOConnection,
    project: int,
    form_id: int,
    input_file: File,
    import_strategy: str,
    output_directory: str,
    strict_validation: bool,
):
    """Write your pipeline orchestration here."""
    current_run.log_info("Starting form submissions import pipeline")

    iaso = authenticate_iaso(iaso_connection)
    form_name = get_form_name(iaso, form_id)
    app_id = get_app_id(iaso, project)

    if not validate_user_roles(iaso, app_id):
        raise PermissionError("User does not have the required roles for this application.")

    # Import submissions file
    df_submissions = read_submissions_file(Path(workspace.files_path, input_file.path))

    # Get form metadata
    questions = get_form_metadata(iaso=iaso, form_id=form_id)
    choices = get_form_metadata(iaso=iaso, form_id=form_id, type_metadata="choices")

    validation_result = validate_data_structure(
        df_submissions,
        questions,
        import_strategy,
    )
    if strict_validation and not validation_result["is_valid"]:
        error_messages = "\n".join(validation_result["errors"])
        current_run.log_error(f"Data structure validation failed:\n{error_messages}")
        raise ValueError(f"Data structure validation failed:\n{error_messages}")

    for warning in validation_result["warnings"]:
        current_run.log_warning(f"Data structure validation warning:\n{warning}")

    for col_name, (expected, _actual) in validation_result["invalid_types"].items():
        current_run.log_info(f"Casting column '{col_name}' to expected type '{expected}'.")
        df_submissions = df_submissions.with_columns(
            pl.col(col_name).cast(CAST_MAP[expected]).alias(col_name)
        )

    current_run.log_info("Data structure validation passed")

    # process record by record to parse to endpoint
    push_submissions(
        iaso=iaso,
        df=df_submissions,
        questions=questions,
        choices=choices,
        form_name=form_name,
        form_id=form_id,
        app_id=app_id,
        import_strategy=import_strategy,
        output_directory=output_directory,
        strict_validation=strict_validation,
    )


def generate_templates_for_versions(
    iaso: IASO,
    df: pl.DataFrame,
    form_id: int,
    meta: dict,
    questions: pl.DataFrame,
    choices: pl.DataFrame,
) -> dict:
    """Prepare XML templates per form version present in the submissions dataframe.

    If the dataframe does not contain a `form_version` column, run global
    validation to compute any required summary columns before generating the
    template for the latest version.

    Returns:
        dict: mapping version -> xml_template (may contain 'latest_version').
    """
    dico_xml_template: dict = {}

    if "form_version" not in df.columns:
        # Create a template for the latest form version exposed by IASO
        latest_version = meta.get("latest_form_version") or {}
        if isinstance(latest_version, dict):
            latest_version_id = str(latest_version.get("version_id", ""))
        else:
            latest_version_id = ""

        # Run global validation to ensure summary columns exist
        df_for_template = validate_global_data(df=df, questions=questions, choices=choices)

        dico_xml_template["latest_version"] = generate_xml_template(
            df_submissions=df_for_template,
            questions=questions,
            id_form=str(meta.get("form_id") or ""),
            form_version=latest_version_id,
        )
    else:
        for version in df["form_version"].unique().to_list():
            questions_for_version = get_form_metadata(
                iaso=iaso, form_id=form_id, form_version=version
            )
            dico_xml_template[version] = generate_xml_template(
                df_submissions=df,
                questions=questions_for_version,
                id_form=str(meta.get("form_id") or ""),
                form_version=version,
            )

    return dico_xml_template


def handle_delete_mode(iaso: IASO, df: pl.DataFrame, headers: dict) -> dict[str, int]:
    """Handle deletion of instances specified in the dataframe's 'id' column.

    Returns:
        dict[str, int]: summary counts for deleted/ignored.
    """
    summary = {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0}

    if "id" not in df.columns:
        msg = "DELETE mode requires an 'id' column with IASO Instance IDs"
        current_run.log_error(msg)
        raise RuntimeError(msg)

    for record in df.iter_rows(named=True):
        try:
            record_id = record.get("id")
            if record_id is None:
                current_run.log_error("Skipping record with missing 'id' column value")
                summary["ignored"] += 1
                continue

            try:
                instance_id = int(record_id)
            except (ValueError, TypeError):
                current_run.log_error(f"Invalid instance id for record: {record_id}")
                summary["ignored"] += 1
                continue

            inst_res = iaso.api_client.delete(f"/api/instances/{instance_id}", headers=headers)

            if inst_res.status_code in (200, 201, 204):
                summary["deleted"] += 1
            else:
                err = getattr(inst_res, "text", None)
                msg = (
                    f"Failed to delete instance (id={instance_id}, "
                    f"status={inst_res.status_code}, resp={err})"
                )
                current_run.log_error(msg)
                summary["ignored"] += 1

        except Exception as exc:
            current_run.log_error(f"Error processing record (id={record.get('id')}): {exc}")
            summary["ignored"] += 1

    return summary


def handle_create_mode(
    iaso: IASO,
    df: pl.DataFrame,
    questions: pl.DataFrame,
    choices: pl.DataFrame,
    form_name: str,
    form_id: int,
    app_id: str,
    strict_validation: bool,
    output_directory: str | None,
    dico_xml_template: dict,
) -> dict[str, int]:
    """Handle creation/import of new instances from the dataframe.

    Returns:
        dict[str, int]: summary counts for imported/ignored/updated.
    """
    summary = {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0}

    constraints_present = "constraints_validation_summary" in df.columns
    choices_present = "choices_validation_summary" in df.columns

    default_output = f"iaso-pipelines/import-submissions/{form_name}"
    output_dir = Path(workspace.files_path) / (output_directory or default_output)
    output_dir.mkdir(exist_ok=True, parents=True)

    for record in df.iter_rows(named=True):
        try:
            # Choose template and determine validity
            if "latest_version" in dico_xml_template:
                if constraints_present and choices_present:
                    is_valid = bool(record.get("constraints_validation_summary")) and bool(
                        record.get("choices_validation_summary")
                    )
                elif constraints_present:
                    is_valid = bool(record.get("constraints_validation_summary"))
                elif choices_present:
                    is_valid = bool(record.get("choices_validation_summary"))
                else:
                    is_valid = True

                xml_template = dico_xml_template["latest_version"]
            else:
                is_valid = validate_field_constraints(record, questions, choices)
                xml_template = dico_xml_template.get(record.get("form_version"))

            is_valid = is_valid or not strict_validation
            if not is_valid:
                summary["ignored"] += 1
                continue

            the_uuid = str(uuid.uuid4())
            file_path = output_dir / f"{the_uuid}.xml"

            if not xml_template:
                current_run.log_error(
                    "No XML template available for record "
                    f"form_version={record.get('form_version')}, skipping"
                )
                summary["ignored"] += 1
                continue
            data = {**record, **{"uuid": the_uuid}}
            xml_data = Template(xml_template).render(
                **{k: v if v is not None else "" for k, v in data.items()}
            )
            with file_path.open("w", encoding="utf-8") as f:
                f.write(xml_data)

            instance_body = [
                {
                    "id": the_uuid,
                    "orgUnitId": int(record.get("org_unit_id")),  # type: ignore
                    "created_at": int(datetime.now().timestamp()),
                    "formId": form_id,
                    "accuracy": 0,
                    "altitude": 0,
                    "latitude": None,
                    "longitude": None,
                    "file": file_path.as_posix(),
                    "name": file_path.name,
                    "period": datetime.now().year,
                }
            ]

            headers = get_token_headers(iaso)
            inst_res = iaso.api_client.post(
                "/api/instances",
                json=instance_body,
                headers=headers,
                params={"app_id": str(app_id)},
            )
            if inst_res.status_code not in (200, 201):
                current_run.log_error(
                    "Failed to create instance for record "
                    f"(org_unit_id={record.get('org_unit_id')}), "
                    f"status={inst_res.status_code}, resp={getattr(inst_res, 'text', None)}"
                )
                summary["ignored"] += 1
                continue

            with file_path.open("rb") as fp:
                upload_res = iaso.api_client.post(
                    "/sync/form_upload/", files={"xml_submission_file": fp}, headers=headers
                )

            if upload_res.status_code == 201:
                summary["imported"] += 1
            else:
                current_run.log_error(
                    "Upload failed for "
                    f"{file_path.name}: status={upload_res.status_code} "
                    f"resp={getattr(upload_res, 'text', None)}"
                )
                summary["ignored"] += 1

        except Exception as exc:
            current_run.log_error(f"Error processing record {record.get('org_unit_id')}: {exc}")
            summary["ignored"] += 1
            continue

    return summary


def push_submissions(
    iaso: IASO,
    df: pl.DataFrame,
    questions: pl.DataFrame,
    choices: pl.DataFrame,
    form_name: str,
    form_id: int,
    app_id: str,
    import_strategy: str,
    output_directory: str | None,
    strict_validation: bool,
) -> dict[str, int]:
    """Orchestrate pushing submissions to IASO by delegating to per-mode handlers.

    Currently supports CREATE and DELETE.

    Returns:
        dict[str, int]: summary counts for imported/updated/ignored/deleted.
    """
    current_run.log_info(f"Pushing {len(df)} submissions to IASO for app ID {app_id} start")

    mode = (import_strategy or "CREATE").upper()
    if mode not in ("CREATE", "DELETE"):
        msg = (
            f"Import mode '{import_strategy}' is not implemented by this pipeline yet. "
            "Only 'CREATE', 'DELETE' is supported at the moment."
        )
        current_run.log_warning(msg)
        raise NotImplementedError(msg)

    headers = get_token_headers(iaso)
    meta = fetch_form_meta(iaso, form_id)
    dico_xml_template = generate_templates_for_versions(iaso, df, form_id, meta, questions, choices)

    if mode == "DELETE":
        summary = handle_delete_mode(iaso=iaso, df=df, headers=headers)
    else:
        summary = handle_create_mode(
            iaso=iaso,
            df=df,
            questions=questions,
            choices=choices,
            form_name=form_name,
            form_id=form_id,
            app_id=app_id,
            strict_validation=strict_validation,
            output_directory=output_directory,
            dico_xml_template=dico_xml_template,
        )

    current_run.log_info(f"Push finished. Summary: {summary}")
    return summary


if __name__ == "__main__":
    iaso_import_submissions()
