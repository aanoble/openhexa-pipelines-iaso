"""Template for newly generated pipelines."""

import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import polars as pl
import requests
from iaso_client import (
    authenticate_iaso,
    fetch_form_meta,
    get_app_id,
    get_form_metadata,
    get_form_name,
    get_token_headers,
    get_user_id_from_jwt,
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
from template import enrich_submission_xml, generate_xml_template
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
    type=File,  # type: ignore
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
    questions_by_version: dict[str, pl.DataFrame] | None = None,
) -> dict:
    """Generate XML templates keyed by form version.

    If no `form_version` column exists, only a `latest_version` template is created
    using the latest version id from metadata. Otherwise, each distinct version
    in the dataframe gets its own template generated with version-specific questions.

    Returns:
        dict: {version_or_latest: xml_template_string}
    """
    templates: dict = {}

    if "form_version" not in df.columns:
        latest_version = meta.get("latest_form_version") or {}
        latest_version_id = (
            str(latest_version.get("version_id", "")) if isinstance(latest_version, dict) else ""
        )
        templates["latest_version"] = generate_xml_template(
            df=df,
            questions=questions,
            id_form=str(meta.get("form_id") or ""),
            form_version=latest_version_id,
        )
    else:
        for version in df["form_version"].drop_nulls().unique().to_list():
            version_str = str(version)
            questions_for_version = (
                questions_by_version.get(version_str)
                if questions_by_version
                else get_form_metadata(iaso=iaso, form_id=form_id, form_version=version_str)
            )
            if questions_for_version is None:
                questions_for_version = get_form_metadata(
                    iaso=iaso, form_id=form_id, form_version=version_str
                )
            templates[version_str] = generate_xml_template(
                df=df,
                questions=questions_for_version,
                id_form=str(meta.get("form_id") or ""),
                form_version=version_str,
            )

    return templates


def _select_template_and_is_valid(
    record: dict,
    df: pl.DataFrame,
    strict_validation: bool,
    questions: pl.DataFrame,
    choices: pl.DataFrame,
    templates: dict,
    questions_by_version: dict[str, pl.DataFrame] | None = None,
    choices_by_version: dict[str, pl.DataFrame] | None = None,
) -> tuple[bool, str | None]:
    """Return (is_valid, xml_template) for a record.

    - If a 'latest_version' template exists, prefer it and use global summary columns
      when present to determine validity.
    - Otherwise, validate the record against field constraints and select the
      template matching the record's form_version.

    If strict_validation is False, records are considered valid regardless of
    validation results.

    Returns:
        tuple[bool, str | None]:
            - is_valid: whether the record passes validation (or strict_validation is disabled)
            - xml_template: the XML template string to use for rendering this record, or None
    """
    if "latest_version" in templates:
        constraints_present = "constraints_validation_summary" in df.columns
        choices_present = "choices_validation_summary" in df.columns

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

        xml_template = templates["latest_version"]
    else:
        version = str(record.get("form_version") or "")
        questions_for_validation = (
            questions_by_version.get(version, questions) if questions_by_version else questions
        )
        choices_for_validation = (
            choices_by_version.get(version, choices) if choices_by_version else choices
        )
        is_valid = validate_field_constraints(
            record, questions_for_validation, choices_for_validation
        )
        xml_template = templates.get(record.get("form_version")) or templates.get(version)

    # If strict validation is disabled, accept the record regardless
    if not strict_validation:
        is_valid = True

    return is_valid, xml_template


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

    current_run.log_info(f"Deleted submissions successfully. Summary: {summary}")
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
    templates: dict,
    questions_by_version: dict[str, pl.DataFrame] | None = None,
    choices_by_version: dict[str, pl.DataFrame] | None = None,
) -> dict[str, int]:
    """Handle creation/import of new instances from the dataframe.

    Returns:
        dict[str, int]: summary counts for imported/ignored/updated.
    """
    summary = {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0}

    default_output = f"iaso-pipelines/import-submissions/{form_name}/creates"
    output_dir = Path(workspace.files_path) / (output_directory or default_output)
    output_dir.mkdir(exist_ok=True, parents=True)

    headers = get_token_headers(iaso)

    for record in df.iter_rows(named=True):
        try:
            is_valid, xml_template = _select_template_and_is_valid(
                record=record,
                df=df,
                strict_validation=strict_validation,
                questions=questions,
                choices=choices,
                templates=templates,
                questions_by_version=questions_by_version,
                choices_by_version=choices_by_version,
            )
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

            instance_body = {
                "id": the_uuid,
                "orgUnitId": int(record.get("org_unit_id")),  # type: ignore
                "created_at": int(datetime.now().timestamp()),
                "formId": form_id,
                "accuracy": float(record.get("accuracy") or 0) if "accuracy" in record else 0,
                "altitude": float(record.get("altitude") or 0) if "altitude" in record else 0,
                "latitude": float(record.get("latitude") or 0) if "latitude" in record else None,
                "longitude": float(record.get("longitude") or 0) if "longitude" in record else None,
                "file": file_path.as_posix(),
                "name": file_path.name,
            }

            inst_res = iaso.api_client.post(
                "/api/instances",
                json=[instance_body],
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


def handle_update_mode(
    iaso: IASO,
    df: pl.DataFrame,
    questions: pl.DataFrame,
    choices: pl.DataFrame,
    form_name: str,
    form_id: int,
    strict_validation: bool,
    output_directory: str | None,
    templates: dict,
    questions_by_version: dict[str, pl.DataFrame] | None = None,
    choices_by_version: dict[str, pl.DataFrame] | None = None,
) -> dict[str, int]:
    """Handle update of existing instances from the dataframe.

    Returns:
        dict[str, int]: summary counts for updated/ignored/imported.
    """
    summary = {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0}
    default_output = f"iaso-pipelines/import-submissions/{form_name}/updates"
    output_dir = Path(workspace.files_path) / (output_directory or default_output)
    output_dir.mkdir(exist_ok=True, parents=True)

    if "id" not in df.columns:
        msg = "UPDATE mode requires an 'id' column with IASO Instance IDs"
        current_run.log_error(msg)
        raise RuntimeError(msg)

    if "instanceID" not in df.columns:
        msg = "UPDATE mode requires an 'instanceID' column with IASO Instance UUIDs"
        current_run.log_warning(msg)
        raise RuntimeError(msg)

    headers = get_token_headers(iaso)
    token = headers.get("Authorization", "").removeprefix("Bearer ")
    user_id = get_user_id_from_jwt(token)
    for record in df.iter_rows(named=True):
        try:
            is_valid, xml_template = _select_template_and_is_valid(
                record=record,
                df=df,
                strict_validation=strict_validation,
                questions=questions,
                choices=choices,
                templates=templates,
                questions_by_version=questions_by_version,
                choices_by_version=choices_by_version,
            )
            if not is_valid:
                summary["ignored"] += 1
                continue

            instance_uuid_raw = record.get("instanceID")
            instance_uuid = (
                instance_uuid_raw.removeprefix("uuid:")
                if isinstance(instance_uuid_raw, str)
                else None
            )

            if instance_uuid is None:
                current_run.log_error("Skipping record with missing 'instanceID' column value")
                summary["ignored"] += 1
                continue

            if record.get("org_unit_id"):
                # Handle the case where org_unit_id is present
                payload = {
                    "org_unit": str(record.get("org_unit_id")),
                    "formID": int(form_id),
                    "accuracy": float(record.get("accuracy") or 0) if "accuracy" in record else 0,
                    "altitude": float(record.get("altitude") or 0) if "altitude" in record else 0,
                    "latitude": float(record.get("latitude") or 0)
                    if "latitude" in record
                    else None,
                    "longitude": float(record.get("longitude") or 0)
                    if "longitude" in record
                    else None,
                }

                iaso.api_client.patch(
                    f"/api/instances/{record.get('id')}",
                    json=payload,
                    headers=headers,
                )

            if not xml_template:
                current_run.log_error(
                    "No XML template available for record "
                    f"form_version={record.get('form_version')}, skipping"
                )
                summary["ignored"] += 1
                continue

            # Get iaso instance from xml instances
            res = iaso.api_client.get(
                f"/api/instances/{record.get('id')}/",
                headers=headers,
                params={"fields": "is_locked"},
            )
            res_json = res.json()

            is_locked = res_json.get("is_locked", False)
            if is_locked:
                current_run.log_warning(
                    f"Instance id={record.get('id')} is locked, skipping update."
                )
                summary["ignored"] += 1
                continue

            the_uuid = str(instance_uuid)
            file_path = output_dir / f"update_{the_uuid}.xml"
            data = {**record, **{"uuid": the_uuid}}
            xml_data = Template(xml_template).render(
                **{k: v if v is not None else "" for k, v in data.items()}
            )
            current_run.log_debug(xml_data)
            xml_data = enrich_submission_xml(
                xml_str=xml_data,
                iaso_instance=int(record.get("id")),  # type: ignore
                edit_user_id=int(user_id) if user_id else None,
            )
            current_run.log_debug(xml_data.decode("utf-8"))

            with file_path.open("wb") as f:
                f.write(xml_data)

            edit_url_res = iaso.api_client.get(
                f"api/enketo/edit/{instance_uuid}/",
                headers=headers,
            )

            edit_url = urlparse(edit_url_res.json().get("edit_url", ""))

            with file_path.open("rb") as fp:
                files = {"xml_submission_file": (file_path.name, fp, "application/xml")}
                upload_res = requests.post(
                    f"{edit_url.scheme}://{edit_url.netloc}/submission/{edit_url.path.split('/')[-1]}",
                    headers=headers,
                    files=files,
                )
            if upload_res.status_code in (200, 201):
                summary["updated"] += 1
            else:
                current_run.log_error(
                    "Update failed for id"
                    f"{record.get('id', '')}: status={upload_res.status_code} "
                    f"resp={getattr(upload_res, 'text', None)}"
                )
                summary["ignored"] += 1
        except Exception as exc:
            current_run.log_error(f"Error processing record {record.get('org_unit_id', '')}: {exc}")
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
    headers = get_token_headers(iaso)
    meta = fetch_form_meta(iaso, form_id)

    if import_strategy == "DELETE":
        current_run.log_info(
            f"Starting deletion of {len(df)} submissions in IASO for app ID {app_id}."
        )
        return handle_delete_mode(iaso=iaso, df=df, headers=headers)

    if "form_version" not in df.columns:
        # Run global validation to ensure summary columns exist
        df = validate_global_data(df=df, questions=questions, choices=choices)

    questions_by_version: dict[str, pl.DataFrame] = {}
    choices_by_version: dict[str, pl.DataFrame] = {}
    if "form_version" in df.columns:
        for version in df["form_version"].drop_nulls().unique().to_list():
            version_str = str(version)
            questions_by_version[version_str] = get_form_metadata(
                iaso=iaso, form_id=form_id, form_version=version_str
            )
            choices_by_version[version_str] = get_form_metadata(
                iaso=iaso,
                form_id=form_id,
                form_version=version_str,
                type_metadata="choices",
            )

    if import_strategy == "CREATE":
        templates = generate_templates_for_versions(
            iaso,
            df,
            form_id,
            meta,
            questions,
            questions_by_version=questions_by_version,
        )
        current_run.log_info(f"Pushing {len(df)} submissions to IASO for app ID {app_id} start")
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
            templates=templates,
            questions_by_version=questions_by_version,
            choices_by_version=choices_by_version,
        )
        current_run.log_info(f"Push finished. Summary: {summary}")

    if import_strategy == "UPDATE":
        templates = generate_templates_for_versions(
            iaso,
            df,
            form_id,
            meta,
            questions,
            questions_by_version=questions_by_version,
        )
        current_run.log_info(f"Updating {len(df)} submissions in IASO for app ID {app_id} start")
        summary = handle_update_mode(
            iaso=iaso,
            df=df,
            questions=questions,
            choices=choices,
            form_name=form_name,
            form_id=form_id,
            strict_validation=strict_validation,
            output_directory=output_directory,
            templates=templates,
            questions_by_version=questions_by_version,
            choices_by_version=choices_by_version,
        )
        current_run.log_info(f"Update finished. Summary: {summary}")

    if import_strategy == "CREATE_AND_UPDATE":
        df_create = df.filter(pl.col("id").is_null() & pl.col("org_unit_id").is_not_null())
        df_update = df.filter(pl.col("id").is_not_null())

        summary_create = handle_create_mode(
            iaso=iaso,
            df=df_create,
            questions=questions,
            choices=choices,
            form_name=form_name,
            form_id=form_id,
            app_id=app_id,
            strict_validation=strict_validation,
            output_directory=output_directory,
            templates=generate_templates_for_versions(
                iaso,
                df_create,
                form_id,
                meta,
                questions,
                questions_by_version=questions_by_version,
            ),
            questions_by_version=questions_by_version,
            choices_by_version=choices_by_version,
        )
        summary_update = handle_update_mode(
            iaso=iaso,
            df=df_update,
            questions=questions,
            choices=choices,
            form_name=form_name,
            form_id=form_id,
            strict_validation=strict_validation,
            output_directory=output_directory,
            templates=generate_templates_for_versions(
                iaso,
                df_update,
                form_id,
                meta,
                questions,
                questions_by_version=questions_by_version,
            ),
            questions_by_version=questions_by_version,
            choices_by_version=choices_by_version,
        )
        summary = {
            "imported": summary_create["imported"],
            "updated": summary_update["updated"],
            "ignored": summary_create["ignored"] + summary_update["ignored"],
            "deleted": 0,
        }

        current_run.log_info(f"Create and Update finished. Summary: {summary}")

    return summary


if __name__ == "__main__":
    iaso_import_submissions()
