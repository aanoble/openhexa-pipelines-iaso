import re

import polars as pl
from openhexa.sdk import current_run
from pydantic import BaseModel  # type: ignore


class ValidationResult(BaseModel):
    """Represents the result of validating submission data.

    Attributes:
        is_valid (bool): Indicates if the data is valid.
        errors (list[str]): List of error messages.
        warnings (list[str]): List of warning messages.
        missing_columns (list[str]): List of missing columns.
        invalid_types (dict[str, tuple[str, str]]): Mapping of columns to expected and actual types.
        required_columns_present (set[str]): Set of required columns that are present.
    """

    is_valid: bool
    errors: list[str] = []
    warnings: list[str] = []
    missing_columns: list[str] = []
    invalid_types: dict[str, tuple[str, str]] = {}
    required_columns_present: set[str] = set()


def validate_data_structure(
    df: pl.DataFrame,
    questions: pl.DataFrame,
    import_strategy: str,
) -> dict:
    """Validate the structure of the submissions data.

    Args:
        df (pl.DataFrame): DataFrame containing the submissions data.
        questions (pl.DataFrame): DataFrame containing the form questions metadata.
        import_strategy (str): The import strategy being used.

    Returns:
        bool: True if the data structure is valid, False otherwise.
    """
    result = ValidationResult(is_valid=True)

    # Définition des exigences par stratégie
    strategy_requirements = {
        "CREATE": {
            "required": {"org_unit_id"},
            "optional": {"created_at", "form_version", "latitude", "longitude"},
            "types": {
                "org_unit_id": pl.Int64,
                "created_at": (pl.Date, pl.Datetime, pl.Utf8),
                "form_version": pl.Utf8,
            },
        },
        "UPDATE": {
            "required": {"id", "org_unit_id"},
            "optional": {"created_at", "form_version"},
            "types": {
                "id": pl.Utf8,
                "org_unit_id": pl.Int64,
                "created_at": (pl.Date, pl.Datetime, pl.Utf8),
            },
        },
        "CREATE_UPDATE": {
            "required": {"org_unit_id", "created_at"},
            "optional": {"id", "form_version"},
            "types": {
                "org_unit_id": pl.Int64,
                "created_at": (pl.Date, pl.Datetime, pl.Utf8),
                "id": pl.Utf8,
            },
        },
        "DELETE": {
            "required": {"id"},
            "optional": set(),
            "types": {"id": pl.Utf8},
        },
    }
    # We start by the latest form metadata version
    requirements = strategy_requirements[import_strategy]
    current_columns = set(df.columns)

    # 1. Check for required columns
    required_columns = requirements["required"] | set(
        questions.filter(pl.col("required") == "yes")["name"].unique()
    )
    missing_required = required_columns - current_columns

    if missing_required:
        # Généralement ce sont les informations sur les hint qui ne figure pas dans le fichier d'importation  # noqa: E501
        result.is_valid = False
        result.missing_columns.extend(missing_required)
        result.errors.append(
            f"Strategy {import_strategy}: required columns missing: {', '.join(missing_required)}"
        )

    # 2. Check for column types
    dico_dtypes = {
        **{name: pl.String for name in questions.filter(pl.col("type") == "text")["name"].unique()},
        **{
            name: pl.Int64
            for name in questions.filter(pl.col("type") == "integer")["name"].unique()
        },
        **{
            name: pl.Float64
            for name in questions.filter(pl.col("type") == "calculate")["name"].unique()
        },
    }
    type_validation_result = _validate_column_types(
        df, {**requirements["types"], **dico_dtypes}, import_strategy
    )
    result.invalid_types.update(type_validation_result)
    if type_validation_result:
        result.is_valid = False
        for col, (expected, actual) in type_validation_result.items():
            result.errors.append(
                f"Invalid type for column '{col}': expected {expected}, got {actual}"
            )

    # 3. Validate column presence
    result.required_columns_present = requirements["required"] & current_columns

    # 4. Detect unexpected columns
    expected_columns = requirements["required"] | requirements["optional"]
    unexpected_columns = current_columns - expected_columns

    if unexpected_columns:
        truly_unexpected = unexpected_columns - set(questions["name"].unique())
        for col in truly_unexpected:
            result.warnings.append(f"Unexpected column found: '{col}'")

    return result.__dict__


def _validate_column_types(
    df: pl.DataFrame, type_requirements: dict, import_strategy: str
) -> dict[str, tuple[str, str]]:
    """Validates column types with flexibility."""  # noqa: DOC201
    invalid_types = {}
    schema = df.schema

    if import_strategy == "DELETE":
        type_requirements = {"id": pl.Utf8}

    for column, expected_type in type_requirements.items():
        if column not in schema:
            continue

        actual_type = schema[column]

        # Handle cases where expected_type is a tuple of types
        if isinstance(expected_type, tuple):
            if actual_type not in expected_type:
                expected_str = " or ".join(str(t) for t in expected_type)
                invalid_types[column] = (expected_str, str(actual_type))
        else:
            if actual_type != expected_type:
                invalid_types[column] = (str(expected_type), str(actual_type))

    return invalid_types


def validate_global_data(
    df: pl.DataFrame, questions: pl.DataFrame, choices: pl.DataFrame
) -> pl.DataFrame:
    """Validate global data constraints and choices for form submissions.

    Args:
        df: DataFrame containing the submissions data.
        questions: DataFrame containing the form questions metadata.
        choices: DataFrame containing the form choices metadata.

    Returns:
        DataFrame with added validation columns for constraints and choices.
    """
    # 1) Computed fields: add missing calculate columns
    computed_fields = (
        questions.filter(pl.col("type") == "calculate").select(["name", "calculation"]).to_dicts()
    )
    for rule in computed_fields:
        col_name = rule["name"]
        calculation = rule.get("calculation")
        if not calculation or col_name in df.columns:
            continue

        expr_str = _calculate_to_polars_expr(calculation)
        try:
            # expr_str is expected to be a Polars expression string using `pl` namespace
            expr = eval(expr_str, {"pl": pl})
            df = df.with_columns(expr.alias(col_name))
        except Exception as exc:  # keep narrow enough but robust
            current_run.log_critical(
                f"Failed to compute calculated column '{col_name}': {exc}; filling with 0"
            )
            df = df.with_columns(pl.lit(0).alias(col_name))

    # 2) Constraints validation: add <col>_valid boolean columns
    constraints_list = (
        questions.filter(pl.col("constraint").is_not_null())
        .select(["name", "constraint"])
        .to_dicts()
    )
    constraint_cols = []
    for rule in constraints_list:
        col_name = rule["name"]
        constraint = rule["constraint"]
        if col_name not in df.columns:
            current_run.log_warning(f"Constraint for missing column '{col_name}' skipped")
            continue

        # Use a Python validator per element because constraints may be arbitrary;
        # bind the constraint string into the function to avoid late-binding issues.
        def _validate_elem(v: object, c: str = constraint) -> bool:
            try:
                return bool(_validate_value(str(v), c))
            except Exception:
                return False

        df = df.with_columns(
            pl.col(col_name)
            .map_elements(_validate_elem)
            .cast(pl.Boolean)
            .fill_null(False)
            .alias(f"{col_name}_valid")
        )
        constraint_cols.append(f"{col_name}_valid")

    # combine constraints into a per-row summary
    if constraint_cols:
        df = df.with_columns(
            pl.fold(
                acc=pl.lit(True),
                function=lambda acc, x: acc & x,
                exprs=[pl.col(c) for c in constraint_cols],
            ).alias("constraints_validation_summary")
        )
        # drop individual constraint columns to keep output tidy
        df = df.drop(constraint_cols)

    # 3) Choices validation: vectorized membership checks where possible
    choices_field = questions.filter(pl.col("type").str.contains("select"))["name"].to_list()
    list_name = next((c for c in ("list name", "list_name") if c in choices.columns), None)
    if list_name is None:
        current_run.log_warning(
            "Choices metadata missing expected column 'list name' or 'list_name'; "
            "skipping choices validation"
        )
        return df

    choices_cols = []
    for col_name in choices_field:
        if col_name not in df.columns:
            # nothing to validate for absent column
            continue

        # build allowed values for this question
        allowed = choices.filter(pl.col(list_name) == col_name)["label"].to_list()
        if not allowed:
            # no allowed values defined; mark as invalid conservatively
            df = df.with_columns(pl.lit(False).alias(f"{col_name}_choices_valid"))
            choices_cols.append(f"{col_name}_choices_valid")
            continue

        # vectorized membership test
        df = df.with_columns(
            pl.col(col_name).is_in(allowed).fill_null(False).alias(f"{col_name}_choices_valid")
        )
        choices_cols.append(f"{col_name}_choices_valid")

    if choices_cols:
        df = df.with_columns(
            pl.fold(
                acc=pl.lit(True),
                function=lambda acc, x: acc & x,
                exprs=[pl.col(c) for c in choices_cols],
            ).alias("choices_validation_summary")
        )

    return df


def _calculate_to_polars_expr(calc_str: str) -> str:
    expr = calc_str

    expr = re.sub(r"\$\{([^}]+)\}", r'pl.col("\1")', expr)

    expr = expr.replace(" div ", " / ")

    expr = re.sub(r"round\((.+?),\s*0\)", r"(\1).round()", expr)

    expr = re.sub(r"round\((.+?)\)", r"(\1).round()", expr)

    expr = re.sub(r"abs\((.+?)\)", r"(\1).abs()", expr)

    def repl_coalesce(match: re.Match) -> str:
        args = match.group(1)
        return f"pl.coalesce([{args}])"

    return re.sub(r"coalesce\((.+?)\)", repl_coalesce, expr)


def validate_field_constraints(
    record: dict, questions: pl.DataFrame, choices: pl.DataFrame
) -> bool:
    """Validate field constraints and choices for a single record.

    Args:
        record: Dictionary containing field values to validate.
        questions: DataFrame containing form questions metadata.
        choices: DataFrame containing form choices metadata.

    Returns:
        bool: True if all field values satisfy their constraints, False otherwise.
    """
    constraints_fields = questions.filter(pl.col("constraint").is_not_null())["name"].to_list()
    multiple_choices = questions.filter(pl.col("type").str.contains("select"))["name"].to_list()
    is_valid = True
    for col, value in record.items():
        if col in constraints_fields:
            constraints = questions.filter(pl.col("name") == col)["constraint"][0]
            is_valid = _validate_value(value, constraints)

        if col in multiple_choices:
            is_valid = (
                is_valid and value in choices.filter(pl.col("list name") == col)["label"].to_list()
            )
    return is_valid


def _validate_value(value: str, constraints: str) -> bool:
    if constraints.startswith("regex"):
        # Extract pattern
        pattern = re.search(r"regex\(.,\s*'(.+)'\)", constraints)
        if pattern:
            try:
                return bool(re.match(pattern.group(1), str(value)))
            except (ValueError, TypeError):
                return False

    if constraints.startswith(".<="):
        threshold = float(constraints[3:])
        try:
            return float(value) <= threshold
        except (ValueError, TypeError):
            return False

    elif constraints.startswith(".>="):
        threshold = float(constraints[3:])
        try:
            return float(value) >= threshold
        except (ValueError, TypeError):
            return False

    # Other constraintss
    else:
        # not yet implemented
        return True
