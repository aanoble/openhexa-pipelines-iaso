import re
import unicodedata

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


def calculate_to_polars_expr(calc_str: str) -> str:
    """Convert ODK calculate expressions to Polars expressions.

    Args:
        calc_str: The ODK calculate expression string to convert.

    Returns:
        A string containing the equivalent Polars expression.
    """
    expr = calc_str.strip()

    if expr in ("0", "0.0"):
        return "pl.lit(0)"

    expr = re.sub(r"\$\{([^}]+)\}", r'pl.col("\1")', expr)

    expr = expr.replace(" div ", " / ")

    expr = re.sub(r"round\((.+?),\s*0\)", r"(\1).round()", expr)

    expr = re.sub(r"round\((.+?)\)", r"(\1).round()", expr)

    expr = re.sub(r"abs\((.+?)\)", r"(\1).abs()", expr)

    def repl_coalesce(match: re.Match) -> str:
        args = match.group(1)
        return f"pl.coalesce([{args}])"

    return re.sub(r"coalesce\((.+?)\)", repl_coalesce, expr)


def local_name_xml_tag(tag: str) -> str:
    """Extract local name from a potentially namespaced XML tag.

    Args:
        tag (str): The XML tag name, possibly including namespace.

    Returns:
        str: The local name without namespace.
    """
    return tag.split("}", 1)[-1] if "}" in tag else tag
