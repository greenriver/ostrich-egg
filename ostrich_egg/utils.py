import os
import logging
from typing import List

import duckdb
from jinja2 import Environment, PackageLoader

ENVIRONMENT_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DEFAULT_MASKING_VALUE = "redacted"


def get_logger(name: str = "anonymization_service"):
    logger = logging.getLogger(name=name)
    logger.setLevel(getattr(logging, ENVIRONMENT_LOG_LEVEL))
    formatter = logging.Formatter(
        "%(asctime)s-%(filename)s:%(lineno)d-%(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(console_handler)
    return logger


def identifier(column: str):
    """
    Much like postgres, a duckdb identifier is wrapped in double-quotes.
    If your column somehow gets to us with double quotes, we will simply strip them.
    """
    return f'''"{column.replace('"', '')}"'''


def dict_to_filter_expressions(data: dict) -> List[duckdb.Expression]:
    return [
        (
            duckdb.ColumnExpression(key).isin(duckdb.ConstantExpression(value))
            if value is not None
            else duckdb.ColumnExpression(key).isnull()
        )
        for key, value in data.items()
    ]


def merge_conditions(conditions: List[duckdb.Expression]) -> str:
    return " and ".join([str(f) for f in conditions])


def apply_list_of_filters_to_relation(
    relation: duckdb.DuckDBPyRelation, filters: List[duckdb.Expression]
) -> duckdb.DuckDBPyRelation:
    # this could also be written pithier with reduce
    # from functools import reduce
    # return reduce(lambda tbl, f: tbl.filter(f), filters, relation )
    # I think it's better this way, the above or similar wrap it in many subqueries, it lacks active records .merge() functionality

    return relation.filter(merge_conditions(filters))


def make_when_statement_from_dict(data: dict, value: str) -> duckdb.CaseExpression:
    """haven't figured out how to merge filter expressions with AND/OR, so instead of the Expression API we'll just compile the SQL"""
    value_expression = duckdb.ConstantExpression(value)
    filters = dict_to_filter_expressions(data)
    condition = merge_conditions(filters)
    expression = f"when {condition} then {value_expression}"
    return expression


def should_redact_along_axis(
    incidence: float | int,
    masked_value_count: int = 0,
    minimum_threshold: int | float = 11,
    is_anonymous: bool = True,
    previous_cell_redacted: bool | None = None,
    run_sum_by_axis: float | int = 0,
) -> bool:
    """
    Given a set of dimensions (i.e., the axis) and some pre-calculated windowed rows,
    determine if the cell needs to be redacted based on the available criteria.

    This will be run iteratively until the dataset consists only of cells that are suppressed to anonymity.
    """
    if not is_anonymous:
        return True  # all non-anonymous cells need redacted.
    if previous_cell_redacted is False:
        return False  # there is no latency in this pass, do not redact.

    # if previous_cell_redacted is True
    if run_sum_by_axis - incidence >= minimum_threshold and masked_value_count >= 2:
        return False  # The window along this axis is sufficiently redacted
    # otherwise, we should redact this cell if
    # the previous cell was redacted as the window being surveyed along this axis is insufficiently redacted.
    # if the previous cell was not redacted, we do not need to redact this cell.
    return previous_cell_redacted is True


ostrich_egg_jinja_env = Environment(
    loader=PackageLoader("ostrich_egg"),
)
ostrich_egg_jinja_env.filters.update(
    {
        "identifier": identifier,
        "dequote": lambda x: x.replace('"', ""),
        "list_of_identifiers": lambda x: ", ".join([identifier(i) for i in x]),
    }
)
