from datetime import date, datetime
from pypomes_core import DATE_FORMAT_INV, DATETIME_FORMAT_INV
from typing import Any


def bind_arguments(stmt: str,
                   bind_vals: list[Any]) -> str:
    """
    Replace the placeholders in *query_stmt* with the values in *bind_vals*, and return the modified query statement.

    Note that using a statement in a situation where values for types other than *bool*, *str*, *int*, *float*,
    *date*, or *datetime* were replaced, may bring about undesirable consequences, as the standard string
    representations for these other types would be used.

    :param stmt: the query statement
    :param bind_vals: the values to replace the placeholders with
    :return: the query statement with the placeholders replaced with their corresponding values
    """
    # initialize the return variable
    result: str | None = None

    # bind the arguments
    for bind_val in bind_vals:
        val: str
        if isinstance(bind_val, bool):
            val = "1" if bind_val else "0"
        elif isinstance(bind_val, int | float):
            val = f"{bind_val}"
        elif isinstance(bind_val, date):
            val = f"STR_TO_DATE('{bind_val.strftime(format=DATE_FORMAT_INV)}', '%Y-%m-%d')"
        elif isinstance(bind_val, datetime):
            val = f"STR_TO_DATE('{bind_val.strftime(format=DATETIME_FORMAT_INV)}', '%Y-%m-%d %H:%i:%s')"
        else:
            val = f"'{bind_val}'"
        result = stmt.replace("?", val, 1)

    return result
