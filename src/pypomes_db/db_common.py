from logging import Logger
from pypomes_core import APP_PREFIX, env_get_int, env_get_str, str_sanitize
from typing import Final

DB_ENGINE: Final[str] = env_get_str(f"{APP_PREFIX}_DB_ENGINE")
DB_NAME: Final[str] = env_get_str(f"{APP_PREFIX}_DB_NAME")
DB_HOST: Final[str] = env_get_str(f"{APP_PREFIX}_DB_HOST")
DB_PORT: Final[int] = env_get_int(f"{APP_PREFIX}_DB_PORT")
DB_PWD: Final[str] = env_get_str(f"{APP_PREFIX}_DB_PWD")
DB_USER: Final[str] = env_get_str(f"{APP_PREFIX}_DB_USER")


def _assert_query_quota(errors: list[str], query: str, where_vals: tuple,
                        count: int, require_min: int, require_max: int) -> bool:

    # initialize the return variable
    result: bool = True

    # has an exact number of tuples been defined but not returned ?
    if isinstance(require_min, int) and isinstance(require_max, int) and \
            require_min == require_max and require_min != count:
        # yes, report the error, if applicable
        if isinstance(errors, list):
            errors.append(
                f"{count} tuples returned, exactly {require_min} expected, "
                f"for '{_db_build_query_msg(query, where_vals)}'"
            )

    # has a minimum number of tuples been defined but not returned ?
    elif isinstance(require_min, int) and require_min > 0 and count < require_min:
        # yes, report the error, if applicable
        if isinstance(errors, list):
            errors.append(
                f"{count} tuples returned, at least {require_min} expected, "
                f"for '{_db_build_query_msg(query, where_vals)}'"
            )

    return result


def _db_except_msg(exception: Exception) -> str:
    """
    Format and return the error message corresponding to the exception raised while accessing the database.

    :param exception: the exception raised
    :return:the formatted error message
    """
    return f"Error accessing '{DB_NAME}' at '{DB_HOST}': {str_sanitize(f'{exception}')}"


def _db_build_query_msg(query_stmt: str, bind_vals: tuple) -> str:
    """
    Format and return the message indicative of an empty search.

    :param query_stmt: the query command
    :param bind_vals: values associated with the query command
    :return: message indicative of empty search
    """
    result: str = str_sanitize(query_stmt)

    if bind_vals:
        for val in bind_vals:
            if isinstance(val, str):
                sval: str = f"'{val}'"
            else:
                sval: str = str(val)
            result = result.replace("?", sval, 1)

    return result


def _db_log(errors: list[str], err_msg: str, logger: Logger,
            query_stmt: str, bind_vals: tuple = None) -> None:
    """
    Log *err_msg* and add it to *errors*, or else log the executed query, whichever is applicable.

    :param errors: incidental errors
    :param err_msg: the error message
    :param logger: the logger object
    :param query_stmt: the query statement
    :param bind_vals: optional bind values for the query statement
    """
    if err_msg:
        if logger:
            logger.error(err_msg)
        if isinstance(errors, list):
            errors.append(err_msg)
    elif logger:
        debug_msg: str = _db_build_query_msg(query_stmt, bind_vals)
        logger.debug(debug_msg)
