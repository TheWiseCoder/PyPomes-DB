from logging import DEBUG, Logger
from pypomes_core import (
    APP_PREFIX,
    env_get_int, env_get_str, str_sanitize, str_get_positional
)
from typing import Any, Iterable

# the preferred way to specify database connection parameters is dynamically with 'db_setup_params'
# specifying database connection parameters with environment variables can be done in two ways:
# 1. specify the set
#   {APP_PREFIX}_DB_ENGINE (one of 'mysql', 'oracle', 'postgres', 'sqlserver')
#   {APP_PREFIX}_DB_NAME
#   {APP_PREFIX}_DB_USER
#   {APP_PREFIX}_DB_PWD
#   {APP_PREFIX}_DB_HOST
#   {APP_PREFIX}_DB_PORT
#   {APP_PREFIX}_DB_CLIENT  (for oracle)
#   {APP_PREFIX}_DB_DRIVER  (for sqlserver)
# 2. alternatively, specify a comma-separated list of engines in
#   {APP_PREFIX}_DB_ENGINES
#   and for each engine, specify the set above, replacing 'DB' with
#   'MSQL', 'ORCL', 'PG', and 'SQLS', respectively for the engines listed above

_DB_CONN_DATA: dict = {}
_DB_ENGINES: list[str] = []
_prefix: str = env_get_str(key=f"{APP_PREFIX}_DB_ENGINE")
if _prefix:
    _default_setup: bool = True
    _DB_ENGINES.append(_prefix)
else:
    _default_setup: bool = False
    _engines: str = env_get_str(key=f"{APP_PREFIX}_DB_ENGINES")
    if _engines:
        _DB_ENGINES.extend(_engines.split(sep=","))
for engine in _DB_ENGINES:
    if _default_setup:
        _tag = "DB"
        _default_setup = False
    else:
        _tag: str = str_get_positional(source=engine,
                                       list_origin=["mysql", "oracle", "postgres", "sqlserver"],
                                       list_dest=["MSQL", "ORCL", "PG", "SQLS"])
    _db_data = {
        "name":  env_get_str(key=f"{APP_PREFIX}_{_tag}_NAME"),
        "user": env_get_str(key=f"{APP_PREFIX}_{_tag}_USER"),
        "pwd": env_get_str(key=f"{APP_PREFIX}_{_tag}_PWD"),
        "host": env_get_str(key=f"{APP_PREFIX}_{_tag}_HOST"),
        "port": env_get_int(key=f"{APP_PREFIX}_{_tag}_PORT")
    }
    if engine == "oracle":
        _db_data["client"] = env_get_str(key=f"{APP_PREFIX}_{_tag}_CLIENT")
    elif engine == "sqlserver":
        _db_data["driver"] = env_get_str(key=f"{APP_PREFIX}_{_tag}_DRIVER")
    _DB_CONN_DATA[engine] = _db_data


def _assert_engine(errors: list[str],
                   engine: str) -> str:
    """
    Verify if *engine* is in the list of supported engines.

    If *engine* is a supported engine, it is returned. If its value is 'None',
    the first engine in the list of supported engines (the default engine) is returned.

    :param errors: incidental errors
    :param engine: the reference database engine
    :return: the validated or default engine
    """
    # initialize the return valiable
    result: str | None = None

    if not engine and _DB_ENGINES:
        result = _DB_ENGINES[0]
    elif engine in _DB_ENGINES:
        result = engine
    else:
        err_msg = f"Database engine '{engine}' unknown or not configured"
        errors.append(err_msg)

    return result


def _assert_query_quota(errors: list[str],
                        engine: str,
                        query: str,
                        where_vals: tuple,
                        count: int,
                        min_count: int,
                        max_count: int,
                        require_count: int) -> bool:
    """
    Verify whether the number of tuples returned is compliant with the constraints specified.

    :param errors: incidental error messages
    :param engine: the reference database engine
    :param query: the query statement used
    :param where_vals: the bind values used in the query
    :param count: the number of tuples returned
    :param min_count: optionally defines the minimum number of tuples to be returned
    :param max_count: optionally defines the maximum number of tuples to be returned
    :param require_count: number of touples that must exactly satisfy the query
    :return: whether or not the number of tuples returned is compliant
    """
    # initialize the control message variable
    err_msg: str | None = None

    # has an exact number of tuples been defined but not returned ?
    if (isinstance(require_count, int) and
        require_count > 0 and require_count != count):
        # yes, report the error, if applicable
        err_msg = f"{count} tuples returned, exactly {require_count} expected"

    # has a minimum number of tuples been defined but not returned ?
    elif (isinstance(min_count, int) and
          min_count > 0 and min_count > count):
        # yes, report the error, if applicable
        err_msg = f"{count} tuples returned, at least {min_count} expected'"

    # has a maximum number of tuples been defined but not complied with ?
    # INSANITY CHECK: expected to never occur
    elif (isinstance(max_count, int) and
        0 < max_count < count):
        # yes, report the error, if applicable
        err_msg = f"{count} tuples returned, up to {max_count} expected"

    if err_msg:
        result: bool = False
        if isinstance(errors, list):
            query: str = _build_query_msg(query_stmt=query,
                                          engine=engine,
                                          bind_vals=where_vals)
            errors.append(f"{err_msg}, for '{query}'")
    else:
        result: bool = True

    return result


def _get_param(engine: str,
               param: str) -> Any:
    """
    Return the current value of *param* being used by *engine*.

    :param engine: the reference database engine
    :param param: the reference parameter
    :return: the parameter's current value
    """
    return _DB_CONN_DATA.get(engine or {}).get(param)


def _get_params(engine: str) -> tuple:
    """
    Return the current connection parameters being used for *engine*.

    The connection parameters are returned as a *tuple*, with the elements
    *name*, *user*, *pwd*, *host*, *port*.
    For *oracle* engines, the extra element *client* is returned.
    For *sqlserver* engines, the extra element *driver* is returned.
    The meaning of some parameters may vary between different database engines.

    :param engine: the reference database engine
    :return: the current connection parameters for the engine
    """
    conn_data: dict = _DB_CONN_DATA[engine] or {}
    name: str = conn_data.get("name")
    user: str = conn_data.get("user")
    pwd: str = conn_data.get("pwd")
    host: str = conn_data.get("host")
    port: int = conn_data.get("port")

    result: tuple
    if engine == "sqlserver":
        driver: str = conn_data.get("driver")
        result = (name, user, pwd, host, port, driver)
    else:
        result = (name, user, pwd, host, port)

    return result


def _except_msg(exception: Exception,
                engine: str) -> str:
    """
    Format and return the error message corresponding to the exception raised while accessing the database.

    :param exception: the exception raised
    :param engine: the reference database engine
    :return: the formatted error message
    """
    name: str = _DB_CONN_DATA[engine].get("name")
    host: str = _DB_CONN_DATA[engine].get("host")
    return f"Error accessing '{name}' at '{host}': {str_sanitize(f'{exception}')}"


def _log(logger: Logger,
         engine: str,
         err_msg: str = None,
         level: int = DEBUG,
         errors: list[str] = None,
         stmt: str = None,
         bind_vals: tuple = None) -> None:
    """
    Log *err_msg* and add it to *errors*, or else log the executed query, whichever is applicable.

    :param logger: the logger object
    :param engine: the reference database engine
    :param err_msg: the error message to log
    :param level: log level (defaults to DEBUG)
    :param errors: optional incidental errors
    :param stmt: optional query statement
    :param bind_vals: optional bind values for the query statement
    """
    if err_msg:
        if logger:
            logger.log(level, err_msg)
        if isinstance(errors, list):
            errors.append(err_msg)
    if logger and stmt:
        log_msg: str = _build_query_msg(query_stmt=stmt,
                                        engine=engine,
                                        bind_vals=bind_vals)
        logger.log(level, log_msg)


def _build_query_msg(query_stmt: str,
                     engine: str,
                     bind_vals: tuple) -> str:
    """
    Format and return the message indicative of a query problem.

    :param query_stmt: the query command
    :param engine: the reference database engine
    :param bind_vals: values associated with the query command
    :return: message indicative of empty search
    """
    result: str = str_sanitize(query_stmt)

    for inx, val in enumerate(bind_vals or [], 1):
        if isinstance(val, str):
            sval: str = f"'{val}'"
        else:
            sval: str = str(val)
        match engine:
            case "oracle":
                result = result.replace(f":{inx}", sval, 1)
            case "postgres":
                result = result.replace("%s", sval, 1)
            case "sqlserver":
                result = result.replace("?", sval, 1)

    return result


def _bind_columns(engine: str,
                  columns: list[str],
                  concat: str,
                  start_index: int) -> str:
    """
    Concatenate a list of column names bindings, appropriate for the engine sepcified.

    The concatenation term *concat* is typically *" AND "*, if the bindings are aimed at a
    *WHERE* clause, or *", "* otherwise.

    :param engine: the reference database engine
    :param columns: the columns to concatenate
    :param concat: the concatenation term
    :param start_index: the index to start the enumeration (rlevant to oracle, only)
    :return: the concatenated string
    """

    # initialize the return variable
    result: str | None = None

    match engine:
        case "mysql":
            pass
        case "oracle":
            result = concat.join([f"{column} = :{inx}"
                                 for column, inx in enumerate(iterable=columns,
                                                              start=start_index)])
        case "postgres":
            result =  concat.join([f"{column} = %s" for column in columns])
        case "sqlserver":
            result = concat.join([f"{column} = ?" for column in columns])

    return result


def _bind_marks(engine: str,
                start: int,
                finish: int) -> str:
    """
    Concatenate a list of binding marks, appropriate for the engine sepcified.

    :param engine: the reference database engine
    :param start: the number to start from, inclusive
    :param finish: the number to finish at, exclusive
    :return: the concatenated string
    """

    # initialize the return variable
    result: str | None = None

    match engine:
        case "mysql":
            pass
        case "oracle":
            result = ",".join([f":{inx}" for inx in range(start, finish)])
        case "postgres":
            result = ",".join(["%s" for _inx in range(start, finish)])
        case "sqlserver":
            result = ",".join(["?" for _inx in range(start, finish)])

    return result


def _remove_nulls(row: Iterable) -> list[Any]:
    """
    Remove all occurrences of *NULL* (char(0)) values from the elements in *row*.

    :param row: the row to be cleaned
    :return: a row with cleaned data, or None if no cleaning was necessary
    """
    # initialize the return variable
    result: list[Any] | None = None

    cleaned_row: list[Any] = []
    was_cleaned: bool = False
    for val in row:
        # is 'val' a string containing NULLs ?
        if isinstance(val, str) and val.count(chr(0)) > 0:
            # yes, clean it up and mark the row as having been cleaned
            clean_val: str = val.replace(chr(0), "")
            was_cleaned = True
        else:
            clean_val: str = val
        cleaned_row.append(clean_val)

    # was the row cleaned ?
    if was_cleaned:
        # yes, return it
        result =  cleaned_row

    return result
