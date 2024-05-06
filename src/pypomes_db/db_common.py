from __future__ import annotations
from logging import Logger
from pypomes_core import (
    APP_PREFIX,
    env_get_int, env_get_str, str_sanitize, str_get_positional
)

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
#   'MSQL', 'ORCL', 'PG', and 'SQLS', respectively for engines listed above

_DB_CONN_DATA: dict = {}
_DB_ENGINES: list[str] = []
if env_get_str(f"{APP_PREFIX}_DB_ENGINE",  None):
    _default_setup: bool = True
    _DB_ENGINES.append(env_get_str(f"{APP_PREFIX}_DB_ENGINE"))
else:
    _default_setup: bool = False
    _engines = env_get_str(f"{APP_PREFIX}_DB_ENGINES", None)
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
        "name":  env_get_str(f"{APP_PREFIX}_{_tag}_NAME"),
        "user": env_get_str(f"{APP_PREFIX}_{_tag}_USER"),
        "pwd": env_get_str(f"{APP_PREFIX}_{_tag}_PWD"),
        "host": env_get_str(f"{APP_PREFIX}_{_tag}_HOST"),
        "port": env_get_int(f"{APP_PREFIX}_{_tag}_PORT")
    }
    if engine == "oracle":
        _db_data["client"] = env_get_str(f"{APP_PREFIX}_{_tag}_CLIENT", None)
    elif engine == "sqlserver":
        _db_data["driver"] = env_get_str(f"{APP_PREFIX}_{_tag}_DRIVER")
    _DB_CONN_DATA[engine] = _db_data


def _assert_engine(errors: list[str],
                   engine: str) -> str:
    """
    Verify if *engine* is in the list of supported engines.

    If *engine* is a supported engine, it is returned. If its value is 'None',
    the first engine in the list of supported engines (the default engine) is returned.

    :param errors: incidental errors
    :param engine: the database engine to verify
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
                        query: str,
                        where_vals: tuple,
                        count: int,
                        require_min: int,
                        require_max: int) -> bool:
    """
    Verify whether the number of tuples returned is compliant with the constraints specified.

    :param errors:  incidental error messages
    :param query: the query statement used
    :param where_vals: the bind values used in the query
    :param count: the number of tuples returned
    :param require_min: optionally defines the minimum number of tuples to be returned
    :param require_max: optionally defines the maximum number of tuples to be returned
    :return: whether or not the number of tuples returned is compliant
    """
    # initialize the return variable
    result: bool = True

    # has an exact number of tuples been defined but not returned ?
    if (isinstance(require_min, int) and
        isinstance(require_max, int) and
        require_min == require_max and
        require_min != count):
        # yes, report the error, if applicable
        result = False
        if isinstance(errors, list):
            errors.append(
                f"{count} tuples returned, {require_min} expected, "
                f"for '{_db_build_query_msg(query, where_vals)}'"
            )
    # has a minimum number of tuples been defined but not returned ?
    elif (isinstance(require_min, int) and
          require_min > 0 and
          count < require_min):
        # yes, report the error, if applicable
        result = False
        if isinstance(errors, list):
            errors.append(
                f"{count} tuples returned, at least {require_min} expected, "
                f"for '{_db_build_query_msg(query, where_vals)}'"
            )

    return result

def _db_get_params(engine: str) -> tuple:
    """
    Return the current connection parameters being used for *engine*.

    The connection parameters are returned as a *tuple*, with the elements
    *name*, *user*, *pwd*, *host*, *port*.
    The meaning of some parameters may vary between different database engines.

    :param engine: the database engine
    :return: the current connection parameters for the engine
    """
    name: str = _DB_CONN_DATA[engine]["name"]
    user: str = _DB_CONN_DATA[engine]["user"]
    pwd: str = _DB_CONN_DATA[engine]["pwd"]
    host: str = _DB_CONN_DATA[engine]["host"]
    port: int = _DB_CONN_DATA[engine]["port"]

    result: tuple
    if engine == "sqlserver":
        driver: str = _DB_CONN_DATA[engine]["driver"]
        result = (name, user, pwd, host, port, driver)
    else:
        result = (name, user, pwd, host, port)

    return result


def _db_except_msg(exception: Exception,
                   engine: str) -> str:
    """
    Format and return the error message corresponding to the exception raised while accessing the database.

    :param exception: the exception raised
    :param engine: the database engine to use (uses the default engine, if not specified)
    :return: the formatted error message
    """
    name: str = _DB_CONN_DATA[engine]["name"]
    host: str = _DB_CONN_DATA[engine]["host"]
    return f"Error accessing '{name}' at '{host}': {str_sanitize(f'{exception}')}"


def _db_build_query_msg(query_stmt: str,
                        bind_vals: tuple) -> str:
    """
    Format and return the message indicative of a query problem.

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


def _db_log(errors: list[str],
            err_msg: str,
            logger: Logger,
            query_stmt: str,
            bind_vals: tuple = None) -> None:
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
