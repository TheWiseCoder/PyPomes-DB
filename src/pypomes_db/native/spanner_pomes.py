import sys
from datetime import date, datetime, time
from decimal import Decimal
# unused scalar elements: INTERVAL, JSON, NUMERIC, PG_JSONB, PG_NUMERIC, PG_OID
from google.cloud.spanner_v1.param_types import (
    Type, BOOL, BYTES, DATE, FLOAT32, FLOAT64, INT64, STRING, TIMESTAMP
)
from io import BytesIO
from logging import Logger
from pathlib import Path
from pypomes_core import (
    APP_PREFIX, TZ_LOCAL, DateFormat, DatetimeFormat,
    env_get_str, exc_format, str_splice
)
from typing import Any

from ..db_common import (
    DB_CONN_DATA, DbEngine, DbParam, _assert_query_quota
)
from .spanner_frame import GoogleSpanner, SpannerConnection, SpannerParam


RESERVED_WORDS: list[str] = [
    "ALL", "AND", "ANY", "ARRAY", "AS", "ASC", "ASSERT_ROWS_MODIFIED", "AT", "BETWEEN", "BY",
    "CASE", "CAST", "COLLATE", "CONTAINS", "CREATE", "CROSS", "CUBE", "CURRENT",
    "DEFAULT", "DEFINE", "DESC", "DISTINCT", "ELSE", "END", "ENUM", "ESCAPE", "EXCEPT",
    "EXCLUDE", "EXISTS", "EXTRACT", "FALSE", "FETCH", "FOLLOWING", "FOR", "FROM", "FULL",
    "GROUP", "GROUPING", "GROUPS", "HASH", "HAVING", "IF", "IN", "INNER", "INTERSECT",
    "INTERVAL", "INTO", "IS", "JOIN", "LEFT", "LIKE", "LIMIT", "LOOKUP", "MERGE",
    "NATURAL", "NEW", "NO", "NOT", "NULL", "NULLS", "OF", "ON", "OR", "ORDER", "OUTER", "OVER",
    "PARTITION", "PRECEDING", "PROTO", "RANGE", "RECURSIVE", "RESPECT", "RIGHT", "ROLLUP", "ROWS",
    "SELECT", "SET", "SOME", "STRUCT", "TABLESAMPLE", "THEN", "TO", "TREAT", "TRUE",
    "UNBOUNDED", "UNION", "UNNEST", "USING", "WHEN", "WHERE", "WINDOW", "WITH"
]


def spanner_setup(engine_id: str,
                  instance_id: str,
                  database_id: str,
                  project_id: str = None,
                  credentials: dict[str, str] | BytesIO | Path | str | bytes = None,
                  session_default_timeout: int = 10,
                  session_pool_size: int = 10,
                  session_ping_interval: int = 3000,
                  logger: Logger = None,
                  errors: list[str] = None) -> bool:
    """
    Establish the configuration parameters for access to the Google Cloud Spanner engine.

    If the Cloud Spanner's local emulator is being used, no session usage is possible, and consequently,
    no session pools are available. The standard way to enable local emulation is to set the environment
    variable *SPANNER_EMULATOR_HOST* to its local IP address (e.g., *http://localhost:9010*).

    :param engine_id: the engine identification
    :param instance_id: the instance identification
    :param database_id: the databae identification
    :param project_id: optional project identification
    :param credentials: optional access credentials
    :param session_default_timeout: time to wait for a returned session (defaults to 10 seconds)
    :param session_pool_size: size of the session pool (defaults to 10 for cloud operation)
    :param session_ping_interval: interval at which to ping sessions in the background (defaults to 3000 seconds)
    :param logger: the logger for the database engine
    :param errors: incidental error messages (may be a non-empty list)
    :return: *True* if the data was accepted, *False* otherwise
    """
    # initialize the return variable
    result: bool = False
    op_errors: list[str] = []

    if env_get_str(key=f"{APP_PREFIX}_SPANNER_EMULATOR_HOST"):
        session_pool_size = 0
        session_ping_interval = 0
        session_default_timeout = 0

    if engine_id and instance_id and database_id and (credentials or project_id):
        DB_CONN_DATA[engine_id] = {
            DbParam.TYPE: DbEngine.SPANNER,
            SpannerParam.ENGINE: None,                  # instance of GoogleSpanner
            SpannerParam.DATABASE_ID: database_id,
            SpannerParam.INSTANCE_ID: instance_id,
            SpannerParam.PROJECT_ID: project_id,
            SpannerParam.CREDENTIALS: credentials,
            SpannerParam.SESSION_POOL_SIZE: session_pool_size,
            SpannerParam.SESSION_PING_INTERVAL: session_ping_interval,
            SpannerParam.SESSION_DEFAULT_TIMEOUT: session_default_timeout,
            DbParam.LOGGER: logger
        }
        GoogleSpanner(engine_id=engine_id,
                      errors=op_errors)
    else:
        op_errors.append(f"GoogleSpanner '{engine_id}' has missing setup data")

    for op_error in op_errors:
        if logger:
            logger.error(msg=op_error)
        if isinstance(errors, list):
            errors.append(op_error)

    return result


def connect(engine_id: str,
            autocommit: bool = None,
            errors: list[str] = None) -> SpannerConnection | None:
    """
    Obtain and return a connection to the database.

    Return *None* if the connection could not be obtained.

    :param engine_id: the reference database engine
    :param autocommit: whether the connection is to be in autocommit mode (defaults to *False*)
    :param errors: incidental error messages (must be *[]* or *None*)
    :return: the connection to the database, or *None* if error
    """
    # initialize the return variable
    result: SpannerConnection | None = None

    try:
        result = SpannerConnection(engine_id=engine_id,
                                   autocommit=autocommit,
                                   errors=errors)
    except Exception as e:
        exc_err: str = exc_format(exc=e,
                                  exc_info=sys.exc_info())
        logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
        if logger:
            logger.error(msg=exc_err)
        if isinstance(errors, list):
            errors.append(exc_err)

    return result


def select(engine_id: str,
           sel_stmt: str,
           where_vals: tuple | None,
           min_count: int | None,
           max_count: int | None,
           offset_count: int | None,
           limit_count: int | None,
           connection: SpannerConnection | None,
           committable: bool | None,
           errors: list[str] = None) -> list[tuple] | None:
    """
    Query the database and return all tuples that satisfy the *sel_stmt* command.

    The command can optionally contain selection criteria, with respective values given in *where_vals*.
    If not positive integers, *min_count*, *max_count*, *offset_count*, and *limit_count* are ignored.
    If both *min_count* and *max_count* are specified with equal values, then exactly that number of
    tuples must be returned by the query. The parameter *offset_count* is used to offset the retrieval
    of tuples, and *limit_count* establishes a ceiling on the number of tuples returned.
    If the search is empty, an empty list is returned.

    The parameter *committable* defines whether a commit or rollback is performed on the provided *connection*.

    :param engine_id: the reference database engine
    :param sel_stmt: SELECT command for the search
    :param where_vals: the values to be associated with the selection criteria
    :param min_count: optionally defines the minimum number of tuples expected
    :param max_count: optionally defines the maximum number of expected
    :param offset_count: number of tuples to skip
    :param limit_count: limit to the number of tuples returned, to be specified in the query statement itself
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation upon errorless completion
    :param errors: incidental error messages (must be *[]* or *None*)
    :return: list of tuples containing the search result, *[]* on empty search, or *None* if error
    """
    # initialize the return variable
    result: list[tuple] | None = None

    # make sure to have an errors list
    if not isinstance(errors, list):
        errors = []

    # make sure to have a connection
    curr_conn: SpannerConnection = connection or connect(engine_id=engine_id,
                                                         autocommit=True,
                                                         errors=errors)
    # build the parameter maps
    param_values: dict[str, Any] | None = None
    param_types: dict[str, Type] | None = None
    if not errors and where_vals:
        param_values, param_types = __build_param_maps(engine_id=engine_id,
                                                       values=where_vals,
                                                       errors=errors)
    if not errors:
        # establish offset and limit
        if offset_count or limit_count:
            sel_stmt = add_query_limits(sel_stmt=sel_stmt,
                                        offset_count=offset_count,
                                        limit_count=limit_count)
        # retrieve the tuples
        rows: list[tuple] = curr_conn.select(sel_stmt=sel_stmt,
                                             param_values=param_values,
                                             param_types=param_types,
                                             committable=committable,
                                             errors=errors)
        if not errors:
            # log the retrieval operation
            count: int = len(rows)
            logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
            if logger:
                from_table: str = str_splice(sel_stmt + " ",
                                             seps=(" FROM ", " "))[1]
                logger.debug(msg=f"Read {count} tuples from "
                                 f"{DbEngine.SPANNER}.{from_table}, offset "
                                 f"{offset_count}, connection {id(curr_conn)}")
            if _assert_query_quota(query=sel_stmt,
                                   engine_type=DbEngine.SPANNER,
                                   where_vals=where_vals,
                                   count=count,
                                   min_count=min_count,
                                   max_count=max_count,
                                   errors=errors):
                # the query quota was satisfied, return the tuples
                result = rows

    return result


def execute(engine_id: str,
            exc_stmt: str,
            bind_vals: tuple | None,
            return_cols: dict[str, type] | None,
            min_count: int | None,
            max_count: int | None,
            connection: SpannerConnection | None,
            committable: bool | None,
            errors: list[str] = None) -> tuple | int | None:
    """
    Execute the command *exc_stmt* on the database.

    This command might be a DML ccommand modifying the database, such as inserting, updating or
    deleting tuples, or it might be a DDL statement, or it might even be an environment-related command.

    The optional bind values for this operation are in *bind_vals*. The optional *return_cols* indicate that
    the values of the columns therein should be returned upon execution of *exc_stmt*. This is typical for
    *INSERT* or *UPDATE* statements on tables with *identity-type* columns, which are columns whose values
    are generated by the database itself. Otherwise, the value returned is the number of inserted, modified,
    or deleted tuples, or *None* if an error occurred.

    If not positive integers, *min_count* and *max_count* are ignored. If both *min_count* and *max_count*
    are specified with equal values, then exactly that number of tuples must have been affected by the query.

    The parameter *committable* defines whether a commit or rollback is performed on the provided *connection*.

    :param engine_id: the reference database engine
    :param exc_stmt: the command to execute
    :param bind_vals: optional bind values
    :param return_cols: optional columns and respective types, whose values are returned on *INSERT* or *UPDATE*
    :param min_count: optionally defines the minimum number of tuples to be affected
    :param max_count: optionally defines the maximum number of tuples to be affected
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit or rollback the operation, upon completion
    :param errors: incidental error messages (must be *[]* or *None*)
    :return: the values of *return_cols*, the value returned by the operation, or *None* if error
    """
    # initialize the return variable
    result: tuple | int | None = None

    # make sure to have an errors list
    if not isinstance(errors, list):
        errors = []

    # make sure to have a connection
    curr_conn: SpannerConnection = connection or connect(engine_id=engine_id,
                                                         autocommit=True,
                                                         errors=errors)
    # build the parameter maps
    param_values: dict[str, Any] | None = None
    param_types: dict[str, Type] | None = None
    if not errors and bind_vals:
        param_values, param_types = __build_param_maps(engine_id=engine_id,
                                                       values=bind_vals,
                                                       errors=errors)
    if not errors:
        if return_cols:
            exc_stmt += F" THEN RETURN {', '.join(return_cols.keys())}"

        # execte the opration
        reply: tuple | int = curr_conn.execute(exc_stmt=exc_stmt,
                                               param_values=param_values,
                                               param_types=param_types,
                                               committable=committable,
                                               errors=errors)
        if not errors:
            if return_cols:
                result = reply
            elif isinstance(reply, int) and _assert_query_quota(query=exc_stmt,
                                                                engine_type=DbEngine.SPANNER,
                                                                where_vals=bind_vals,
                                                                count=reply,
                                                                min_count=min_count,
                                                                max_count=max_count,
                                                                errors=errors):
                # the query quota was satisfied
                result = reply

    return result


def add_query_limits(sel_stmt: str,
                     *,
                     offset_count: int | None,
                     limit_count: int | None) -> str:
    """
    Add offset and limit to *sel_stmt*.

    If not positive integers, *offset_count*, and *limit_count* are ignored. The parameter *offset_count*
    is used to offset the retrieval of tuples, whereas *limit_count* establishes a ceiling on the number
    of tuples returned.

    :param sel_stmt: the query statement to modify
    :param offset_count: number of tuples to skip
    :param limit_count: limit to the number of tuples returned
    :return: the modified query statement
    """
    # initialize the return variable
    result: str = sel_stmt

    # establish a limit to the number of tuples returned
    if isinstance(limit_count, int) and limit_count > 0:
        result += f" LIMIT {limit_count}"

    # establish an offset into the result set
    if isinstance(offset_count, int) and offset_count > 0:
        result += f" OFFSET {offset_count}"

    return result


def bind_arguments(stmt: str,
                   bind_vals: list[Any]) -> str:
    """
    Replace the placeholders in *stmt* with the values in *bind_vals*, and return the modified query statement.

    Note that using a statement in a situation where values for types other than *bool*, *str*, *int*, *float*,
    *date*, or *datetime* were replaced, may bring about undesirable consequences, as the standard string
    representations for these other types would be used.

    The third parameter in the *CONVERT()* function is the style code to be used. Refer to SQLServer's
    documentation for details about this function.

    :param stmt: the query statement
    :param bind_vals: the values to replace the placeholders with
    :return: the query statement with the placeholders replaced with their corresponding values
    """
    # initialize the return variable
    result: str = stmt

    # bind the arguments
    for bind_val in bind_vals:
        val: str
        if isinstance(bind_val, bool):
            val = "TRUE" if bind_val else "FALSE"
        elif isinstance(bind_val, int | float):
            val = f"{bind_val}"
        elif isinstance(bind_val, date):
            fmt: str = DateFormat.INV
            val = f"PARSE_DATE('{fmt}', '{bind_val.strftime(format=fmt)}')"
        elif isinstance(bind_val, datetime):
            fmt: str = DatetimeFormat.INV
            val = f"PARSE_TIMESTAMP('{fmt}', '{bind_val.strftime(format=fmt)}', '{TZ_LOCAL}')"
        elif isinstance(bind_val, time):
            fmt: str = "%H:%M:%S"
            val = f"PARSE_TIMESTAMP('{fmt}', '{bind_val.strftime(format=fmt)}', '{TZ_LOCAL}')"
        else:
            val = f"'{bind_val}'"
        result = result.replace("?", val, 1)

    return result


def __build_param_maps(engine_id: str,
                       values: tuple,
                       errors: list[str]) -> tuple[dict[str, Any], dict[str, Type]]:
    """
    Build the mappings of parameters to values and types, as per the Cloud Spanner standard.

    :param values: values to be used
    :param errors: incidental error messages
    :return: mappings of params to values and params to types
    """
    # initialize the return variables
    param_values: dict[str, Any] = {}
    param_types: dict[str, Type] = {}

    for idx, val in enumerate(iterable=values,
                              start=1):
        key: str = f"gs{idx}"
        param_values[key] = val
        if isinstance(val, str):
            param_types[key] = STRING
        elif isinstance(val, bytes):
            param_types[key] = BYTES
        elif isinstance(val, float):
            param_types[key] = FLOAT32
        elif isinstance(val, Decimal):
            param_types[key] = FLOAT64

        # HAZARD: must test for 'bool' first ('bool' is subclass of 'int')
        elif isinstance(val, bool):
            param_types[key] = BOOL
        elif isinstance(val, int):
            param_types[key] = INT64

        # HAZARD: must test for 'datetime' first ('datetime' is subclass of 'date')
        elif isinstance(val, datetime):
            param_types[key] = TIMESTAMP
        elif isinstance(val, date):
            param_types[key] = DATE

        else:
            msg: str = f"Unable to handle type '{val.__class__}"
            logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
            if logger:
                logger.error(msg=msg)
            errors.append(msg)

    return param_values, param_types
