import sys
from datetime import date, datetime
from decimal import Decimal
# unused scalar elements: INTERVAL, JSON, NUMERIC, PG_JSONB, PG_NUMERIC, PG_OID
from google.cloud.spanner_v1.param_types import (
    Type, BOOL, BYTES, DATE, FLOAT32, FLOAT64, INT64, STRING, TIMESTAMP
)
from pypomes_core import env_get_str, exc_format, str_splice
from typing import Any

from .db_common import (
    _DB_CONN_DATA, _DB_LOGGERS, DbEngine, _assert_query_quota
)
from .db_pomes import db_add_query_limits
from .spanner_frame import SpannerConnection, SpannerParam


def spanner_setup(instance_id: str,
                  database_id: str,
                  session_pool_size: int = 10,
                  session_ping_interval: int = 3000,
                  session_default_timeout: int = 10) -> bool:
    """
    Establish the configuration parameters for access to the Google Cloud Spanner engine.

    If the Cloud Spanner's local emulator is being used, no session usage is possible, and consequently,
    no session pools are available. The standard way to enable local emulation is to set the environment
    variable *SPANNER_EMULATOR_HOST* to its local IP address (e.g., *http://localhost:9010*).

    :param instance_id: the instance identification
    :param database_id: the databae identification
    :param session_pool_size: size of the session pool (defaults to 10 for cloud operation)
    :param session_ping_interval: interval at which to ping sessions in the background (defaults to 3000 seconds)
    :param session_default_timeout: time to wait for a returned session (defaults to 10 seconds)
    :return: *True* if the data was accepted, *False* otherwise
    """
    # initialize the return variable
    result: bool = False

    emulated: bool = bool(env_get_str(key="SPANNER_EMULATOR_HOST"))

    # accept data only if GoogleSpanner has not been instantiated
    if not (_DB_CONN_DATA.get(DbEngine.SPANNER) or {}).get(SpannerParam.ENGINE):
        _DB_CONN_DATA[DbEngine.SPANNER] = {
            SpannerParam.DATABASE_ID: database_id,
            SpannerParam.INSTANCE_ID: instance_id,
            SpannerParam.SESSION_POOL_SIZE: 0 if emulated else session_pool_size,
            SpannerParam.SESSION_PING_INTERVAL: 0 if emulated else session_ping_interval,
            SpannerParam.SESSION_DEFAULT_TIMEOUT: 0 if emulated else session_default_timeout
        }
        result = True

    return result


def connect(autocommit: bool = None,
            errors: list[str] = None) -> SpannerConnection | None:
    """
    Obtain and return a connection to the database.

    Return *None* if the connection could not be obtained.

    :param autocommit: whether the connection is to be in autocommit mode (defaults to *False*)
    :param errors: incidental error messages (must be *[]* or *None*)
    :return: the connection to the database, or *None* if error
    """
    # initialize the return variable
    result: SpannerConnection | None = None

    try:
        result = SpannerConnection(errors=errors,
                                   autocommit=autocommit)
    except Exception as e:
        exc_err: str = exc_format(exc=e,
                                  exc_info=sys.exc_info())
        if _DB_LOGGERS[DbEngine.SPANNER]:
            _DB_LOGGERS[DbEngine.SPANNER].error(msg=exc_err)
        if isinstance(errors, list):
            errors.append(exc_err)

    return result


def select(sel_stmt: str,
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
    curr_conn: SpannerConnection = connection or connect(autocommit=True,
                                                         errors=errors)
    # build the parameter maps
    param_values: dict[str, Any] | None = None
    param_types: dict[str, Type] | None = None
    if not errors and where_vals:
        param_values, param_types = __build_param_maps(values=where_vals,
                                                       errors=errors)
    if not errors:
        # establish offset and limit
        if offset_count or limit_count:
            sel_stmt = db_add_query_limits(sel_stmt=sel_stmt,
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
            if _DB_LOGGERS[DbEngine.POSTGRES]:
                from_table: str = str_splice(sel_stmt + " ",
                                             seps=(" FROM ", " "))[1]
                _DB_LOGGERS[DbEngine.POSTGRES].debug(msg=f"Read {count} tuples from "
                                                         f"{DbEngine.SPANNER}.{from_table}, offset "
                                                         f"{offset_count}, connection {id(curr_conn)}")
            if _assert_query_quota(query=sel_stmt,
                                   engine=DbEngine.SPANNER,
                                   where_vals=where_vals,
                                   count=count,
                                   min_count=min_count,
                                   max_count=max_count,
                                   errors=errors):
                # the query quota was satisfied, return the tuples
                result = rows

    return result


def execute(exc_stmt: str,
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

    :param exc_stmt: the command to execute
    :param bind_vals: optional bind values
    :param return_cols: optional columns and respective types, whose values are to be returned on *INSERT* or *UPDATE*
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
    curr_conn: SpannerConnection = connection or connect(autocommit=True,
                                                         errors=errors)
    # build the parameter maps
    param_values: dict[str, Any] | None = None
    param_types: dict[str, Type] | None = None
    if not errors and bind_vals:
        param_values, param_types = __build_param_maps(values=bind_vals,
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
                                                                engine=DbEngine.SPANNER,
                                                                where_vals=bind_vals,
                                                                count=reply,
                                                                min_count=min_count,
                                                                max_count=max_count,
                                                                errors=errors):
                # the query quota was satisfied
                result = reply

    return result


def __build_param_maps(values: tuple,
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
            if _DB_LOGGERS[DbEngine.SPANNER]:
                _DB_LOGGERS[DbEngine.SPANNER].error(msg=msg)
            errors.append(msg)

    return param_values, param_types
