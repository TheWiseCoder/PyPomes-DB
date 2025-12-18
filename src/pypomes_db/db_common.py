from pypomes_core import (
    APP_PREFIX,
    str_sanitize, str_positional,
    env_get_str,  env_get_int,
    env_get_enum, env_get_enums, env_get_path
)
from enum import StrEnum, auto
from typing import Any, Final, Literal


class DbEngine(StrEnum):
    """
    Supported database engines.
    """
    MYSQL = auto()
    ORACLE = auto()
    POSTGRES = auto()
    SQLSERVER = auto()
    SPANNER = auto()


class DbParam(StrEnum):
    """
    Parameters for connecting to database engines. Class does not apply to Google Cloud Spanner.
    """
    ENGINE = auto()
    NAME = auto()
    USER = auto()
    PWD = auto()
    HOST = auto()
    PORT = auto()
    CLIENT = auto()
    DRIVER = auto()
    VERSION = auto()


# the bind meta-tag to use in DML statements
# (guarantees cross-engine compatilitiy, as this is replaced by the engine's bind tag)
DB_BIND_META_TAG: Final[str] = env_get_str(key=f"{APP_PREFIX}_DB_BIND_META_TAG",
                                           def_value="%?")
_BUILTIN_FUNCTIONS: Final[list[tuple[str, str, str, str]]] = [
    # current date only
    ("CURRENT_DATE()",              # MySQL: yyyy-mm-dd
     "CURRENT_DATE",                # Oracle: yyyy-mm-dd
     "CURRENT_DATE",                # PostgreSQL: yyyy-mm-dd
     "CONVERT(DATE, GETDATE())"),   # SQLServer: yyyy-mm-dd
    # current time only (trailing -/+ indicates the timezone difference from GMT)
    ("CURTIME()",                   # MySQL: hh:mm:ss
     "CURRENT_TIME",                # Oracle: hh:mm:ss.123456 - 00:00
     "CURRENT_TIME",                # PostgreSQL: hh:mm:ss.123456 + 00
     "CONVERT(TIME, GETDATE())"),   # SQLServer: hh:mm:ss.123
    # current date and time
    ("NOW()",                       # MySQL: yyyy-mm-dd hh:mm:ss
     "SYSDATE",                     # Oracle: yyyy-mm-dd hh:mm:ss
     "NOW()",                       # PostgreSQL: yyyy-mm-dd hh:mm:ss.123456 + 00
     "GETDATE()"),                  # SQLServer: yyyy-mm-dd hh:mm:ss.123
    # current timestamp
    ("CURRENT_TIMESTAMP()",         # MySQL: yyyy-mm-dd hh:mm:ss
     "SYSTIMESTAMP",                # Oracle: yyyy-mm-dd hh:mm:ss.123456 - 00:00
     "CURRENT_TIMESTAMP",           # PostgreSQL: yyyy-mm-dd hh:mm:ss.123456 + 00
     "SYSDATETIME()")               # SQLServer: yyyy-mm-dd hh:mm:ss.1234567
]


def __get_conn_data() -> dict[DbEngine, dict[DbParam, Any]]:
    """
    Establish the connection data for select database engines, from environment variables.

    The preferred way to specify database connection parameters is dynamically with *db_setup()*.
    Specifying database connection parameters with environment variables cannot be done for Google Cloud Spanner
    (instead, *GoogleSpanner.setup()* in module *spanner_pomes* must be used).
    For the other database engines, it can be done in two ways:

    1. for a single database engine, specify the data set
          - *<APP_PREFIX>_DB_ENGINE* (one of *mysql*, *oracle*, *postgres*, *sqlserver*)
          - *<APP_PREFIX>_DB_NAME*
          - *<APP_PREFIX>_DB_USER*
          - *<APP_PREFIX>_DB_PWD*
          - *<APP_PREFIX>_DB_HOST*
          - *<APP_PREFIX>_DB_PORT*
          - *<APP_PREFIX>_DB_CLIENT*  (for Oracle)
          - *<APP_PREFIX>_DB_DRIVER*  (for SQLServer)

    2. for multiple database engines, specify a comma-separated list of engines in
       *<APP_PREFIX>_DB_ENGINES*, and for each engine, specify the data set above,
       respectively replacing *_DB_* with *_MSQL_*, *_ORCL_*, *_PG_*, or *_SQLS_*,
       for the engines listed above

    All required parameters must be provided for the selected database engines, as there are no defaults.

    :return: the connection data for the select database engines
    """
    # initialize the return variable
    result: dict[DbEngine, dict[DbParam, Any]] = {}

    engines: list[DbEngine] = []
    single_engine: DbEngine = env_get_enum(key=f"{APP_PREFIX}_DB_ENGINE",
                                           enum_class=DbEngine)
    if single_engine:
        default_setup: bool = True
        if single_engine != DbEngine.SPANNER:
            engines.append(single_engine)
    else:
        default_setup: bool = False
        multi_engines: list[DbEngine] = env_get_enums(key=f"{APP_PREFIX}_DB_ENGINES",
                                                      enum_class=DbEngine)
        if multi_engines:
            engines.extend(multi_engines)

    for engine in engines:
        if default_setup:
            prefix: str = "DB"
            default_setup = False
        else:
            prefix: str = str_positional(engine,
                                         keys=tuple(DbEngine),
                                         values=("MSQL", "ORCL", "PG", "SQLS"))
        result[engine] = {
            DbParam.ENGINE: engine.value,
            DbParam.NAME: env_get_str(key=f"{APP_PREFIX}_{prefix}_NAME"),
            DbParam.USER: env_get_str(key=f"{APP_PREFIX}_{prefix}_USER"),
            DbParam.PWD: env_get_str(key=f"{APP_PREFIX}_{prefix}_PWD"),
            DbParam.HOST: env_get_str(key=f"{APP_PREFIX}_{prefix}_HOST"),
            DbParam.PORT: env_get_int(key=f"{APP_PREFIX}_{prefix}_PORT"),
            DbParam.VERSION: ""
        }
        if engine == DbEngine.ORACLE:
            result[engine][DbParam.CLIENT] = env_get_path(key=f"{APP_PREFIX}_{prefix}_CLIENT")
        elif engine == DbEngine.SQLSERVER:
            result[engine][DbParam.DRIVER] = env_get_str(key=f"{APP_PREFIX}_{prefix}_DRIVER")

    return result


# connection data for the configured database engines
_DB_CONN_DATA: Final[dict[DbEngine, dict[DbParam, Any]]] = __get_conn_data()


def _assert_engine(engine: DbEngine,
                   errors: list[str] = None) -> DbEngine | None:
    """
    Verify if *engine* is in the list of configured engines.

    If *engine* is a configured engine, it is returned. If its value is *None*,
    the first engine in the list of configured engines (the default engine) is returned.

    :param engine: the reference database engine
    :param errors: incidental error messages (might be a non-empty list)
    :return: the validated or the default engine, or *None* if error
    """
    # initialize the return valiable
    result: DbEngine | None = None

    if not engine and _DB_CONN_DATA:
        result = next(iter(_DB_CONN_DATA))
    elif engine in _DB_CONN_DATA:
        result = engine
    elif isinstance(errors, list):
        errors.append(f"Database engine '{engine}' unknown or not configured")

    return result


def _assert_query_quota(engine: DbEngine,
                        query: str,
                        where_vals: tuple | None,
                        count: int,
                        min_count: int | None,
                        max_count: int | None,
                        errors: list[str] = None) -> bool:
    """
    Verify whether the number of tuples returned is compliant with the constraints specified.

    :param engine: the reference database engine
    :param query: the query statement used
    :param where_vals: the bind values used in the query
    :param count: the number of tuples returned
    :param min_count: optionally defines the minimum number of tuples to be returned
    :param max_count: optionally defines the maximum number of tuples to be returned
    :param errors: incidental error messages
    :return: whether the number of tuples returned is compliant
    """
    # initialize the control message variable
    err_msg: str | None = None

    # has an exact number of tuples been defined but not returned ?
    if isinstance(min_count, int) and isinstance(max_count, int) and \
       min_count > 0 and min_count != count:
        # yes, report the error, if applicable
        err_msg = f"{count} tuples affected, exactly {min_count} expected"

    # has a minimum number of tuples been defined but not returned ?
    elif (isinstance(min_count, int) and
          min_count > 0 and min_count > count):
        # yes, report the error, if applicable
        err_msg = f"{count} tuples affected, at least {min_count} expected'"

    # has a maximum number of tuples been defined but not complied with ?
    # SANITY CHECK: expected to never occur for SELECTs
    elif isinstance(max_count, int) and 0 < max_count < count:
        # yes, report the error, if applicable
        err_msg = f"{count} tuples affected, up to {max_count} expected"

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


def _get_param(engine: DbEngine,
               param: DbParam) -> Any:
    """
    Return the current value of *param* being used by *engine*.

    :param engine: the reference database engine
    :param param: the reference parameter
    :return: the parameter's current value
    """
    return (_DB_CONN_DATA.get(engine) or {}).get(param)


def _get_params(engine: DbEngine) -> dict[DbParam, Any]:
    """
    Return the current connection parameters being used for *engine*.

    :param engine: the reference database engine
    :return: the current connection parameters for the engine
    """
    return _DB_CONN_DATA.get(engine)


# HAZARD: due to buggy PyCharm type checking, optional 'None' is added to type of parameter 'engine',
#         to prevent having to annotate all invocations of '_exc_msg' with 'noinspection PyTypeChecker'
def _except_msg(exception: Exception,
                connection: Any | None,
                engine: DbEngine | None) -> str:
    """
    Format and return the error message corresponding to the exception raised while accessing the database.

    :param exception: the exception raised
    :param engine: the reference database engine
    :return: the formatted error message
    """
    db_data: dict[DbParam, Any] = _DB_CONN_DATA.get(engine) or {}
    conn_data: str = f", connection {id(connection)}," if connection else ""
    return (f"Error accessing '{db_data.get(DbParam.NAME)}'{conn_data} "
            f"at '{db_data.get(DbParam.HOST)}': {str_sanitize(f'{exception}')}")


def _build_query_msg(query_stmt: str,
                     engine: DbEngine,
                     bind_vals: tuple | None) -> str:
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
            case DbEngine.MYSQL:
                pass
            case DbEngine.ORACLE:
                result = result.replace(f":{inx}", sval, 1)
            case DbEngine.POSTGRES:
                result = result.replace("%s", sval, 1)
            case DbEngine.SQLSERVER:
                result = result.replace("?", sval, 1)

    return result


def _bind_columns(engine: DbEngine,
                  columns: list[str],
                  concat: str,
                  start_index: int) -> str:
    """
    Concatenate a list of column names bindings, appropriate for the DB engine *engine*.

    The concatenation term *concat* is typically *AND*, if the bindings are aimed at a
    *WHERE* clause, or *,* otherwise.

    :param engine: the reference database engine
    :param columns: the columns to concatenate
    :param concat: the concatenation term
    :param start_index: the index to start the enumeration (relevant to *oracle*, only)
    :return: the concatenated string
    """
    # initialize the return variable
    result: str | None = None

    match engine:
        case DbEngine.MYSQL:
            pass
        case DbEngine.ORACLE:
            result = concat.join([f"{column} = :{inx}"
                                  for inx, column in enumerate(iterable=columns,
                                                               start=start_index)])
        case DbEngine.POSTGRES:
            result = concat.join([f"{column} = %s" for column in columns])
        case DbEngine.SQLSERVER:
            result = concat.join([f"{column} = ?" for column in columns])

    return result


def _bind_marks(engine: DbEngine,
                start: int,
                finish: int) -> str:
    """
    Concatenate a list of binding marks, appropriate for the engine specified.

    :param engine: the reference database engine
    :param start: the number to start from, inclusive
    :param finish: the number to finish at, exclusive
    :return: the concatenated string
    """
    # initialize the return variable
    result: str | None = None

    match engine:
        case DbEngine.MYSQL:
            pass
        case DbEngine.ORACLE:
            result = ",".join([f":{inx}" for inx in range(start, finish)])
        case DbEngine.POSTGRES:
            result = ",".join(["%s" for _inx in range(start, finish)])
        case DbEngine.SQLSERVER:
            result = ",".join(["?" for _inx in range(start, finish)])

    return result


def _combine_search_data(query_stmt: str,
                         where_clause: str,
                         where_vals: tuple,
                         where_data: dict[str |
                                          tuple[str,
                                                Literal["=", ">", "<", ">=", "<=",
                                                        "<>", "in", "like", "between"] | None,
                                                Literal["and", "or"] | None], Any] | None,
                         orderby_clause: str | None,
                         engine: DbEngine) -> tuple[str, tuple]:
    """
    Rebuild the query statement *query_stmt* and the list of bind values *where_vals*.

    This is done by adding to them the search criteria specified by the key-value pairs in *where_data*.
    The syntax specific to *where_data*'s key/value pairs is as follows:
        1. *key*:
            - an attribute (possibly aliased), or
            - a 2/3-tuple with an attribute and the corresponding SQL comparison operation
              ("=", ">", "<", ">=", "<=", "<>", "in", "like", "between" - defaults to "="), followed
              by a SQL logical operator relating it to the next item ("and", "or" - defaults to "and")
        2. *value*:
            - a scalar, or a list, or an expression possibly containing other attribute(s)

    :param query_stmt: the query statement to add to
    :param where_clause: optional criteria for tuple selection (ignored if *query_stmt* contains a *WHERE* clause)
    :param where_vals: the bind values list to add to
    :param where_data: the search criteria specified as key-value pairs
    :param orderby_clause: optional retrieval order (ignored if *query_stmt* contains an *ORDER BY* clause)
    :param engine: the reference database engine
    :return: the modified query statement and bind values list
    """
    # use 'WHERE' as found in 'stmt' (defaults to 'WHERE')
    pos: int = query_stmt.lower().find(" where ")
    if pos > 0:
        where_tag: str = query_stmt[pos + 1:pos + 6]
        where_clause = None
    else:
        where_tag = "WHERE"

    # extract 'ORDER BY' clause
    pos = query_stmt.lower().find(" order by ")
    if pos > 0:
        orderby_clause = query_stmt[pos+1:]
        query_stmt = query_stmt[:pos]
    elif orderby_clause:
        orderby_clause = f"ORDER BY {orderby_clause}"

    # extract 'GROUP BY' clause
    group_by: str | None = None
    pos = query_stmt.lower().find(" group by ")
    if pos > 0:
        group_by = query_stmt[pos+1:]
        query_stmt = query_stmt[:pos]

    # add 'WHERE' clause
    if where_clause:
        query_stmt += f" {where_tag} {where_clause}"

    # process the search parameters
    if where_data:
        if where_vals:
            where_vals = list(where_vals)
        else:
            where_vals = []

        if where_tag in query_stmt:
            query_stmt = query_stmt.replace(f"{where_tag} ", f"{where_tag} (") + ") AND "
        else:
            query_stmt += f" {where_tag} "

        # process key/value pairs in 'where_data'
        for key, value in where_data.items():

            # set comparison and logical operators to their defaults
            op: str = "="
            con: str = "AND"

            # normalize 'key', 'value', 'op', 'con'
            if isinstance(key, list | tuple):
                # make sure 'key' is a list
                key = list(key)
                if len(key) > 1 and key[-1] in ["and", "or"]:
                    # extract logical operator from 'value'
                    con = key[-1].upper()
                    key = key[:-1]
                if len(key) > 1 and key[-1] in ["=", ">", "<", ">=", "<=", "<>", "in", "like", "between"]:
                    # set comparison operator
                    op = key[1].upper()
                # revert 'key' to scalar
                key = key[0]
            if isinstance(value, list | tuple):
                # make sure 'value' is a list
                value = list(value)
                if len(value) < 2:
                    # revert 'value' to scalar (breaks if 'value' is an empty list)
                    value = value[0]
                elif op == "=":
                    # implicit IN operator
                    op = "IN"
            if op in ["BETWEEN", "IN"] and not isinstance(value, list):
                # revert 'op' to simple equality
                op = "="

            # process the selection criteria
            if op == "BETWEEN":
                query_stmt += f"({key} BETWEEN {DB_BIND_META_TAG} AND {DB_BIND_META_TAG}) {con} "
                where_vals.append(value[0])
                where_vals.append(value[1])
            elif op == "IN":
                if engine == DbEngine.POSTGRES:
                    query_stmt += f"{key} IN {DB_BIND_META_TAG} {con} "
                    where_vals.append(tuple(value))
                else:
                    query_stmt += f"{key} IN (" + f"{DB_BIND_META_TAG}, " * len(value)
                    query_stmt = f"{query_stmt[:-2]}) {con} "
                    where_vals.extend(value)
            else:
                query_stmt += f"{key} {op} {DB_BIND_META_TAG} {con} "
                where_vals.append(value)

        # remove the dangling logical operator
        query_stmt = query_stmt[:-5] if query_stmt.endswith(" AND ") else query_stmt[:-4]

        # set 'WHERE' values back to tuple
        where_vals = tuple(where_vals)

    # put back 'GROUP BY' clause
    if group_by:
        query_stmt = f"{query_stmt} {group_by}"

    # put back 'ORDER BY' clause
    if orderby_clause:
        query_stmt = f"{query_stmt} {orderby_clause}"

    return query_stmt, where_vals


def _combine_update_data(update_stmt: str,
                         update_vals: tuple,
                         update_data: dict[str, Any]) -> tuple[str, tuple]:
    """
    Rebuild the update statement *update_stmt* and the list of bind values *update_vals*.

    This is done by adding to them the data specified by the key-value pairs in *update_data*.

    :param update_stmt: the update statement to add to
    :param update_vals: the update values list to add to
    :param update_data: the update data specified as key-value pairs
    :return: the modified update statement and bind values list
    """
    # extract 'WHERE' clause
    where_clause: str | None = None
    if " where " in update_stmt.lower():
        pos = update_stmt.lower().index(" where ")
        where_clause = update_stmt[pos + 1:]
        update_stmt = update_stmt[:pos]

    # account for existence of 'SET' keyword
    if " set " in update_stmt.lower():
        update_stmt += ", "
    else:
        update_stmt += " SET "

    # add the key-value pairs
    update_stmt += f" = {DB_BIND_META_TAG}, ".join(update_data.keys()) + f" = {DB_BIND_META_TAG}"
    if update_vals:
        update_vals += tuple(update_data.values())
    else:
        update_vals = tuple(update_data.values())

    # put back 'WHERE' clause
    if where_clause:
        update_stmt = f"{update_stmt} {where_clause}"

    return update_stmt, update_vals


def _combine_insert_data(insert_stmt: str,
                         insert_vals: tuple,
                         insert_data: dict[str, Any]) -> tuple[str, tuple]:
    """
    Rebuild the insert statement *insert_stmt* and the list of bind values *insert_vals*.

    This is done by adding to them the data specified by the key-value pairs in *insert_data*.

    :param insert_stmt: the insert statement to add to
    :param insert_vals: the insert values list to add to
    :param insert_data: the insert data specified as key-value pairs
    :return: the modified insert statement and bind values list
    """
    # handle the 'VALUES' clause
    if " values(" in insert_stmt.lower():
        pos = insert_stmt.lower().index(" values(")
        values_clause: str = insert_stmt[pos:].rstrip()[:-1]
        insert_stmt = insert_stmt[:pos].rstrip()[:-1]
    else:
        values_clause: str = " VALUES("
        insert_stmt += " ("
        insert_vals = ()

    # add the key-value pairs
    insert_stmt += (f"{', '.join(insert_data.keys())})" +
                    values_clause + f"{DB_BIND_META_TAG}, " * len(insert_data))[:-2] + ")"
    insert_vals += insert_vals + tuple(insert_data.values())

    return insert_stmt, insert_vals


def _remove_ctrlchars(rows: list[tuple]) -> None:
    """
    Remove all occurrences of control characters from *str* elements of rows in *rows*.

    Only the elements of type *str* are inspected. Control characters are characters in the
    ASCII range [0 - 31] less:
      - *HT*: Hotizontal Tab (09)
      - *LF*: Line Feed (10)
      - *VT*: Vertical Tab (11)
      - *FF*: Form Feed (12)
      - *CR*  Carriage Return(13)

    :param rows: the rows to be cleaned
    """
    # traverse the rows
    last: int = len(rows) - 1
    for inx, row in enumerate(reversed(rows)):
        new_row: list = []
        # traverse the row
        cleaned: bool = False
        for val in row:
            if isinstance(val, str) and any(ord(ch) < 32 and ord(ch) not in range(9, 14)for ch in val):
                # 'val' contains control characters, clean it up
                new_row.append("".join(ch if ord(ch) > 31 or ord(ch) in range(9, 14) else " " for ch in val))
                cleaned = True
            else:
                new_row.append(val)
        if cleaned:
            rows[last - inx] = tuple(new_row)
