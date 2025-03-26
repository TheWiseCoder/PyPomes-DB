from pypomes_core import (
    APP_PREFIX,
    str_sanitize, str_positional,
    env_get_int, env_get_str, env_get_strs, env_get_path
)
from enum import StrEnum, auto
from typing import Any, Final


class DbEngine(StrEnum):
    """
    Supported database engines.
    """
    MYSQL = auto()
    ORACLE = auto()
    POSTGRES = auto()
    SQLSERVER = auto()

    def __str__(self) -> str:  # noqa: D105
        return self.name


class DbParam(StrEnum):
    """
    Parameters for connecting to database engines.
    """
    NAME = auto()
    USER = auto()
    PWD = auto()
    HOST = auto()
    PORT = auto()
    CLIENT = auto()
    DRIVER = auto()

    def __str__(self) -> str:  # noqa: D105
        return self.name


# the bind meta-tag to use in DML statements
# (guarantees cross-engine compatilitiy, as this is replaced by the engine's bind tag)
DB_BIND_META_TAG: Final[str] = env_get_str(key=f"{APP_PREFIX}_DB_BIND_META_TAG",
                                           def_value="%?")

# the preferred way to specify database connection parameters is dynamically with 'db_setup()'
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
_DB_ENGINES: list[DbEngine] = []
_engine: str = env_get_str(key=f"{APP_PREFIX}_DB_ENGINE",
                           values=list(map(str, DbEngine)))
if _engine:
    _default_setup: bool = True
    _DB_ENGINES.append(DbEngine(_engine))
else:
    _default_setup: bool = False
    _engines: list[str] = env_get_strs(key=f"{APP_PREFIX}_DB_ENGINES",
                                       values=list(map(str, DbEngine)))
    if _engines:
        _DB_ENGINES.extend([DbEngine(v) for v in _engines])
for _db_engine in _DB_ENGINES:
    if _default_setup:
        _prefix: str = "DB"
        _default_setup = False
    else:
        _prefix: str = str_positional(source=str(_db_engine),
                                      list_origin=["mysql", "oracle", "postgres", "sqlserver"],
                                      list_dest=["MSQL", "ORCL", "PG", "SQLS"])
    _DB_CONN_DATA[_db_engine] = {
        DbParam.NAME: env_get_str(key=f"{APP_PREFIX}_{_prefix}_NAME"),
        DbParam.USER: env_get_str(key=f"{APP_PREFIX}_{_prefix}_USER"),
        DbParam.PWD: env_get_str(key=f"{APP_PREFIX}_{_prefix}_PWD"),
        DbParam.HOST: env_get_str(key=f"{APP_PREFIX}_{_prefix}_HOST"),
        DbParam.PORT: env_get_int(key=f"{APP_PREFIX}_{_prefix}_PORT")
    }
    if _db_engine == DbEngine.ORACLE:
        _DB_CONN_DATA[_db_engine][DbParam.CLIENT] = env_get_path(key=f"{APP_PREFIX}_{_prefix}_CLIENT")
    elif _db_engine == DbEngine.SQLSERVER:
        _DB_CONN_DATA[_db_engine][DbParam.DRIVER] = env_get_str(key=f"{APP_PREFIX}_{_prefix}_DRIVER")


def _assert_engine(errors: list[str] | None,
                   engine: DbEngine) -> DbEngine:
    """
    Verify if *engine* is in the list of supported engines.

    If *engine* is a supported engine, it is returned. If its value is 'None',
    the first engine in the list of supported engines (the default engine) is returned.

    :param errors: incidental errors
    :param engine: the reference database engine
    :return: the validated or default engine
    """
    # initialize the return valiable
    result: DbEngine | None = None

    if not engine and _DB_ENGINES:
        result = _DB_ENGINES[0]
    elif engine in _DB_ENGINES:
        result = engine
    elif isinstance(errors, list):
        err_msg = f"Database engine '{engine}' unknown or not configured"
        errors.append(err_msg)

    return result


def _assert_query_quota(errors: list[str] | None,
                        engine: DbEngine,
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
    :param require_count: number of tuples that must exactly satisfy the query
    :return: whether or not the number of tuples returned is compliant
    """
    # initialize the control message variable
    err_msg: str | None = None

    # has an exact number of tuples been defined but not returned ?
    if isinstance(require_count, int) and \
       require_count > 0 and require_count != count:
        # yes, report the error, if applicable
        err_msg = f"{count} tuples returned, exactly {require_count} expected"

    # has a minimum number of tuples been defined but not returned ?
    elif (isinstance(min_count, int) and
          min_count > 0 and min_count > count):
        # yes, report the error, if applicable
        err_msg = f"{count} tuples returned, at least {min_count} expected'"

    # has a maximum number of tuples been defined but not complied with ?
    # INSANITY CHECK: expected to never occur
    elif isinstance(max_count, int) and 0 < max_count < count:
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


def _except_msg(exception: Exception,
                engine: DbEngine) -> str:
    """
    Format and return the error message corresponding to the exception raised while accessing the database.

    :param exception: the exception raised
    :param engine: the reference database engine
    :return: the formatted error message
    """
    db_data: dict[DbParam, Any] = _DB_CONN_DATA.get(engine) or {}
    return (f"Error accessing '{db_data.get(DbParam.NAME)}' "
            f"at '{db_data.get(DbParam.HOST)}': {str_sanitize(target_str=f'{exception}')}")


def _build_query_msg(query_stmt: str,
                     engine: DbEngine,
                     bind_vals: tuple) -> str:
    """
    Format and return the message indicative of a query problem.

    :param query_stmt: the query command
    :param engine: the reference database engine
    :param bind_vals: values associated with the query command
    :return: message indicative of empty search
    """
    result: str = str_sanitize(target_str=query_stmt)

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
                         where_data: dict[str, Any],
                         orderby_clause: str | None,
                         engine: DbEngine) -> tuple[str, tuple]:
    """
    Rebuild the query statement *query_stmt* and the list of bind values *where_vals*.

    This is done by adding to them the search criteria specified by the key-value pairs in *where_data*.

    :param query_stmt: the query statement to add to
    :param where_clause: optional criteria for tuple selection (ignored if *query_stmt* contains a *WHERE* clause)
    :param where_vals: the bind values list to add to
    :param where_data: the search criteria specified as key-value pairs
    :param orderby_clause: optional retrieval order (ignored if *query_stmt* contains a *ORDER BY* clause)
    :param engine: the reference database engine
    :return: the modified query statement and bind values list
    """
    # use 'WHERE' as found in 'stmt' (defaults to 'WHERE')
    pos: int = query_stmt.lower().find(" where ")
    if pos > 0:
        where: str = query_stmt[pos + 1:pos + 6]
        where_clause = None
    else:
        where = "WHERE"

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
        query_stmt += f" {where} {where_clause}"

    # process the search parameters
    if where_data:
        if where_vals:
            where_vals = list(where_vals)
        else:
            where_vals = []

        if where in query_stmt:
            query_stmt = query_stmt.replace(f"{where} ", f"{where} (") + ") AND "
        else:
            query_stmt += f" {where} "

        # process key-value pairs
        for key, value in where_data.items():
            if isinstance(value, list | tuple):
                if len(value) == 1:
                    where_vals.append(value[0])
                    query_stmt += f"{key} = {DB_BIND_META_TAG} AND "
                elif engine == DbEngine.POSTGRES:
                    where_vals.append(tuple(value))
                    query_stmt += f"{key} IN {DB_BIND_META_TAG} AND "
                else:
                    where_vals.extend(value)
                    query_stmt += f"{key} IN (" + f"{DB_BIND_META_TAG}, " * len(value)
                    query_stmt = f"{query_stmt[:-2]}) AND "
            else:
                where_vals.append(value)
                query_stmt += f"{key} = {DB_BIND_META_TAG} AND "
        query_stmt = query_stmt[:-5]
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

    This is done by adding to them the data specified by the key-value pairs in *update_data*.

    :param insert_stmt: the insert statement to add to
    :param insert_vals: the insert values list to add to
    :param insert_data: the insert data specified as key-value pairs
    :return: the modified insert statement and bind values list
    """
    # handle the 'VALUES' clause
    if " values(" in insert_stmt.lower():
        pos = insert_stmt.lower().index(" values(")
        values_clause: str = insert_stmt[pos + 1:].rstrip()[:-1]
        insert_stmt = insert_stmt[:pos].rstrip()[:-1]
    else:
        values_clause: str = "VALUES("
        insert_stmt += " ("
        insert_vals = ()

    # add the key-value pairs
    insert_stmt += (f"{', '.join(insert_data.keys())})" +
                    values_clause + f"{DB_BIND_META_TAG}, " * len(insert_data))[:-2] + ")"
    insert_vals += insert_vals + tuple(insert_data.values())

    return insert_stmt, insert_vals


def _remove_nulls(rows: list[tuple]) -> list[tuple]:
    """
    Remove all occurrences of *NULL* (char(0)) values from the rows in *rows*.

    :param rows: the rows to be cleaned
    :return: a row with cleaned data, or None if no cleaning was necessary
    """
    # initialize the return variable
    result: list[tuple] = []

    # traverse the rows
    for row in rows:
        cleaned_row: list[Any] = []

        # traverse the values
        for val in row:
            # is 'val' a string containing NULLs ?
            if isinstance(val, str) and val.find(chr(0)) > 0:
                # yes, clean it up
                cleaned_row.append(val.replace(chr(0), ""))
            else:
                # no, use it as is
                cleaned_row.append(val)
        result.append(tuple(cleaned_row))

    return result
