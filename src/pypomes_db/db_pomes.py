from logging import Logger
from pathlib import Path
from pypomes_core import str_sanitize
from typing import Any, Literal, BinaryIO

from .db_common import (
    DB_BIND_META_TAG, _DB_ENGINES, _DB_CONN_DATA,
    _assert_engine, _get_param,
    _combine_insert_data, _combine_update_data, _combine_search_data
)


def db_setup(engine: Literal["mysql", "oracle", "postgres", "sqlserver"],
             db_name: str,
             db_user: str,
             db_pwd: str,
             db_host: str,
             db_port: int,
             db_client: str | Path = None,
             db_driver: str = None) -> bool:
    """
    Establish the provided parameters for access to *engine*.

    The meaning of some parameters may vary between different database engines.
    All parameters, are required, with these exceptions:
        - *db_client* may be provided for *oracle*, only
        - *db_driver* is required for *sqlserver*, only

    :param engine: the database engine (one of ['mysql', 'oracle', 'postgres', 'sqlserver'])
    :param db_name: the database or service name
    :param db_user: the logon user
    :param db_pwd: the logon password
    :param db_host: the host URL
    :param db_port: the connection port (a positive integer)
    :param db_driver: the database driver (SQLServer only)
    :param db_client: the path to the client software (optional, Oracle only)
    :return: 'True' if the data was accepted, 'False' otherwise
    """
    # initialize the return variable
    result: bool = False

    # are the parameters compliant ?
    if (engine in ["mysql", "oracle", "postgres", "sqlserver"] and
        db_name and db_user and db_pwd and db_host and
        not (engine != "oracle" and db_client) and
        not (engine != "sqlserver" and db_driver) and
        not (engine == "sqlserver" and not db_driver) and
        isinstance(db_port, int) and db_port > 0):
        _DB_CONN_DATA[engine] = {
            "name": db_name,
            "user": db_user,
            "pwd": db_pwd,
            "host": db_host,
            "port": db_port
        }
        if engine == "oracle":
            _DB_CONN_DATA[engine]["client"] = Path(db_client)
        elif engine == "sqlserver":
            _DB_CONN_DATA[engine]["driver"] = db_driver
        if engine not in _DB_ENGINES:
            _DB_ENGINES.append(engine)
        result = True

    return result


def db_get_engines() -> list[str]:
    """
    Retrieve and return the list of configured engines.

    This list may include any of the supported engines:
    *mysql*, *oracle*, *postgres*, *sqlserver*.

    :return: the list of configured engines
    """
    # SANITY-CHECK: return a cloned 'list'
    return _DB_ENGINES.copy()


def db_get_param(key: Literal["name", "user", "pwd", "host", "port", "client", "driver"],
                 engine: str = None) -> Any:
    """
    Return the connection parameter value for *key*.

    The connection key should be one of *name*, *user*, *pwd*, *host*, and *port*.
    For *oracle* and *sqlserver* engines, the extra keys *client* and *driver*
    might be used, respectively.

    :param key: the reference parameter
    :param engine: the reference database engine (the default engine, if not provided)
    :return: the current value of the connection parameter
    """
    # determine the database engine
    curr_engine: str = _DB_ENGINES[0] if not engine and _DB_ENGINES else engine

    return _get_param(curr_engine, key)


def db_get_params(engine: str = None) -> dict[str, Any]:
    """
    Return the connection parameters as a *dict*.

    The returned *dict* contains the keys *name*, *user*, *pwd*, *host*, and *port*.
    For *oracle* engines, the returned *dict* contains the extra key *client*.
    For *sqlserver* engines, the returned *dict* contains the extra key *driver*.
    The meaning of these parameters may vary between different database engines.
    Note that the value of the *pwd* parameter is not returned.

    :param engine: the reference database engine (the default engine, if not provided)
    :return: the current connection parameters for the engine
    """
    # determine the database engine
    curr_engine: str = _DB_ENGINES[0] if not engine and _DB_ENGINES else engine

    # SANITY-CHECK: return a cloned 'dict'
    return dict(_DB_CONN_DATA.get(curr_engine))


def db_get_connection_string(engine: str = None) -> str:
    """
    Build and return the connection string for connecting to the database.

    :param engine: the reference database engine (the default engine, if not provided)
    :return: the connection string
    """
    # initialize the return variable
    result: Any = None

    # determine the database engine
    curr_engine: str = _DB_ENGINES[0] if not engine and _DB_ENGINES else engine

    if curr_engine == "mysql":
        pass
    elif curr_engine == "oracle":
        from . import oracle_pomes
        result = oracle_pomes.get_connection_string()
    elif curr_engine == "postgres":
        from . import postgres_pomes
        result = postgres_pomes.get_connection_string()
    elif curr_engine == "sqlserver":
        from . import sqlserver_pomes
        result = sqlserver_pomes.get_connection_string()

    return result


def db_bind_stmt(stmt: str,
                 engine: str = None) -> str:
    """
    Replace the occurrences of bind meta-tag in *stmt*, with the appropriate bind tag for *engine*.

    The bind meta-tag is defined by *DB_BIND_META_TAG*, an environment variable with the default value *%?*.

    :param stmt: the statement for which to replace the bind meta-tags with the proper bind tags
    :param engine: the reference database engine (the default engine, if not provided)
    :return: the statement with the proper bind tags, or 'None' if the engine is not known
    """
    # initialize the return variable
    result: str | None = None

    # determine the database engine
    curr_engine: str = _DB_ENGINES[0] if not engine and _DB_ENGINES else engine

    match curr_engine:
        case "mysql" | "postgres":
            result = stmt.replace(DB_BIND_META_TAG, "%s")
        case "oracle":
            pos: int = 0
            while result != stmt:
                pos += 1
                result = stmt.replace(DB_BIND_META_TAG, f":{pos}", 1)
        case "sqlserver":
            result = stmt.replace(DB_BIND_META_TAG, "?")

    return result


def db_assert_connection(errors: list[str] | None,
                         engine: str = None,
                         logger: Logger = None) -> bool:
    """
    Determine whether the *engine*'s current configuration allows for a safe connection.

    :param errors: incidental errors
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param logger: optional logger
    :return: 'True' if the trial connection succeeded, 'False' otherwise
    """
    # initialize the return variable
    result: bool = False

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(errors=op_errors,
                                      engine=engine)
    proceed: bool = True
    if curr_engine == "oracle":
        from . import oracle_pomes
        proceed = oracle_pomes.initialize(errors=op_errors,
                                          logger=logger)
    if proceed:
        conn: Any = db_connect(errors=op_errors,
                               engine=curr_engine,
                               logger=logger)
        if conn:
            conn.close()
            result = True

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_connect(errors: list[str] | None,
               autocommit: bool = False,
               engine: str = None,
               logger: Logger = None) -> Any:
    """
    Obtain and return a connection to the database, or *None* if the connection cannot be obtained.

    The target database engine, specified or default, must have been previously configured.

    :param errors: incidental error messages
    :param autocommit: whether the connection is to be in autocommit mode (defaults to False)
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param logger: optional logger
    :return: the connection to the database
    """
    # initialize the return variable
    result: Any = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(errors=op_errors,
                                      engine=engine)
    if curr_engine == "mysql":
        pass
    elif curr_engine == "oracle":
        from . import oracle_pomes
        result = oracle_pomes.connect(errors=op_errors,
                                      autocommit=autocommit,
                                      logger=logger)
    elif curr_engine == "postgres":
        from . import postgres_pomes
        result = postgres_pomes.connect(errors=op_errors,
                                        autocommit=autocommit,
                                        logger=logger)
    elif curr_engine == "sqlserver":
        from . import sqlserver_pomes
        result = sqlserver_pomes.connect(errors=op_errors,
                                         autocommit=autocommit,
                                         logger=logger)
    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_commit(errors: list[str] | None,
              connection: Any,
              logger: Logger = None) -> None:
    """
    Commit the current transaction on *connection*.

    :param errors: incidental error messages
    :param connection: the reference database connection
    :param logger: optional logger
    """
    # commit the transaction
    try:
        connection.commit()
    except Exception as e:
        err_msg: str = (f"Error committing the transaction on '{connection}': "
                        f"{str_sanitize(f'{e}')}")
        if logger:
            logger.error(msg=err_msg)
        if isinstance(errors, list):
            errors.append(err_msg)

    if logger:
        logger.debug(f"Transaction committed on {connection}")


def db_rollback(errors: list[str] | None,
                connection: Any,
                logger: Logger = None) -> None:
    """
    Rollback the current transaction on *connection*.

    :param errors: incidental error messages
    :param connection: the reference database connection
    :param logger: optional logger
    """
    # rollback the transaction
    try:
        connection.rollback()
    except Exception as e:
        err_msg: str = (f"Error rolling back the transaction on '{connection}': "
                        f"{str_sanitize(f'{e}')}")
        if logger:
            logger.error(msg=err_msg)
        if isinstance(errors, list):
            errors.append(err_msg)

    if logger:
        logger.debug(f"Transaction rolled back on {connection}")


def db_count(errors: list[str] | None,
             table: str,
             where_data: dict[str, Any] = None,
             engine: str = None,
             connection: Any = None,
             committable: bool = None,
             logger: Logger = None) -> int:
    """
    Obtain and return the number of tuples in *table* meeting the criteria defined in *where_data*.

    The attributes and corresponding values for the query's WHERE clause are held in *where_data*.
    If more than one, the attributes are concatenated by the *AND* logical connector.
    The targer database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param table: the table to be searched
    :param where_data: the search criteria specified as key-value pairs
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: 'True' if at least one tuple was found, 'False' otherwise, 'None' if an error ocurred
    """
    # initialize the return variable
    result: int | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    sel_stmt: str = f"SELECT COUNT(*) FROM {table}"
    recs: list[tuple[int]] = db_select(errors=op_errors,
                                       sel_stmt=sel_stmt,
                                       where_data=where_data,
                                       engine=engine,
                                       connection=connection,
                                       committable=committable,
                                       logger=logger)
    if not op_errors:
        result = recs[0][0]
    elif isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_exists(errors: list[str] | None,
              table: str,
              where_data: dict[str, Any] = None,
              engine: str = None,
              connection: Any = None,
              committable: bool = None,
              logger: Logger = None) -> bool:
    """
    Determine whether at least one tuple in *table* meets the criteria defined in *where_data*.

    The attributes and corresponding values for the query's WHERE clause are held in *where_data*.
    If more than one, the attributes are concatenated by the *AND* logical connector.
    The targer database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param table: the table to be searched
    :param where_data: the search criteria specified as key-value pairs
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: 'True' if at least one tuple was found, 'False' otherwise, 'None' if an error ocurred
    """
    # initialize the return variable
    result: bool | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # noinspection PyDataSource
    sel_stmt: str = f"SELECT * FROM {table}"
    recs: list[tuple] = db_select(errors=op_errors,
                                  sel_stmt=sel_stmt,
                                  where_data=where_data,
                                  max_count=1,
                                  engine = engine,
                                  connection=connection,
                                  committable=committable,
                                  logger=logger)
    if not op_errors:
        result = recs is not None and len(recs) > 0
    elif isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_select(errors: list[str] | None,
              sel_stmt: str,
              where_vals: tuple = None,
              where_data: dict[str, Any] = None,
              min_count: int = None,
              max_count: int = None,
              require_count: int = None,
              engine: str = None,
              connection: Any = None,
              committable: bool = None,
              logger: Logger = None) -> list[tuple]:
    """
    Search the database and return all tuples that satisfy the *sel_stmt* search command.

    The command can optionally contain search criteria, with respective values given in *where_vals*,
    or specified additionally by key-value pairs in *where_data*.
    For PostgreSQL, the list of values for an attribute with the *IN* clause must be contained in a
    specific tuple. If not positive integers, *min_count*, *max_count*, and *require_count* are ignored.
    If *require_count* is specified, then exactly that number of tuples must be
    returned by the query. If the search is empty, an empty list is returned.
    The target database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param sel_stmt: SELECT command for the search
    :param where_vals: values to be associated with the search criteria
    :param where_data: search criteria specified as key-value pairs
    :param min_count: optionally defines the minimum number of tuples to be returned
    :param max_count: optionally defines the maximum number of tuples to be returned
    :param require_count: number of tuples that must exactly satisfy the query (overrides 'min_count' and 'max_count')
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: list of tuples containing the search result, '[]' if the search was empty, or 'None' if there was an error
    """
    # initialize the return variable
    result: list[tuple] | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(errors=op_errors,
                                      engine=engine)

    # process search data provided as key-value pairs
    if where_data:
        sel_stmt, where_vals = _combine_search_data(query_stmt=sel_stmt,
                                                    where_vals=where_vals,
                                                    where_data=where_data,
                                                    engine=curr_engine)
    # establish the correct bind tags
    if where_vals and DB_BIND_META_TAG in sel_stmt:
        sel_stmt = db_bind_stmt(stmt=sel_stmt,
                                engine=curr_engine)

    if curr_engine == "mysql":
        pass
    elif curr_engine == "oracle":
        from . import oracle_pomes
        result = oracle_pomes.select(errors=op_errors,
                                     sel_stmt=sel_stmt,
                                     where_vals=where_vals,
                                     min_count=min_count,
                                     max_count=max_count,
                                     require_count=require_count,
                                     conn=connection,
                                     committable=committable,
                                     logger=logger)
    elif curr_engine == "postgres":
        from . import postgres_pomes
        result = postgres_pomes.select(errors=op_errors,
                                       sel_stmt=sel_stmt,
                                       where_vals=where_vals,
                                       min_count=min_count,
                                       max_count=max_count,
                                       require_count=require_count,
                                       conn=connection,
                                       committable=committable,
                                       logger=logger)
    elif curr_engine == "sqlserver":
        from . import sqlserver_pomes
        result = sqlserver_pomes.select(errors=op_errors,
                                        sel_stmt=sel_stmt,
                                        where_vals=where_vals,
                                        min_count=min_count,
                                        max_count=max_count,
                                        require_count=require_count,
                                        conn=connection,
                                        committable=committable,
                                        logger=logger)
    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_insert(errors: list[str] | None,
              insert_stmt: str,
              insert_vals: tuple = None,
              insert_data: dict[str, Any] = None,
              engine: str = None,
              connection: Any = None,
              committable: bool = None,
              logger: Logger = None) -> int:
    """
    Insert a tuple, with values defined in *insert_vals*, into the database.

    The target database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param insert_stmt: the INSERT command
    :param insert_vals: values to be inserted
    :param insert_data: data to be inserted as key-value pairs
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: the number of inserted tuples (0 ou 1), or 'None' if an error occurred
    """
    # initialize the local errors list
    op_errors: list[str] = []

    # process insert data provided as key-value pairs
    if insert_data:
        insert_stmt, insert_vals = _combine_insert_data(insert_stmt=insert_stmt,
                                                        insert_vals=insert_vals,
                                                        insert_data=insert_data)
    result: int = db_execute(errors=op_errors,
                             exc_stmt=insert_stmt,
                             bind_vals=insert_vals,
                             engine=engine,
                             connection=connection,
                             committable=committable,
                             logger=logger)
    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_update(errors: list[str] | None,
              update_stmt: str,
              update_vals: tuple = None,
              update_data: dict[str, Any] = None,
              where_vals: tuple = None,
              where_data: dict[str, Any] = None,
              engine: str = None,
              connection: Any = None,
              committable: bool = None,
              logger: Logger = None) -> int:
    """
    Update one or more tuples in the database, as defined by the command *update_stmt*.

    The values for this update are in *update_vals*, and/or specified by key-value pairs in *update_data*.
    The values for selecting the tuples to be updated are in *where_vals*, and/ar specified
    by key-value pairs in *where_data*. The target database engine, specified or default,
    must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param update_stmt: the UPDATE command
    :param update_vals: values for the update operation
    :param update_data: update data as key-value pairs
    :param where_vals: values to be associated with the search criteria
    :param where_data: search criteria as key-value pairs
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: the number of updated tuples, or 'None' if an error occurred
    """
    # initialize the local errors list
    op_errors: list[str] = []

    # process update data provided as key-value pairs
    if update_data:
        update_stmt, update_vals = _combine_update_data(update_stmt=update_stmt,
                                                        update_vals=update_vals,
                                                        update_data=update_data)
    # process search data provided as key-value pairs
    if where_data:
        curr_engine: str = _assert_engine(errors=[],
                                          engine=engine)
        update_stmt, where_vals = _combine_search_data(query_stmt=update_stmt,
                                                       where_vals=where_vals,
                                                       where_data=where_data,
                                                       engine=curr_engine)
    # combine 'update' and 'where' bind values
    bind_vals: tuple | None = None
    if update_vals and where_vals:
        bind_vals = update_vals + where_vals
    elif update_vals:
        bind_vals = update_vals
    elif where_vals:
        bind_vals = where_vals

    result: int = db_execute(errors=op_errors,
                             exc_stmt=update_stmt,
                             bind_vals=bind_vals,
                             engine=engine,
                             connection=connection,
                             committable=committable,
                             logger=logger)
    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_delete(errors: list[str] | None,
              delete_stmt: str,
              where_vals: tuple = None,
              where_data: dict[str, Any] = None,
              engine: str = None,
              connection: Any = None,
              committable: bool = None,
              logger: Logger = None) -> int:
    """
    Delete one or more tuples in the database, as defined by the *delete_stmt* command.

    The values for selecting the tuples to be deleted are in *where_vals*, or specified additionally
    by key-value pairs in *where_data*.
    The target database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param delete_stmt: the DELETE command
    :param where_vals: values to be associated with the search criteria
    :param where_data: search criteria as key-value pairs
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: the number of deleted tuples, or 'None' if an error occurred
    """
    # initialize the local errors list
    op_errors: list[str] = []

    # process search data provided as key-value pairs
    if where_data:
        curr_engine: str = _assert_engine(errors=[],
                                          engine=engine)
        delete_stmt, where_vals = _combine_search_data(query_stmt=delete_stmt,
                                                       where_vals=where_vals,
                                                       where_data=where_data,
                                                       engine=curr_engine)
    result: int = db_execute(errors=op_errors,
                             exc_stmt=delete_stmt,
                             bind_vals=where_vals,
                             engine=engine,
                             connection=connection,
                             committable=committable,
                             logger=logger)
    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_bulk_insert(errors: list[str] | None,
                   insert_stmt: str,
                   insert_vals: list[tuple],
                   engine: str = None,
                   connection: Any = None,
                   committable: bool = None,
                   logger: Logger = None) -> int:
    """
    Insert the tuples, with values defined in *insert_vals*, into the database.

    The binding is done by position. Thus, the *VALUES* clause in *insert_stmt* must contain
    as many placeholders as there are elements in the tuples found in the list provided in
    *insert_vals*. This is applicable for *mysql*, *oracle*, and *sqlserver*, where the
    placeholders are '%VARn%, ':n', and '?', respectively, and 'n' is the 1-based position of the
    data in the tuple. In the specific case of *postgres*, the *VALUES* clause must be simply *VALUES %s*.
    The target database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param insert_stmt: the INSERT command
    :param insert_vals: the list of values to be inserted
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: the number of inserted tuples (1 for postgres), or 'None' if an error occurred
    """
    # initialize the return variable
    result: int | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(errors=op_errors,
                                      engine=engine)
    # establish the correct bind tags
    if insert_vals and DB_BIND_META_TAG in insert_stmt:
        insert_stmt = db_bind_stmt(stmt=insert_stmt,
                                   engine=curr_engine)
    if curr_engine == "mysql":
        pass
    elif curr_engine == "oracle":
        from . import oracle_pomes
        result = oracle_pomes.bulk_execute(errors=op_errors,
                                           exc_stmt=insert_stmt,
                                           exc_vals=insert_vals,
                                           conn=connection,
                                           committable=committable,
                                           logger=logger)
    elif curr_engine == "postgres":
        from . import postgres_pomes
        result = postgres_pomes.bulk_execute(errors=op_errors,
                                             exc_stmt=insert_stmt,
                                             exc_vals=insert_vals,
                                             conn=connection,
                                             committable=committable,
                                             logger=logger)
    elif curr_engine == "sqlserver":
        from . import sqlserver_pomes
        result = sqlserver_pomes.bulk_execute(errors=op_errors,
                                              exc_stmt=insert_stmt,
                                              exc_vals=insert_vals,
                                              conn=connection,
                                              committable=committable,
                                              logger=logger)
    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_bulk_update(errors: list[str] | None,
                   update_stmt: str,
                   update_vals: list[tuple],
                   engine: str = None,
                   connection: Any = None,
                   committable: bool = None,
                   logger: Logger = None) -> int:
    """
    Update the tuples, with values defined in *update_vals*, in the database.

    The binding is done by position. Thus, the binding clauses in *update_stmt* must contain as many
    placeholders as there are elements in the tuples found in the list provided in *update_vals*.
    This is applicable for *mysql*, *oracle*, and *sqlserver*, where the placeholders are
    '%', ':n', and '?', respectively, and 'n' is the 1-based position of the data in the tuple.
    Note that the placeholders in the *WHERE* clause will follow the ones in the *SET* clause.
    In the specific case of *postgres*, a unique syntax applies. The *VALUES* clause must be simply
    *VALUES %s*, and it is used in both *INSERT* and *UPDATE* operations. In the latter case,
    there is also a *FROM* clause to go along with it.
    The target database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param update_stmt: the UPDATE command
    :param update_vals: the list of values to update the database with
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: the number of updated tuples, or 'None' if an error occurred
    """
    # initialize the return variable
    result: int | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(errors=op_errors,
                                      engine=engine)
    # establish the correct bind tags
    if update_vals and DB_BIND_META_TAG in update_stmt:
        update_stmt = db_bind_stmt(stmt=update_stmt,
                                   engine=curr_engine)

    if curr_engine == "mysql":
        pass
    elif curr_engine == "oracle":
        from . import oracle_pomes
        result = oracle_pomes.bulk_execute(errors=op_errors,
                                           exc_stmt=update_stmt,
                                           exc_vals=update_vals,
                                           conn=connection,
                                           committable=committable,
                                           logger=logger)
    elif curr_engine == "postgres":
        from . import postgres_pomes
        result = postgres_pomes.bulk_execute(errors=op_errors,
                                             exc_stmt=update_stmt,
                                             exc_vals=update_vals,
                                             conn=connection,
                                             committable=committable,
                                             logger=logger)
    elif curr_engine == "sqlserver":
        from . import sqlserver_pomes
        result = sqlserver_pomes.bulk_execute(errors=op_errors,
                                              exc_stmt=update_stmt,
                                              exc_vals=update_vals,
                                              conn=connection,
                                              committable=committable,
                                              logger=logger)
    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_update_lob(errors: list[str] | None,
                  lob_table: str,
                  lob_column: str,
                  pk_columns: list[str],
                  pk_vals: tuple,
                  lob_data: bytes | str | Path | BinaryIO,
                  chunk_size: int,
                  engine: str = None,
                  connection: Any = None,
                  committable: bool = None,
                  logger: Logger = None) -> None:
    """
    Update a large binary object (LOB) in the given table and column.

    The data for the update may come from *bytes*, from a *Path* or its string representation,
    or from a pointer obtained from *BytesIO* or *Path.open()* in binary mode.
    The target database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param lob_table: the table to be update with the new LOB
    :param lob_column: the column to be updated with the new LOB
    :param pk_columns: columns making up a primary key, or a unique identifier for the tuple
    :param pk_vals: values with which to locate the tuple to be updated
    :param lob_data: the LOB data (bytes, a file path, or a file pointer)
    :param chunk_size: size in bytes of the data chunk to read/write, or 0 or None for no limit
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: number of LOBs effectively copied, or 'None' if an error occurred
    """
    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(errors=op_errors,
                                      engine=engine)
    if curr_engine == "mysql":
        pass
    elif curr_engine == "oracle":
        from . import oracle_pomes
        oracle_pomes.update_lob(errors=op_errors,
                                lob_table=lob_table,
                                lob_column=lob_column,
                                pk_columns=pk_columns,
                                pk_vals=pk_vals,
                                lob_data=lob_data,
                                chunk_size=chunk_size,
                                conn=connection,
                                committable=committable,
                                logger=logger)
    elif curr_engine == "postgres":
        from . import postgres_pomes
        postgres_pomes.update_lob(errors=op_errors,
                                  lob_table=lob_table,
                                  lob_column=lob_column,
                                  pk_columns=pk_columns,
                                  pk_vals=pk_vals,
                                  lob_data=lob_data,
                                  chunk_size=chunk_size,
                                  conn=connection,
                                  committable=committable,
                                  logger=logger)
    elif curr_engine == "sqlserver":
        from . import sqlserver_pomes
        sqlserver_pomes.update_lob(errors=op_errors,
                                   lob_table=lob_table,
                                   lob_column=lob_column,
                                   pk_columns=pk_columns,
                                   pk_vals=pk_vals,
                                   lob_data=lob_data,
                                   chunk_size=chunk_size,
                                   conn=connection,
                                   committable=committable,
                                   logger=logger)
    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)


def db_execute(errors: list[str] | None,
               exc_stmt: str,
               bind_vals: tuple = None,
               engine: str = None,
               connection: Any = None,
               committable: bool = None,
               logger: Logger = None) -> int:
    """
    Execute the command *exc_stmt* on the database.

    This command might be a DML ccommand modifying the database, such as
    inserting, updating or deleting tuples, or it might be a DDL statement,
    or it might even be an environment-related command.
    The optional bind values for this operation are in *bind_vals*.
    The target database engine, specified or default, must have been previously configured.
    The value returned is the value obtained from the execution of *exc_stmt*.
    It might be the number of inserted, modified, or deleted tuples,
    ou None if an error occurred.

    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param exc_stmt: the command to execute
    :param bind_vals: optional bind values
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: the return value from the command execution, or 'None' if an error occurred
    """
    # initialize the return variable
    result: int | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(errors=op_errors,
                                      engine=engine)
    # establish the correct bind tags
    if bind_vals and DB_BIND_META_TAG in exc_stmt:
        exc_stmt = db_bind_stmt(stmt=exc_stmt,
                                engine=curr_engine)
    if curr_engine == "mysql":
        pass
    elif curr_engine == "oracle":
        from . import oracle_pomes
        result = oracle_pomes.execute(errors=op_errors,
                                      exc_stmt=exc_stmt,
                                      bind_vals=bind_vals,
                                      conn=connection,
                                      committable=committable,
                                      logger=logger)
    elif curr_engine == "postgres":
        from . import postgres_pomes
        result = postgres_pomes.execute(errors=op_errors,
                                        exc_stmt=exc_stmt,
                                        bind_vals=bind_vals,
                                        conn=connection,
                                        committable=committable,
                                        logger=logger)
    elif curr_engine == "sqlserver":
        from . import sqlserver_pomes
        result = sqlserver_pomes.execute(errors=op_errors,
                                         exc_stmt=exc_stmt,
                                         bind_vals=bind_vals,
                                         conn=connection,
                                         committable=committable,
                                         logger=logger)
    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_call_function(errors: list[str] | None,
                     func_name: str,
                     func_vals: tuple = None,
                     engine: str = None,
                     connection: Any = None,
                     committable: bool = None,
                     logger: Logger = None) -> list[tuple]:
    """
    Execute the stored function *func_name* in the database, with the parameters given in *func_vals*.

    The target database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param func_name: name of the stored function
    :param func_vals: parameters for the stored function
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: the data returned by the function, or 'None' if an error occurred
    """
    # initialize the return variable
    result: Any = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(errors=op_errors,
                                      engine=engine)
    if curr_engine == "mysql":
        pass
    elif curr_engine == "oracle":
        from . import oracle_pomes
        result = oracle_pomes.call_function(errors=op_errors,
                                            func_name=func_name,
                                            func_vals=func_vals,
                                            conn=connection,
                                            committable=committable,
                                            logger=logger)
    elif curr_engine == "postgres":
        from . import postgres_pomes
        result = postgres_pomes.call_procedure(errors=op_errors,
                                               proc_name=func_name,
                                               proc_vals=func_vals,
                                               conn=connection,
                                               committable=committable,
                                               logger=logger)
    elif curr_engine == "sqlserver":
        from . import sqlserver_pomes
        result = sqlserver_pomes.call_procedure(errors=op_errors,
                                                proc_name=func_name,
                                                proc_vals=func_vals,
                                                conn=connection,
                                                committable=committable,
                                                logger=logger)
    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_call_procedure(errors: list[str] | None,
                      proc_name: str,
                      proc_vals: tuple = None,
                      engine: str = None,
                      connection: Any = None,
                      committable: bool = None,
                      logger: Logger = None) -> list[tuple]:
    """
    Execute the stored procedure *proc_name* in the database, with the parameters given in *proc_vals*.

    The target database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param proc_name: name of the stored procedure
    :param proc_vals: parameters for the stored procedure
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: the data returned by the procedure, or 'None' if an error occurred
    """
    # initialize the return variable
    result: Any = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(errors=op_errors,
                                      engine=engine)
    if curr_engine == "mysql":
        pass
    elif curr_engine == "oracle":
        from . import oracle_pomes
        result = oracle_pomes.call_procedure(errors=op_errors,
                                             proc_name=proc_name,
                                             proc_vals=proc_vals,
                                             conn=connection,
                                             committable=committable,
                                             logger=logger)
    elif curr_engine == "postgres":
        from . import postgres_pomes
        result = postgres_pomes.call_procedure(errors=op_errors,
                                               proc_name=proc_name,
                                               proc_vals=proc_vals,
                                               conn=connection,
                                               committable=committable,
                                               logger=logger)
    elif curr_engine == "sqlserver":
        from . import sqlserver_pomes
        result = sqlserver_pomes.call_procedure(errors=op_errors,
                                                proc_name=proc_name,
                                                proc_vals=proc_vals,
                                                conn=connection,
                                                committable=committable,
                                                logger=logger)
    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result
