from logging import Logger
from pathlib import Path
from pypomes_core import str_sanitize
from typing import Any, BinaryIO

from .db_common import (
    DB_BIND_META_TAG, _DB_CONN_DATA, DbEngine, DbParam,
    _assert_engine, _get_param, _bind_columns, _bind_marks,
    _combine_insert_data, _combine_update_data, _combine_search_data
)
from .pool_pomes import db_pool_acquire, db_pool_release


def db_setup(engine: DbEngine,
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

    :param engine: the database engine (one of [*mysql*, *oracle*, *postgres*, *sqlserver*])
    :param db_name: the database or service name
    :param db_user: the logon user
    :param db_pwd: the logon password
    :param db_host: the host URL
    :param db_port: the connection port (a positive integer)
    :param db_driver: the database driver (SQLServer only)
    :param db_client: the path to the client software (Oracle only)
    :return: *True* if the data was accepted, *False* otherwise
    """
    # initialize the return variable
    result: bool = False

    # are the parameters compliant ?
    if engine in DbEngine and \
       db_name and db_user and db_pwd and db_host and \
       not (engine != DbEngine.ORACLE and db_client) and \
       not (engine != DbEngine.SQLSERVER and db_driver) and \
       not (engine == DbEngine.SQLSERVER and not db_driver) and \
       isinstance(db_port, int) and db_port > 0:
        _DB_CONN_DATA[engine] = {
            DbParam.ENGINE: engine.value,
            DbParam.NAME: db_name,
            DbParam.USER: db_user,
            DbParam.PWD: db_pwd,
            DbParam.HOST: db_host,
            DbParam.PORT: db_port,
            DbParam.VERSION: ""
        }
        if engine == DbEngine.ORACLE:
            _DB_CONN_DATA[engine][DbParam.CLIENT] = Path(db_client)
        elif engine == DbEngine.SQLSERVER:
            _DB_CONN_DATA[engine][DbParam.DRIVER] = db_driver
        result = True

    return result


def db_assert_access(engine: DbEngine = None,
                     errors: list[str] = None,
                     logger: Logger = None) -> bool:
    """
    Determine whether the *engine*'s current configuration allows for connections.

    This function should be invoked once, at application's initialization time. This is necessary
    to make sure connections are obtainable with the parameters specified, to establish the version
    of the database engine in use, and, if applicable in the case of *Oracle*, to initialize access
    to its client library.

    :param engine: the database engine to use (uses the default engine, if not provided)
    :param errors: incidental error messages
    :param logger: optional logger
    :return: *True* if the connection attempt succeeded, *False* otherwise
    """
    # initialize the return variable
    result: bool = False

    # initialize the local errors list
    op_errors: list[str] = []

    # assert the database engine
    engine = _assert_engine(engine=engine,
                            errors=op_errors)
    if engine:
        # determine if access to 'engine' has been asserted
        version: str = _DB_CONN_DATA[engine].get(DbParam.VERSION)
        if version:
            result = True
        else:
            # assert access to 'engine'
            if engine == DbEngine.ORACLE:
                from . import oracle_pomes
                oracle_pomes.initialize(errors=op_errors,
                                        logger=logger)
            conn: Any = db_connect(engine=engine,
                                   errors=op_errors,
                                   logger=logger)
            if conn:
                _DB_CONN_DATA[engine][DbParam.VERSION] = __get_version(engine=engine,
                                                                       connection=conn)
                db_close(connection=conn,
                         logger=logger)
                result = True

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_get_engines() -> list[DbEngine]:
    """
    Retrieve the *list* of configured engines.

    This *list* may include any of the supported engines:
    *mysql*, *oracle*, *postgres*, *sqlserver*.
    Note that the values in the returned *list* are instances of *DbEngine*, not strings.

    :return: the *list* of configured engines
    """
    return list(_DB_CONN_DATA)


def db_get_param(key: DbParam,
                 engine: DbEngine = None) -> Any:
    """
    Return the current value for connection parameter *key*.

    The *key* should be one of *name*, *user*, *pwd*, *host*, and *port*.
    For *oracle* and *sqlserver* engines, the extra keys *client* and *driver*
    may be used, respectively.

    :param key: the reference parameter
    :param engine: the reference database engine (the default engine, if not provided)
    :return: the current value of the connection parameter, or *None* if not found
    """
    # assert the database engine
    engine = next(iter(_DB_CONN_DATA)) if not engine and _DB_CONN_DATA else engine

    # return the connection parameter
    return _get_param(engine=engine,
                      param=key)


def db_get_params(engine: DbEngine = None) -> dict[DbParam, Any] | None:
    """
    Return the current connection parameters for *engine* as a *dict*.

    The returned *dict* contains the keys *name*, *user*, *pwd*, *host*, and *port*.
    For *oracle* engines, the returned *dict* contains the extra key *client*.
    For *sqlserver* engines, the returned *dict* contains the extra key *driver*.
    The meaning of these parameters may vary between different database engines.
    Note that the keys in the returned *dict* are strings, not *DbParam* instances.

    :param engine: the reference database engine (the default engine, if not provided)
    :return: the current connection parameters for *engine*, or *None* if not found
    """
    # assert the database engine
    engine = next(iter(_DB_CONN_DATA)) if not engine and _DB_CONN_DATA else engine

    # return the connection parameters
    return _DB_CONN_DATA[engine].copy() if engine in _DB_CONN_DATA else None


def db_get_connection_string(engine: DbEngine = None) -> str:
    """
    Build and return the connection string for connecting to the database.

    :param engine: the reference database engine (the default engine, if not provided)
    :return: the connection string, or *None* if error
    """
    # initialize the return variable
    result: str | None = None

    # assert the database engine
    engine = next(iter(_DB_CONN_DATA)) if not engine and _DB_CONN_DATA else engine

    if engine == DbEngine.MYSQL:
        pass
    elif engine == DbEngine.ORACLE:
        from . import oracle_pomes
        result = oracle_pomes.get_connection_string()
    elif engine == DbEngine.POSTGRES:
        from . import postgres_pomes
        result = postgres_pomes.get_connection_string()
    elif engine == DbEngine.SQLSERVER:
        from . import sqlserver_pomes
        result = sqlserver_pomes.get_connection_string()

    return result


def db_get_reserved_words(engine: DbEngine = None) -> list[str] | None:
    """
    Obtain and return the list of reserved words for *engine*.

    Reserved words can not be used to name database objects like tables, columns, triggers, constraints, etc.

    :param engine: the reference database engine (the default engine, if not provided)
    :return: the engine's list of reserved words, or *None* if error
    """
    # initialize the return variable
    result: list[str] | None = None

    # assert the database engine
    engine = next(iter(_DB_CONN_DATA)) if not engine and _DB_CONN_DATA else engine

    match engine:
        case DbEngine.MYSQL:
            from . import mysql_pomes
            result = mysql_pomes.RESERVED_WORDS
        case DbEngine.ORACLE:
            from . import oracle_pomes
            result = oracle_pomes.RESERVED_WORDS
        case DbEngine.POSTGRES:
            from . import postgres_pomes
            result = postgres_pomes.RESERVED_WORDS
        case DbEngine.SQLSERVER:
            from . import sqlserver_pomes
            result = sqlserver_pomes.RESERVED_WORDS

    return result


def db_is_reserved_word(word: str,
                        engine: DbEngine = None) -> bool | None:
    """
    Verify whether *word* is a reserved word for *engine*.

    Reserved words can not be used to name database objects like tables, columns, triggers, constraints, etc.

    :param word: the word to verify
    :param engine: the reference database engine (the default engine, if not provided)
    :return: *True* if *word* is a reserved word for *engine*, *False* otherwise, *None* on error
    """
    # initialize the return variable
    result: bool | None = None

    # assert the database engine
    engine = next(iter(_DB_CONN_DATA)) if not engine and _DB_CONN_DATA else engine

    if isinstance(word, str):
        word = word.upper()
    match engine:
        case DbEngine.MYSQL:
            from . import mysql_pomes
            result = word in mysql_pomes.RESERVED_WORDS
        case DbEngine.ORACLE:
            from . import oracle_pomes
            result = word in oracle_pomes.RESERVED_WORDS
        case DbEngine.POSTGRES:
            from . import postgres_pomes
            result = word in postgres_pomes.RESERVED_WORDS
        case DbEngine.SQLSERVER:
            from . import sqlserver_pomes
            result = word in sqlserver_pomes.RESERVED_WORDS

    return result


def db_adjust_placeholders(stmt: str,
                           engine: DbEngine = None) -> str:
    """
    Replace the occurrences of bind meta-tag in *stmt*, with the appropriate placeholder for *engine*.

    The bind meta-tag is defined by *DB_BIND_META_TAG*, an environment variable with the default value *%?*.
    These are the placeholders specific to the supported DB engines:
        - mysql:     ?
        - oracle:    :n (1-based)
        - postgres:  %s
        - sqlserver: ?

    :param stmt: the statement for which to replace the bind meta-tags with the proper placeholders
    :param engine: the reference database engine (the default engine, if not provided)
    :return: the statement with the proper placeholders, or *None* if the engine is not known
    """
    # initialize the return variable
    result: str | None = None

    # assert the database engine
    engine = next(iter(_DB_CONN_DATA)) if not engine and _DB_CONN_DATA else engine

    # adjust the placeholders
    match engine:
        case DbEngine.POSTGRES:
            result = stmt.replace(DB_BIND_META_TAG, "%s")
        case DbEngine.ORACLE:
            pos: int = 1
            result = stmt
            while result.find(f":{pos}") > 0:
                result = result.replace(DB_BIND_META_TAG, f":{pos}", 1)
                pos += 1
        case DbEngine.MYSQL | DbEngine.SQLSERVER:
            result = stmt.replace(DB_BIND_META_TAG, "?")

    return result


def db_bind_arguments(stmt: str,
                      bind_vals: list[Any],
                      engine: DbEngine = None) -> str:
    """
    Replace the placeholders in *query_stmt* with the values in *bind_vals*, and return the modified query statement.

    The placeholders in *query_stmt* can be either specific to *engine*, or the generic value set in
    DB_BIND_META_TAG (default value is '%?').
    These are the placeholders specific to the supported DB engines:
        - mysql:     ?
        - oracle:    :n (1-based)
        - postgres:  %s
        - sqlserver: ?

    Note that using a statement in a situation where values for types other than *bool*, *str*, *int*, *float*,
    *date*, or *datetime* were replaced, may bring about undesirable consequences, as the standard string
    representations for these other types would be used.

    :param stmt: the query statement
    :param bind_vals: the values to replace the placeholders with
    :param engine: the database engine to use (uses the default engine, if not provided)
    :return: the query statement with the placeholders replaced with their corresponding values
    """
    # initialize the return variable
    result: str | None = None

    # assert the database engine
    engine = _assert_engine(engine=engine)

    # establish the correct bind tags
    stmt = db_adjust_placeholders(stmt=stmt,
                                  engine=engine)
    # bind the arguments
    if engine == DbEngine.MYSQL:
        from . import mysql_pomes
        result = mysql_pomes.bind_arguments(stmt=stmt,
                                            bind_vals=bind_vals)
    elif engine == DbEngine.ORACLE:
        from . import oracle_pomes
        result = oracle_pomes.bind_arguments(stmt=stmt,
                                             bind_vals=bind_vals)
    elif engine == DbEngine.POSTGRES:
        from . import postgres_pomes
        result = postgres_pomes.bind_arguments(stmt=stmt,
                                               bind_vals=bind_vals)
    elif engine == DbEngine.SQLSERVER:
        from . import sqlserver_pomes
        result = sqlserver_pomes.bind_arguments(stmt=stmt,
                                                bind_vals=bind_vals)
    return result


def db_connect(autocommit: bool = False,
               engine: DbEngine = None,
               errors: list[str] = None,
               logger: Logger = None) -> Any:
    """
    Obtain and return a connection to the database, or *None* if the connection cannot be obtained.

    The target database engine, specified or default, must have been previously configured.
    Obtaining a connection from the connection pool is initially attempted, and if not successful,
    the native driver is summoned for the task.

    :param autocommit: whether the connection is to be in autocommit mode (defaults to False)
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param errors: incidental error messages
    :param logger: optional logger
    :return: the connection to the database
    """
    # initialize the return variable
    result: int | None = None

    # assert the database engine
    engine = _assert_engine(engine=engine,
                            errors=errors)
    # attempt to obtain a connection from the pool
    if engine:
        result: Any = db_pool_acquire(engine=engine,
                                      logger=logger)
    if not result:
        # connect to the database
        if engine == DbEngine.MYSQL:
            pass
        elif engine == DbEngine.ORACLE:
            from . import oracle_pomes
            result = oracle_pomes.connect(autocommit=autocommit,
                                          errors=errors,
                                          logger=logger)
        elif engine == DbEngine.POSTGRES:
            from . import postgres_pomes
            result = postgres_pomes.connect(autocommit=autocommit,
                                            errors=errors,
                                            logger=logger)
        elif engine == DbEngine.SQLSERVER:
            from . import sqlserver_pomes
            result = sqlserver_pomes.connect(autocommit=autocommit,
                                             errors=errors,
                                             logger=logger)
    return result


def db_commit(connection: Any,
              errors: list[str] = None,
              logger: Logger = None) -> None:
    """
    Commit the current transaction on *connection*.

    :param connection: the reference database connection
    :param errors: incidental error messages
    :param logger: optional logger
    """
    # commit the transaction
    try:
        connection.commit()
        if logger:
            logger.debug(f"Transaction committed on '{connection}'")
    except Exception as e:
        err_msg: str = (f"Error committing the transaction on '{connection}': "
                        f"{str_sanitize(source=f'{e}')}")
        if logger:
            logger.error(msg=err_msg)
        if isinstance(errors, list):
            errors.append(err_msg)


def db_rollback(connection: Any,
                logger: Logger = None) -> None:
    """
    Rollback the current transaction on *connection*.

    :param connection: the reference database connection
    :param logger: optional logger
    """
    # rollback the transaction
    try:
        connection.rollback()
        if logger:
            logger.debug(f"Transaction rolled back on '{connection}'")
    except Exception as e:
        if logger:
            logger.error(msg=f"Error rolling back the transaction on '{connection}': "
                             f"{str_sanitize(source=f'{e}')}")


def db_close(connection: Any,
             logger: Logger = None) -> None:
    """
    Close the connection given in *connection*.

    Returning *connection* to the connection pool is initially attempted, and if not successful,
    a *close()* operation is performed on the connection itself.

    :param connection: the reference database connection
    :param logger: optional logger
    """
    # attempt to return the connection to the pool
    if not db_pool_release(conn=connection,
                           logger=logger):
        # close the connection
        try:
            connection.close()
            if logger:
                logger.debug(f"Connection '{connection}' closed")
        except Exception as e:
            if logger:
                logger.error(msg=f"Error closing the connection '{connection}': "
                                 f"{str_sanitize(source=f'{e}')}")


def db_count(table: str,
             count_clause: str = None,
             where_clause: str = None,
             where_vals: tuple = None,
             where_data: dict[str, Any] = None,
             engine: DbEngine = None,
             connection: Any = None,
             committable: bool = None,
             errors: list[str] = None,
             logger: Logger = None) -> int | None:
    """
    Obtain and return the number of tuples in *table* meeting the criteria defined in *where_data*.

    Optionally, selection criteria may be specified in *where_clause*, or additionally by
    key-value pairs in *where_data*, which would be concatenated by the *AND* logical connector.
    Care should be exercised if *where_clause* contains *IN* directives. In PostgreSQL, the list of values
    for an attribute with the *IN* directive must be contained in a specific tuple, and the operation will
    break for a list of values containing only 1 element. The safe way to specify *IN* directives is
    to add them to *where_data*, as the specifics of each DB flavor will then be properly dealt with.

    The targer database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param table: the table to be searched
    :param count_clause: optional parameters in the *COUNT* clause (defaults to 'COUNT(*)')
    :param where_clause: optional criteria for tuple selection
    :param where_vals: values to be associated with the selection criteria
    :param where_data: the selection criteria specified as key-value pairs
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation upon errorless completion
    :param errors: incidental error messages
    :param logger: optional logger
    :return: the number of tuples counted, or *None* if error
    """
    # initialize the return variable
    result: int | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # execute the query
    recs: list[tuple[int]] = db_select(sel_stmt=f"SELECT COUNT({count_clause or '*'}) FROM {table}",
                                       where_clause=where_clause,
                                       where_vals=where_vals,
                                       where_data=where_data,
                                       engine=engine,
                                       connection=connection,
                                       committable=committable,
                                       errors=op_errors,
                                       logger=logger)
    if not op_errors:
        result = recs[0][0]
    elif isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_exists(table: str,
              where_clause: str = None,
              where_vals: tuple = None,
              where_data: dict[str, Any] = None,
              min_count: int = None,
              max_count: int = None,
              engine: DbEngine = None,
              connection: Any = None,
              committable: bool = None,
              errors: list[str] = None,
              logger: Logger = None) -> bool | None:
    """
    Determine whether at least one tuple in *table* meets the criteria defined in *where_data*.

    Optionally, selection criteria may be specified in *where_clause*, or additionally by
    key-value pairs in *where_data*, which would be concatenated by the *AND* logical connector.
    Care should be exercised if *where_clause* contains *IN* directives. In PostgreSQL, the list of values
    for an attribute with the *IN* directive must be contained in a specific tuple, and the operation will
    break for a list of values containing only 1 element. The safe way to specify *IN* directives is
    to add them to *where_data*, as the specifics of each DB flavor will then be properly dealt with.

    If not positive integers, *min_count* and *max_count* are ignored. If both *min_count* and *max_count*
    are specified with equal values, then exactly that number of tuples must exist.  If neither one is
    specified, than at least one tuple is expected to exist.

    The targer database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param table: the table to be searched
    :param where_clause: optional criteria for tuple selection
    :param where_vals: values to be associated with the selection criteria
    :param where_data: the selection criteria specified as key-value pairs
    :param min_count: optionally defines the minimum number of tuples expected to exist
    :param max_count: optionally defines the maximum number of tuples expected to exist
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation upon errorless completion
    :param errors: incidental error messages
    :param logger: optional logger
    :return: *True* if the criteria for tuple existence were met, *False* otherwise, *None* if error
    """
    # initialize the return variable
    result: bool | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # count the tuples
    count: int = db_count(table=table,
                          where_clause=where_clause,
                          where_vals=where_vals,
                          where_data=where_data,
                          engine=engine,
                          connection=connection,
                          committable=committable,
                          errors=op_errors,
                          logger=logger)
    if not op_errors:
        result = not (count == 0 or
                      (isinstance(max_count, int) and 0 < max_count < count) or
                      (isinstance(min_count, int) and min_count > 0 and min_count > count))
    elif isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_select(sel_stmt: str,
              where_clause: str = None,
              where_vals: tuple = None,
              where_data: dict[str, Any] = None,
              orderby_clause: str = None,
              min_count: int = None,
              max_count: int = None,
              offset_count: int = None,
              limit_count: int = None,
              engine: DbEngine = None,
              connection: Any = None,
              committable: bool = None,
              errors: list[str] = None,
              logger: Logger = None) -> list[tuple] | None:
    """
    Query the database and return all tuples that satisfy the *sel_stmt* command.

    Optionally, selection criteria may be specified in *where_clause* and *where_vals*, or additionally by
    key-value pairs in *where_data*, which would be concatenated by the *AND* logical connector.
    Care should be exercised if *where_clause* contains *IN* directives. In PostgreSQL, the list of values
    for an attribute with the *IN* directive must be contained in a specific tuple, and the operation will
    break for a list of values containing only 1 element. The safe way to specify *IN* directives is
    to put them in *where_data*, as the specifics of each DB flavor will then be properly dealt with.

    If not positive integers, *min_count*, *max_count*, *offset_count*, and *limit_count* are ignored.
    If both *min_count* and *max_count* are specified with equal values, then exactly that number of
    tuples must be returned by the query. The parameter *offset_count* is used to offset the retrieval
    of tuples. For both *offset_count* and *limit_count* to be used together with SQLServer, an *ORDER BY*
    clause must have been specifed, otherwise a runtime error is raised.

    If the search is empty, an empty list is returned.
    The target database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param sel_stmt: SELECT command for the search
    :param where_clause: optional criteria for tuple selection (ignored if *sel_stmt* contains a *WHERE* clause)
    :param where_vals: values to be associated with the selection criteria
    :param where_data: selection criteria specified as key-value pairs
    :param orderby_clause: optional retrieval order (ignored if *sel_stmt* contains a *ORDER BY* clause)
    :param min_count: optionally defines the minimum number of tuples expected
    :param max_count: optionally defines the maximum number of tuples expected
    :param offset_count: number of tuples to skip (defaults to none)
    :param limit_count: limit to the number of tuples returned, to be specified in the query statement itself
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation upon errorless completion
    :param errors: incidental error messages
    :param logger: optional logger
    :return: list of tuples containing the search result, *[]* on empty search, or *None* if error
    """
    # initialize the return variable
    result: list[tuple] | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # assert the database engine
    engine = _assert_engine(engine=engine,
                            errors=op_errors)

    # process search data provided as key-value pairs
    if where_clause or where_data or orderby_clause:
        sel_stmt, where_vals = _combine_search_data(query_stmt=sel_stmt,
                                                    where_clause=where_clause,
                                                    where_vals=where_vals,
                                                    where_data=where_data,
                                                    orderby_clause=orderby_clause,
                                                    engine=engine)
    # establish the correct bind tags
    if where_vals and DB_BIND_META_TAG in sel_stmt:
        sel_stmt = db_adjust_placeholders(stmt=sel_stmt,
                                          engine=engine)
    # make sure to have a connection
    curr_conn: Any = connection or db_connect(engine=engine,
                                              errors=op_errors,
                                              logger=logger)
    if not op_errors:
        if engine == DbEngine.MYSQL:
            pass
        elif engine == DbEngine.ORACLE:
            from . import oracle_pomes
            result = oracle_pomes.select(sel_stmt=sel_stmt,
                                         where_vals=where_vals,
                                         min_count=min_count,
                                         max_count=max_count,
                                         offset_count=offset_count,
                                         limit_count=limit_count,
                                         conn=curr_conn,
                                         committable=committable if connection else True,
                                         errors=op_errors,
                                         logger=logger)
        elif engine == DbEngine.POSTGRES:
            from . import postgres_pomes
            result = postgres_pomes.select(sel_stmt=sel_stmt,
                                           where_vals=where_vals,
                                           min_count=min_count,
                                           max_count=max_count,
                                           offset_count=offset_count,
                                           limit_count=limit_count,
                                           conn=curr_conn,
                                           committable=committable if connection else True,
                                           errors=op_errors,
                                           logger=logger)
        elif engine == DbEngine.SQLSERVER:
            from . import sqlserver_pomes
            result = sqlserver_pomes.select(sel_stmt=sel_stmt,
                                            where_vals=where_vals,
                                            min_count=min_count,
                                            max_count=max_count,
                                            offset_count=offset_count,
                                            limit_count=limit_count,
                                            conn=curr_conn,
                                            committable=committable if connection else True,
                                            errors=op_errors,
                                            logger=logger)
        # close the locally acquired connection
        if not connection:
            db_close(connection=curr_conn,
                     logger=logger)

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_insert(insert_stmt: str,
              insert_vals: tuple = None,
              insert_data: dict[str, Any] = None,
              return_cols: dict[str, type] = None,
              engine: DbEngine = None,
              connection: Any = None,
              committable: bool = None,
              errors: list[str] = None,
              logger: Logger = None) -> tuple | int | None:
    """
    Insert a tuple, with values defined in *insert_vals*, and *insert_data*, into the database.

    The target database engine, specified or default, must have been previously configured.
    The optional *return_cols* indicate that the values of the columns therein should be returned.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param insert_stmt: the INSERT command
    :param insert_vals: values to be inserted
    :param insert_data: data to be inserted as key-value pairs
    :param return_cols: optional columns and respective types, whose values are to be returned
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation upon errorless completion
    :param errors: incidental error messages
    :param logger: optional logger
    :return: the values of *return_cols*, or the number of inserted tuples (0 ou 1), or *None* if error
    """
    # initialize the local errors list
    op_errors: list[str] = []

    # process insert data provided as key-value pairs
    if insert_data:
        insert_stmt, insert_vals = _combine_insert_data(insert_stmt=insert_stmt,
                                                        insert_vals=insert_vals,
                                                        insert_data=insert_data)
    result: tuple | int = db_execute(exc_stmt=insert_stmt,
                                     bind_vals=insert_vals,
                                     return_cols=return_cols,
                                     engine=engine,
                                     connection=connection,
                                     committable=committable,
                                     errors=op_errors,
                                     logger=logger)
    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_update(update_stmt: str,
              update_vals: tuple = None,
              update_data: dict[str, Any] = None,
              where_clause: str = None,
              where_vals: tuple = None,
              where_data: dict[str, Any] = None,
              return_cols: dict[str, type] = None,
              min_count: int = None,
              max_count: int = None,
              engine: DbEngine = None,
              connection: Any = None,
              committable: bool = None,
              errors: list[str] = None,
              logger: Logger = None) -> tuple | int | None:
    """
    Update one or more tuples in the database, as defined by the command *update_stmt*.

    The values for this update are in *update_vals*, and/or specified by key-value pairs in *update_data*.
    The values for selecting the tuples to be updated are in *where_vals*, and/or additionally specified
    by key-value pairs in *where_data*.
    Care should be exercised if *where_clause* contains *IN* directives. In PostgreSQL, the list of values
    for an attribute with the *IN* directive must be contained in a specific tuple, and the operation will
    break for a list of values containing only 1 element. The safe way to specify *IN* directives is
    to add them to *where_data*, as the specifics of each DB flavor will then be properly dealt with.

    The optional *return_cols* indicate that the values of the columns therein should be returned.
    The target database engine, specified or default,  must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param update_stmt: the UPDATE command
    :param update_vals: values for the update operation
    :param update_data: update data as key-value pairs
    :param where_clause: optional criteria for tuple selection (ignored if *upd_stmt* contains a *WHERE* clause)
    :param where_vals: values to be associated with the selection criteria
    :param where_data: selection criteria as key-value pairs
    :param return_cols: optional columns and respective types, whose values are to be returned
    :param min_count: optionally defines the minimum number of tuples to be updated
    :param max_count: optionally defines the maximum number of tuples to be updated
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation upon errorless completion
    :param errors: incidental error messages
    :param logger: optional logger
    :return: the values of *return_cols*, or the number of updated tuples, or *None* if error
    """
    # initialize the local errors list
    op_errors: list[str] = []

    # process update data provided as key-value pairs
    if update_data:
        update_stmt, update_vals = _combine_update_data(update_stmt=update_stmt,
                                                        update_vals=update_vals,
                                                        update_data=update_data)
    # process search data provided as key-value pairs
    if where_clause or where_data:
        engine = _assert_engine(engine=engine)
        update_stmt, where_vals = _combine_search_data(query_stmt=update_stmt,
                                                       where_clause=where_clause,
                                                       where_vals=where_vals,
                                                       where_data=where_data,
                                                       orderby_clause=None,
                                                       engine=engine)
    # combine 'update' and 'where' bind values
    bind_vals: tuple | None = None
    if update_vals and where_vals:
        bind_vals = update_vals + where_vals
    elif update_vals:
        bind_vals = update_vals
    elif where_vals:
        bind_vals = where_vals

    result: tuple | int = db_execute(exc_stmt=update_stmt,
                                     bind_vals=bind_vals,
                                     return_cols=return_cols,
                                     min_count=min_count,
                                     max_count=max_count,
                                     engine=engine,
                                     connection=connection,
                                     committable=committable,
                                     errors=op_errors,
                                     logger=logger)
    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_delete(delete_stmt: str,
              where_clause: str = None,
              where_vals: tuple = None,
              where_data: dict[str, Any] = None,
              min_count: int = None,
              max_count: int = None,
              engine: DbEngine = None,
              connection: Any = None,
              committable: bool = None,
              errors: list[str] = None,
              logger: Logger = None) -> int | None:
    """
    Delete one or more tuples in the database, as defined by the *delete_stmt* command.

    The values for selecting the tuples to be deleted are in *where_vals*, and/or additionally specified
    by key-value pairs in *where_data*.
    Care should be exercised if *where_clause* contains *IN* directives. In PostgreSQL, the list of values
    for an attribute with the *IN* directive must be contained in a specific tuple, and the operation will
    break for a list of values containing only 1 element. The safe way to specify *IN* directives is
    to add them to *where_data*, as the specifics of each DB flavor will then be properly dealt with.

    The target database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param delete_stmt: the DELETE command
    :param where_clause: optional criteria for tuple selection (ignored if *delete_stmt* contains a *WHERE* clause)
    :param where_vals: values to be associated with the selection criteria
    :param where_data: selection criteria as key-value pairs
    :param min_count: optionally defines the minimum number of tuples to be deleted
    :param max_count: optionally defines the maximum number of tuples to be deleted
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation upon errorless completion
    :param errors: incidental error messages
    :param logger: optional logger
    :return: the number of deleted tuples, or *None* if error
    """
    # initialize the local errors list
    op_errors: list[str] = []

    # process search data provided as key-value pairs
    if where_clause or where_data:
        engine = _assert_engine(engine=engine)
        delete_stmt, where_vals = _combine_search_data(query_stmt=delete_stmt,
                                                       where_clause=where_clause,
                                                       where_vals=where_vals,
                                                       where_data=where_data,
                                                       orderby_clause=None,
                                                       engine=engine)
    result: int = db_execute(exc_stmt=delete_stmt,
                             bind_vals=where_vals,
                             min_count=min_count,
                             max_count=max_count,
                             engine=engine,
                             connection=connection,
                             committable=committable,
                             errors=op_errors,
                             logger=logger)
    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_bulk_insert(target_table: str,
                   insert_attrs: list[str],
                   insert_vals: list[tuple],
                   engine: DbEngine = None,
                   connection: Any = None,
                   committable: bool = None,
                   identity_column: str = None,
                   errors: list[str] = None,
                   logger: Logger = None) -> int | None:
    """
    Bulk insert rows into *target_table*, with values of *insert_attrs* defined in *insert_vals*.

    Bulk inserts may require non-standard syntax, depending on the database engine being targeted.
    The number of attributes in *insert_attrs* must match the number of bind values in *insert_vals* tuples.
    Specific handling is required for identity columns (i.e., columns whose values are generated directly
    by the database engine - typically, they are also primary keys), and thus they must be identified
    by *identity_column*, and ommited from *insert_stmt*,
    The target database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param target_table: the possibly schema-qualified table to insert into
    :param insert_attrs: the list of table attributes to insert values into
    :param insert_vals: the list of values to be inserted
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation upon errorless completion
    :param identity_column: column whose values are generated by the database
    :param errors: incidental error messages
    :param logger: optional logger
    :return: the number of inserted tuples (1 for postgres), or *None* if error
    """
    # initialize the return variable
    result: int | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # assert the database engine
    engine = _assert_engine(engine=engine,
                            errors=op_errors)

    # make sure to have a connection
    curr_conn: Any = connection or db_connect(engine=engine,
                                              errors=op_errors,
                                              logger=logger)
    if not op_errors:
        if engine == DbEngine.MYSQL:
            pass
        elif engine == DbEngine.POSTGRES:
            from . import postgres_pomes
            insert_stmt: str = f"INSERT INTO {target_table} ({', '.join(insert_attrs)})"
            # pre-insert handling of identity columns
            if identity_column and insert_stmt.find("OVERRIDING SYSTEM VALUE") < 0:
                insert_stmt += " OVERRIDING SYSTEM VALUE"
            insert_stmt += " VALUES %s"
            # obtain template to handle inserts of null values
            template: str = postgres_pomes.build_typified_template(insert_stmt=insert_stmt,
                                                                   nullable_only=True,
                                                                   conn=curr_conn,
                                                                   errors=op_errors,
                                                                   logger=logger)
            if not op_errors:
                result = postgres_pomes.bulk_execute(exc_stmt=insert_stmt,
                                                     exc_vals=insert_vals,
                                                     template=template,
                                                     conn=curr_conn,
                                                     committable=False if identity_column else
                                                     (committable if connection else True),
                                                     errors=op_errors,
                                                     logger=logger)
                # post-insert handling of identity columns
                if not op_errors and identity_column:
                    postgres_pomes.identity_post_insert(insert_stmt=insert_stmt,
                                                        conn=curr_conn,
                                                        committable=committable if connection else True,
                                                        identity_column=identity_column,
                                                        errors=op_errors,
                                                        logger=logger)
        elif engine in [DbEngine.ORACLE, DbEngine.SQLSERVER]:
            bind_marks: str = _bind_marks(engine=engine,
                                          start=1,
                                          finish=len(insert_attrs) + 1)
            insert_stmt: str = (f"INSERT INTO {target_table} "
                                f"({', '.join(insert_attrs)}) VALUES({bind_marks})")
            if engine == DbEngine.ORACLE:
                from . import oracle_pomes
                result = oracle_pomes.bulk_execute(exc_stmt=insert_stmt,
                                                   exc_vals=insert_vals,
                                                   conn=curr_conn,
                                                   committable=committable if connection else True,
                                                   errors=op_errors,
                                                   logger=logger)
            elif engine == DbEngine.SQLSERVER:
                from . import sqlserver_pomes
                # pre-insert handling of identity columns
                if identity_column:
                    sqlserver_pomes.identity_pre_insert(insert_stmt=insert_stmt,
                                                        conn=curr_conn,
                                                        errors=op_errors,
                                                        logger=logger)
                if not op_errors:
                    result = sqlserver_pomes.bulk_execute(exc_stmt=insert_stmt,
                                                          exc_vals=insert_vals,
                                                          conn=curr_conn,
                                                          committable=False if identity_column else
                                                          (committable if connection else True),
                                                          errors=op_errors,
                                                          logger=logger)
                    # post-insert handling of identity columns
                    if not op_errors and identity_column:
                        from . import sqlserver_pomes
                        sqlserver_pomes.identity_post_insert(insert_stmt=insert_stmt,
                                                             conn=curr_conn,
                                                             committable=committable if connection else True,
                                                             identity_column=identity_column,
                                                             errors=op_errors,
                                                             logger=logger)
        # close the locally acquired connection
        if not connection:
            db_close(connection=curr_conn,
                     logger=logger)

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_bulk_update(target_table: str,
                   set_attrs: list[str],
                   where_attrs: list[str],
                   update_vals: list[tuple],
                   engine: DbEngine = None,
                   connection: Any = None,
                   committable: bool = None,
                   errors: list[str] = None,
                   logger: Logger = None) -> int | None:
    """
    Bulk update rows in *target_table*, with values of *where_attrs* defined in *update_vals*.

    Bulk updates require non-standard syntax, specific for the database engine being targeted.
    The number of attributes in *set_attrs*, plus the number of attributes in *where_attrs*,
    must match the number of bind values in *update_vals* tuples. Note that within *update_vals*,
    the bind values for the *WHERE* clause will follow the ones for the *SET* clause.
    The target database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param target_table: the possibly schema-qualified table to update
    :param set_attrs: the list of table attributes to update
    :param where_attrs: the list of table attributes identifying the tuples
    :param update_vals: the list of values to update the database with, and to identify the tuples
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation upon errorless completion
    :param errors: incidental error messages
    :param logger: optional logger
    :return: the number of updated tuples, or *None* if error
    """
    # initialize the return variable
    result: int | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # assert the database engine
    engine = _assert_engine(engine=engine,
                            errors=errors)

    # make sure to have a connection
    curr_conn: Any = connection or db_connect(engine=engine,
                                              errors=op_errors,
                                              logger=logger)
    if not op_errors:
        if engine == DbEngine.MYSQL:
            pass
        elif engine == DbEngine.POSTGRES:
            from . import postgres_pomes
            set_items: str = ""
            for set_attr in set_attrs:
                set_items += f"{set_attr} = data.{set_attr}, "
            where_items: str = ""
            for where_attr in where_attrs:
                where_items += f"{target_table}.{where_attr} = data.{where_attr} AND "
            update_stmt: str = (f"UPDATE {target_table}"
                                f" SET {set_items[:-2]} "
                                f"FROM (VALUES %s) AS data ({', '.join(set_attrs + where_attrs)}) "
                                f"WHERE {where_items[:-5]}")
            # modify statement to handle updates of null values
            update_stmt = postgres_pomes.tipify_bulk_update(update_stmt=update_stmt,
                                                            nullable_only=True,
                                                            conn=curr_conn,
                                                            errors=op_errors,
                                                            logger=logger)
            if not op_errors:
                result = postgres_pomes.bulk_execute(exc_stmt=update_stmt,
                                                     exc_vals=update_vals,
                                                     template=None,
                                                     conn=curr_conn,
                                                     committable=committable if connection else True,
                                                     errors=op_errors,
                                                     logger=logger)
        elif engine in [DbEngine.ORACLE, DbEngine.SQLSERVER]:
            set_items: str = _bind_columns(engine=engine,
                                           columns=set_attrs,
                                           concat=", ",
                                           start_index=1)
            where_items: str = _bind_columns(engine=engine,
                                             columns=where_attrs,
                                             concat=" AND ",
                                             start_index=len(set_attrs)+1)
            update_stmt: str = f"UPDATE {target_table} SET {set_items} WHERE {where_items}"
            if engine == DbEngine.ORACLE:
                from . import oracle_pomes
                result = oracle_pomes.bulk_execute(exc_stmt=update_stmt,
                                                   exc_vals=update_vals,
                                                   conn=curr_conn,
                                                   committable=committable if connection else True,
                                                   errors=op_errors,
                                                   logger=logger)
            else:
                from . import sqlserver_pomes
                result = sqlserver_pomes.bulk_execute(exc_stmt=update_stmt,
                                                      exc_vals=update_vals,
                                                      conn=curr_conn,
                                                      committable=committable if connection else True,
                                                      errors=op_errors,
                                                      logger=logger)
        # close the locally acquired connection
        if not connection:
            db_close(connection=curr_conn,
                     logger=logger)

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_bulk_delete(target_table: str,
                   where_attrs: list[str],
                   where_vals: list[tuple],
                   engine: DbEngine = None,
                   connection: Any = None,
                   committable: bool = None,
                   errors: list[str] = None,
                   logger: Logger = None) -> int | None:
    """
    Bulk delete from *target_table*, with values of *where_attrs* defined in *where_vals*.

    Bulk deletes may require non-standard syntax, depending on the database engine being targeted.
    The number of attributes in *where_attrs* must match the number of bind values in *where_vals* tuples.
    The target database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param target_table: the possibly schema-qualified table to delete from
    :param where_attrs: the list of attributes for identifying the tuples to be deleted
    :param where_vals: the list of values to bind to the attributes
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation upon errorless completion
    :param errors: incidental error messages
    :param logger: optional logger
    :return: the number of inserted tuples (1 for postgres), or *None* if error
    """
    # initialize the return variable
    result: int | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # assert the database engine
    engine = _assert_engine(engine=engine,
                            errors=errors)

    # make sure to have a connection
    curr_conn: Any = connection or db_connect(engine=engine,
                                              errors=op_errors,
                                              logger=logger)
    if not op_errors:
        if engine == DbEngine.MYSQL:
            pass
        elif engine == DbEngine.POSTGRES:
            from . import postgres_pomes
            delete_stmt: str = (f"DELETE FROM {target_table} "
                                f"WHERE ({', '.join(where_attrs)}) IN (%s)")
            result = postgres_pomes.bulk_execute(exc_stmt=delete_stmt,
                                                 exc_vals=where_vals,
                                                 template=None,
                                                 conn=curr_conn,
                                                 committable=committable if connection else True,
                                                 errors=op_errors,
                                                 logger=logger)
        elif engine in [DbEngine.ORACLE, DbEngine.SQLSERVER]:
            where_items: str = _bind_columns(engine=engine,
                                             columns=where_attrs,
                                             concat=" AND",
                                             start_index=1)
            delete_stmt: str = f"DELETE FROM {target_table} WHERE {where_items}"
            if engine == DbEngine.ORACLE:
                from . import oracle_pomes
                result = oracle_pomes.bulk_execute(exc_stmt=delete_stmt,
                                                   exc_vals=where_vals,
                                                   conn=curr_conn,
                                                   committable=committable if connection else True,
                                                   errors=op_errors,
                                                   logger=logger)
            else:
                from . import sqlserver_pomes
                result = sqlserver_pomes.bulk_execute(exc_stmt=delete_stmt,
                                                      exc_vals=where_vals,
                                                      conn=curr_conn,
                                                      committable=committable if connection else True,
                                                      errors=op_errors,
                                                      logger=logger)
        # close the locally acquired connection
        if not connection:
            db_close(connection=curr_conn,
                     logger=logger)

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_update_lob(lob_table: str,
                  lob_column: str,
                  pk_columns: list[str],
                  pk_vals: tuple,
                  lob_data: bytes | str | Path | BinaryIO,
                  chunk_size: int,
                  engine: DbEngine = None,
                  connection: Any = None,
                  committable: bool = None,
                  errors: list[str] = None,
                  logger: Logger = None) -> None:
    """
    Update a large binary object (LOB) in the given table and column.

    The data for the update may come from *bytes*, from a *Path* or its string representation,
    or from a pointer obtained from *BytesIO* or *Path.open()* in binary mode.
    The target database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param lob_table: the table to be update with the new LOB
    :param lob_column: the column to be updated with the new LOB
    :param pk_columns: columns making up a primary key, or a unique identifier for the tuple
    :param pk_vals: values with which to locate the tuple to be updated
    :param lob_data: the LOB data (bytes, a file path, or a file pointer)
    :param chunk_size: size in bytes of the data chunk to read/write, or 0 or None for no limit
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation upon errorless completion
    :param errors: incidental error messages
    :param logger: optional logger
    :return: number of LOBs effectively copied, or *None* if error
    """
    # initialize the local errors list
    op_errors: list[str] = []

    # assert the database engine
    engine = _assert_engine(engine=engine,
                            errors=errors)

    # make sure to have a connection
    curr_conn: Any = connection or db_connect(engine=engine,
                                              errors=op_errors,
                                              logger=logger)
    if not op_errors:
        if engine == DbEngine.MYSQL:
            pass
        elif engine == DbEngine.ORACLE:
            from . import oracle_pomes
            oracle_pomes.update_lob(lob_table=lob_table,
                                    lob_column=lob_column,
                                    pk_columns=pk_columns,
                                    pk_vals=pk_vals,
                                    lob_data=lob_data,
                                    chunk_size=chunk_size,
                                    conn=curr_conn,
                                    committable=committable if connection else True,
                                    errors=op_errors,
                                    logger=logger)
        elif engine == DbEngine.POSTGRES:
            from . import postgres_pomes
            postgres_pomes.update_lob(lob_table=lob_table,
                                      lob_column=lob_column,
                                      pk_columns=pk_columns,
                                      pk_vals=pk_vals,
                                      lob_data=lob_data,
                                      chunk_size=chunk_size,
                                      conn=curr_conn,
                                      committable=committable if connection else True,
                                      errors=op_errors,
                                      logger=logger)
        elif engine == DbEngine.SQLSERVER:
            from . import sqlserver_pomes
            sqlserver_pomes.update_lob(lob_table=lob_table,
                                       lob_column=lob_column,
                                       pk_columns=pk_columns,
                                       pk_vals=pk_vals,
                                       lob_data=lob_data,
                                       chunk_size=chunk_size,
                                       conn=curr_conn,
                                       committable=committable if connection else True,
                                       errors=op_errors,
                                       logger=logger)
        # close the locally acquired connection
        if not connection:
            db_close(connection=curr_conn,
                     logger=logger)

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)


def db_execute(exc_stmt: str,
               bind_vals: tuple = None,
               return_cols: dict[str, type] = None,
               min_count: int = None,
               max_count: int = None,
               engine: DbEngine = None,
               connection: Any = None,
               committable: bool = None,
               errors: list[str] = None,
               logger: Logger = None) -> int | None:
    """
    Execute the command *exc_stmt* on the database.

    This command might be a DML ccommand modifying the database, such as inserting, updating or
    deleting tuples, or it might be a DDL statement, or it might even be an environment-related command.

    The optional bind values for this operation are in *bind_vals*. The optional *return_cols* indicate that
    the values of the columns therein should be returned upon execution of *exc_stmt*. This is typical for
    *INSERT* or *UPDATE* statements on tables with *identity-type* columns, which are columns whose values
    are generated by the database itself. Otherwise, the value returned is the number of inserted, modified,
    or deleted tuples, or *None* if an error occurred.

    The value returned by this operation (as *cursor.rowcount*) is verified against *min_count* or *max_count*,
    if provided. An error is issued if a disagreement exists, followed by a rollback. This is an optional feature,
    intended to minimize data loss due to programming mistakes.

    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param exc_stmt: the command to execute
    :param bind_vals: optional bind values
    :param return_cols: optional columns and respective types, whose values are to be returned
    :param min_count: optionally defines the minimum number of tuples to be affected
    :param max_count: optionally defines the maximum number of tuples to be affected
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation upon errorless completion
    :param errors: incidental error messages
    :param logger: optional logger
    :return: the values of *return_cols*, the return value from the command execution, or *None* if error
    """
    # initialize the return variable
    result: int | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # assert the database engine
    engine = _assert_engine(engine=engine,
                            errors=errors)

    # make sure to have a connection
    curr_conn: Any = connection or db_connect(engine=engine,
                                              errors=op_errors,
                                              logger=logger)
    if not op_errors:
        # establish the correct bind tags
        if bind_vals and DB_BIND_META_TAG in exc_stmt:
            exc_stmt = db_adjust_placeholders(stmt=exc_stmt,
                                              engine=engine)
        if engine == DbEngine.MYSQL:
            pass
        elif engine == DbEngine.ORACLE:
            from . import oracle_pomes
            result = oracle_pomes.execute(exc_stmt=exc_stmt,
                                          bind_vals=bind_vals,
                                          return_cols=return_cols,
                                          min_count=min_count,
                                          max_count=max_count,
                                          conn=curr_conn,
                                          committable=committable if connection else True,
                                          errors=op_errors,
                                          logger=logger)
        elif engine == DbEngine.POSTGRES:
            from . import postgres_pomes
            result = postgres_pomes.execute(exc_stmt=exc_stmt,
                                            bind_vals=bind_vals,
                                            return_cols=return_cols,
                                            min_count=min_count,
                                            max_count=max_count,
                                            conn=curr_conn,
                                            committable=committable if connection else True,
                                            errors=op_errors,
                                            logger=logger)
        elif engine == DbEngine.SQLSERVER:
            from . import sqlserver_pomes
            result = sqlserver_pomes.execute(exc_stmt=exc_stmt,
                                             bind_vals=bind_vals,
                                             return_cols=return_cols,
                                             min_count=min_count,
                                             max_count=max_count,
                                             conn=curr_conn,
                                             committable=committable if connection else True,
                                             errors=op_errors,
                                             logger=logger)
        # close the locally acquired connection
        if not connection:
            db_close(connection=curr_conn,
                     logger=logger)

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_call_function(func_name: str,
                     func_vals: tuple = None,
                     engine: DbEngine = None,
                     connection: Any = None,
                     committable: bool = None,
                     errors: list[str] = None,
                     logger: Logger = None) -> list[tuple] | None:
    """
    Execute the stored function *func_name* in the database, with the parameters given in *func_vals*.

    The target database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param func_name: name of the stored function
    :param func_vals: parameters for the stored function
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation upon errorless completion
    :param errors: incidental error messages
    :param logger: optional logger
    :return: the data returned by the function, or *None* if error
    """
    # initialize the return variable
    result: Any = None

    # initialize the local errors list
    op_errors: list[str] = []

    # assert the database engine
    engine = _assert_engine(engine=engine,
                            errors=errors)

    # make sure to have a connection
    curr_conn: Any = connection or db_connect(engine=engine,
                                              errors=op_errors,
                                              logger=logger)
    if not op_errors:
        if engine == DbEngine.MYSQL:
            pass
        elif engine == DbEngine.ORACLE:
            from . import oracle_pomes
            result = oracle_pomes.call_function(func_name=func_name,
                                                func_vals=func_vals,
                                                conn=curr_conn,
                                                committable=committable if connection else True,
                                                errors=op_errors,
                                                logger=logger)
        elif engine == DbEngine.POSTGRES:
            from . import postgres_pomes
            result = postgres_pomes.call_procedure(proc_name=func_name,
                                                   proc_vals=func_vals,
                                                   conn=curr_conn,
                                                   committable=committable if connection else True,
                                                   errors=op_errors,
                                                   logger=logger)
        elif engine == DbEngine.SQLSERVER:
            from . import sqlserver_pomes
            result = sqlserver_pomes.call_procedure(proc_name=func_name,
                                                    proc_vals=func_vals,
                                                    conn=curr_conn,
                                                    committable=committable if connection else True,
                                                    errors=op_errors,
                                                    logger=logger)
        # close the locally acquired connection
        if not connection:
            db_close(connection=curr_conn,
                     logger=logger)

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_call_procedure(proc_name: str,
                      proc_vals: tuple = None,
                      engine: DbEngine = None,
                      connection: Any = None,
                      committable: bool = None,
                      errors: list[str] = None,
                      logger: Logger = None) -> list[tuple] | None:
    """
    Execute the stored procedure *proc_name* in the database, with the parameters given in *proc_vals*.

    The target database engine, specified or default, must have been previously configured.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param proc_name: name of the stored procedure
    :param proc_vals: parameters for the stored procedure
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation upon errorless completion
    :param errors: incidental error messages
    :param logger: optional logger
    :return: the data returned by the procedure, or *None* if error
    """
    # initialize the return variable
    result: Any = None

    # initialize the local errors list
    op_errors: list[str] = []

    # assert the database engine
    engine = _assert_engine(engine=engine,
                            errors=errors)

    # make sure to have a connection
    # make sure to have a connection
    curr_conn: Any = connection or db_connect(engine=engine,
                                              errors=op_errors,
                                              logger=logger)
    if not op_errors:
        if engine == DbEngine.MYSQL:
            pass
        elif engine == DbEngine.ORACLE:
            from . import oracle_pomes
            result = oracle_pomes.call_procedure(proc_name=proc_name,
                                                 proc_vals=proc_vals,
                                                 conn=curr_conn,
                                                 committable=committable if connection else True,
                                                 errors=op_errors,
                                                 logger=logger)
        elif engine == DbEngine.POSTGRES:
            from . import postgres_pomes
            result = postgres_pomes.call_procedure(proc_name=proc_name,
                                                   proc_vals=proc_vals,
                                                   conn=curr_conn,
                                                   committable=committable if connection else True,
                                                   errors=op_errors,
                                                   logger=logger)
        elif engine == DbEngine.SQLSERVER:
            from . import sqlserver_pomes
            result = sqlserver_pomes.call_procedure(proc_name=proc_name,
                                                    proc_vals=proc_vals,
                                                    conn=curr_conn,
                                                    committable=committable if connection else True,
                                                    errors=op_errors,
                                                    logger=logger)
        # close the locally acquired connection
        if not connection:
            db_close(connection=curr_conn,
                     logger=logger)

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def __get_version(engine: DbEngine,
                  connection: Any) -> str:
    """
    Obtain and return the current version of *engine*.

    :param engine: the reference database engine (the default engine, if not provided)
    :return: the engine's current version, or an empty string if not found
    """
    # initialize the return variable
    result: str = ""

    stmt: str | None = None
    match engine:
        case DbEngine.MYSQL:
            pass
        case DbEngine.ORACLE:
            stmt = "SELECT version FROM v$instance"
        case DbEngine.POSTGRES:
            stmt = "SHOW server_version"
        case DbEngine.SQLSERVER:
            stmt = "SELECT @@VERSION"

    if stmt:
        reply: list[tuple[str]] = db_select(sel_stmt=stmt,
                                            engine=engine,
                                            connection=connection)
        if reply:
            result = reply[0][0]

    return result
