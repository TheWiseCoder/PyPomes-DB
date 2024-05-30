# noinspection DuplicatedCode
from copy import copy
from logging import Logger
from pathlib import Path
from typing import Any, Literal

from .db_common import (
    _DB_ENGINES, _DB_CONN_DATA, _assert_engine, _get_param
)


def db_setup(engine: Literal["mysql", "oracle", "postgres", "sqlserver"],
             db_name: str,
             db_user: str,
             db_pwd: str,
             db_host: str,
             db_port: int,
             db_client: str = None,
             db_driver: str = None) -> bool:
    """
    Establish the provided parameters for access to *engine*.

    The meaning of some parameters may vary between different database engines.
    All parameters, with the exception of *db_client* and *db_driver*, are required.
    *db_client* may be provided for *oracle*, but not for the other engines.
    *db_driver* is required for *sqlserver*, but is not accepted for the other engines.

    :param engine: the database engine (one of [mysql, oracle, postgres, sqlserver])
    :param db_name: the database or service name
    :param db_user: the logon user
    :param db_pwd: the logon password
    :param db_host: the host URL
    :param db_port: the connection port (a positive integer)
    :param db_driver: the database driver (SQLServer only)
    :param db_client: the path to the client software (optional, Oracle only)
    :return: True if the data was accepted, False otherwise
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
            _DB_CONN_DATA[engine]["client"] = db_client
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
    return _DB_ENGINES


def db_get_param(key: Literal["name", "user", "host", "port", "client", "driver"],
                 engine: str = None) -> dict:
    """
    Return the connection parameter value for *key*.

    The connection key should be one of *name*, *user*, *host*, and *port*.
    For *oracle* and *sqlserver* engines, the extra keys *client* and *driver*
    might be used, respectively.

    :param key: the reference parameter
    :param engine: the database engine to use (uses the default engine, if not provided)
    :return: the current connection parameters for the engine
    """
    curr_engine: str = _DB_ENGINES[0] if not engine and _DB_ENGINES else engine
    return None if key == "pwd" else _get_param(curr_engine, key)


def db_get_params(engine: str = None) -> dict:
    """
    Return the connection parameters as a *dict*.

    The returned *dict* contains the keys *name*, *user*, *host*, *port*.
    For *oracle* engines, the returned *dict* contains the extra key *client*.
    For *sqlserver* engines, the  returned *dict* contains the extra key *driver*.
    The meaning of these parameters may vary between different database engines.
    Note that the value of the *pwd* parameter is not returned.

    :param engine: the database engine to use (uses the default engine, if not provided)
    :return: the current connection parameters for the engine
    """
    curr_engine: str = _DB_ENGINES[0] if not engine and _DB_ENGINES else engine
    result: dict = copy(x=_DB_CONN_DATA.get(curr_engine))
    result.pop("pwd", None)

    return result


def db_assert_connection(errors: list[str] | None,
                         engine: str = None,
                         logger: Logger = None) -> bool:
    """
    Determine whether the *engine*'s current configuration allows for a safe connection.

    :param errors: incidental errors
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param logger: optional logger
    :return: True if the trial connection succeeded, False otherwise
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

    # acknowledge eventual local errors
    errors.extend(op_errors)

    return result


def db_connect(errors: list[str] | None,
               autocommit: bool = False,
               engine: str = None,
               logger: Logger = None) -> Any:
    """
    Obtain and return a connection to the database, or *None* if the connection cannot be obtained.

    The target database engine, default or specified, must have been previously configured.

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

    # acknowledge eventual local errors
    errors.extend(op_errors)

    return result


def db_exists(errors: list[str],
              table: str,
              where_attrs: list[str] = None,
              where_vals: tuple = None,
              engine: str = None,
              connection: Any = None,
              committable: bool = True,
              logger: Logger = None) -> bool:
    """
    Determine whether the table *table* in the database contains at least one tuple.

    For this determination, *where_attrs* are made equal to *where_values* in the query, respectively.
    If more than one, the attributes are concatenated by the *AND* logical connector.
    The targer database engine, default or specified, must have been previously configured.

    :param errors: incidental error messages
    :param table: the table to be searched
    :param where_attrs: the search attributes
    :param where_vals: the values for the search attributes
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'connection' to be provided)
    :param logger: optional logger
    :return: True if at least one tuple was found
    """
    # initialize the local errors list
    op_errors: list[str] = []

    # noinspection PyDataSource
    sel_stmt: str = "SELECT * FROM " + table
    if where_attrs:
        sel_stmt += " WHERE " + "".join(f"{attr} = %s AND " for attr in where_attrs)[0:-5]
    rec: tuple = db_select_one(errors=op_errors,
                               sel_stmt=sel_stmt,
                               where_vals=where_vals,
                               require_nonempty=False,
                               engine = engine,
                               connection=connection,
                               committable=committable,
                               logger=logger)
    result: bool = None if op_errors else rec is not None

    # acknowledge eventual local errors
    errors.extend(op_errors)

    return result


def db_select_one(errors: list[str] | None,
                  sel_stmt: str,
                  where_vals: tuple = None,
                  require_nonempty: bool = False,
                  engine: str = None,
                  connection: Any = None,
                  committable: bool = True,
                  logger: Logger = None) -> tuple:
    """
    Search the database and return the first tuple that satisfies the *sel_stmt* search command.

    The command can optionally contain search criteria, with respective values given
    in *where_vals*. The list of values for an attribute with the *IN* clause must be contained
    in a specific tuple. In case of error, or if the search is empty, *None* is returned.
    The target database engine, default or specified, must have been previously configured.

    :param errors: incidental error messages
    :param sel_stmt: SELECT command for the search
    :param where_vals: values to be associated with the search criteria
    :param require_nonempty: defines whether an empty search should be considered an error
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'connection' to be provided)
    :param logger: optional logger
    :return: tuple containing the search result, [] if the search was empty, or None if there was an error
    """
    # initialize the local errors list
    op_errors: list[str] = []

    require_min: int = 1 if require_nonempty else None
    reply: list[tuple] = db_select_all(errors=op_errors,
                                       sel_stmt=sel_stmt,
                                       where_vals=where_vals,
                                       require_min=require_min,
                                       require_max=1,
                                       engine=engine,
                                       connection=connection,
                                       committable=committable,
                                       logger=logger)

    # acknowledge eventual local errors
    errors.extend(op_errors)

    return reply[0] if reply else None


def db_select_all(errors: list[str] | None,
                  sel_stmt: str,
                  where_vals: tuple = None,
                  require_min: int = None,
                  require_max: int = None,
                  engine: str = None,
                  connection: Any = None,
                  committable: bool = True,
                  logger: Logger = None) -> list[tuple]:
    """
    Search the database and return all tuples that satisfy the *sel_stmt* search command.

    The command can optionally contain search criteria, with respective values given
    in *where_vals*. The list of values for an attribute with the *IN* clause must be contained
    in a specific tuple. If not positive integers, *require_min* and *require_max* are ignored.
    If the search is empty, an empty list is returned.
    The target database engine, default or specified, must have been previously configured.

    :param errors: incidental error messages
    :param sel_stmt: SELECT command for the search
    :param where_vals: the values to be associated with the search criteria
    :param require_min: optionally defines the minimum number of tuples to be returned
    :param require_max: optionally defines the maximum number of tuples to be returned
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'connection' to be provided)
    :param logger: optional logger
    :return: list of tuples containing the search result, or [] if the search is empty
    """
    # initialize the return variable
    result: list[tuple] | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(errors=op_errors,
                                      engine=engine)
    if curr_engine == "mysql":
        pass
    elif curr_engine == "oracle":
        from . import oracle_pomes
        result = oracle_pomes.select_all(errors=op_errors,
                                         sel_stmt=sel_stmt,
                                         where_vals=where_vals,
                                         require_min=require_min,
                                         require_max=require_max,
                                         conn=connection,
                                         committable=committable,
                                         logger=logger)
    elif curr_engine == "postgres":
        from . import postgres_pomes
        result = postgres_pomes.select_all(errors=op_errors,
                                           sel_stmt=sel_stmt,
                                           where_vals=where_vals,
                                           require_min=require_min,
                                           require_max=require_max,
                                           conn=connection,
                                           committable=committable,
                                           logger=logger)
    elif curr_engine == "sqlserver":
        from . import sqlserver_pomes
        result = sqlserver_pomes.select_all(errors=op_errors,
                                            sel_stmt=sel_stmt,
                                            where_vals=where_vals,
                                            require_min=require_min,
                                            require_max=require_max,
                                            conn=connection,
                                            committable=committable,
                                            logger=logger)

    # acknowledge eventual local errors
    errors.extend(op_errors)

    return result


def db_insert(errors: list[str] | None,
              insert_stmt: str,
              insert_vals: tuple,
              engine: str = None,
              connection: Any = None,
              committable: bool = True,
              logger: Logger = None) -> int:
    """
    Insert a tuple, with values defined in *insert_vals*, into the database.

    The target database engine, default or specified, must have been previously configured.

    :param errors: incidental error messages
    :param insert_stmt: the INSERT command
    :param insert_vals: the values to be inserted
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'connection' to be provided)
    :param logger: optional logger
    :return: the number of inserted tuples (0 ou 1), or None if an error occurred
    """
    # initialize the local errors list
    op_errors: list[str] = []

    result: int = db_execute(errors=op_errors,
                             exc_stmt=insert_stmt,
                             bind_vals=insert_vals,
                             engine=engine,
                             connection=connection,
                             committable=committable,
                             logger=logger)

    # acknowledge eventual local errors
    errors.extend(op_errors)

    return result


def db_update(errors: list[str] | None,
              update_stmt: str,
              update_vals: tuple = None,
              where_vals: tuple = None,
              engine: str = None,
              connection: Any = None,
              committable: bool = True,
              logger: Logger = None) -> int:
    """
    Update one or more tuples in the database, as defined by the command *update_stmt*.

    The values for this update are in *update_vals*.
    The values for selecting the tuples to be updated are in *where_vals*.
    The target database engine, default or specified, must have been previously configured.

    :param errors: incidental error messages
    :param update_stmt: the UPDATE command
    :param update_vals: the values for the update operation
    :param where_vals: the values to be associated with the search criteria
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'connection' to be provided)
    :param logger: optional logger
    :return: the number of updated tuples, or None if an error occurred
    """
    # initialize the local errors list
    op_errors: list[str] = []

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

    # acknowledge eventual local errors
    errors.extend(op_errors)

    return result


def db_delete(errors: list[str] | None,
              delete_stmt: str,
              where_vals: tuple = None,
              engine: str = None,
              connection: Any = None,
              committable: bool = True,
              logger: Logger = None) -> int:
    """
    Delete one or more tuples in the database, as defined by the *delete_stmt* command.

    The values for selecting the tuples to be deleted are in *where_vals*.
    The target database engine, default or specified, must have been previously configured.

    :param errors: incidental error messages
    :param delete_stmt: the DELETE command
    :param where_vals: the values to be associated with the search criteria
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'connection' to be provided)
    :param logger: optional logger
    :return: the number of deleted tuples, or None if an error occurred
    """
    # initialize the local errors list
    op_errors: list[str] = []

    result: int = db_execute(errors=op_errors,
                             exc_stmt=delete_stmt,
                             bind_vals=where_vals,
                             engine=engine,
                             connection=connection,
                             committable=committable,
                             logger=logger)

    # acknowledge eventual local errors
    errors.extend(op_errors)

    return result


def db_bulk_insert(errors: list[str] | None,
                   insert_stmt: str,
                   insert_vals: list[tuple],
                   engine: str = None,
                   connection: Any = None,
                   committable: bool = True,
                   logger: Logger = None) -> int:
    """
    Insert the tuples, with values defined in *insert_vals*, into the database.

    The target database engine, default or specified, must have been previously configured.

    The binding is done by position. Thus, the *VALUES* clause in *insert_stmt* must contain
    as many placeholders as there are elements in the tuples found in the list provided in
    *insert_vals*. This is applicable for *mysql*, *oracle*, and *sqlserver*, where the
    placeholders are '%VARn%, ':n', and '?', respectively, and 'n' is the 1-based position of the
    data in the tuple. In the specific case of *postgres*, the *VALUES* clause must be simply *VALUES %s*.

    :param errors: incidental error messages
    :param insert_stmt: the INSERT command
    :param insert_vals: the list of values to be inserted
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'connection' to be provided)
    :param logger: optional logger
    :return: the number of inserted tuples (1 for postgres), or None if an error occurred
    """
    # initialize the return variable
    result: int | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(op_errors, engine)
    
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

    # acknowledge eventual local errors
    errors.extend(op_errors)

    return result


def db_bulk_update(errors: list[str] | None,
                   update_stmt: str,
                   update_vals: list[tuple],
                   engine: str = None,
                   connection: Any = None,
                   committable: bool = True,
                   logger: Logger = None) -> int:
    """
    Insert the tuples, with values defined in *insert_vals*, into the database.

    The target database engine, default or specified, must have been previously configured.

    The binding is done by position. Thus, the binding clauses in *update_stmt* must contain as many
    placeholders as there are elements in the tuples found in the list provided in *update_vals*.
    This is applicable for *mysql*, *oracle*, and *sqlserver*, where the placeholders are
    '%VARn%, ':n', and '?', respectively, and 'n' is the 1-based position of the data in the tuple.
    Note that the placeholders in the *WHERE* clause will follow the ones in the *SET* clause.

    In the specific case of *postgres*, a unique syntax applies. The *VALUES* clause must be simply
    *VALUES %s*, and it is used in both *INSERT* and *UPDATE* operations. In the latter case,
    there is also a *FROM* clause to go along with it.

    :param errors: incidental error messages
    :param update_stmt: the UPDATE command
    :param update_vals: the list of values to update the database with, and to locate the tuple
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'connection' to be provided)
    :param logger: optional logger
    :return: the number of updated tuples, or None if an error occurred
    """
    # initialize the return variable
    result: int | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(op_errors, engine)

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

    # acknowledge eventual local errors
    errors.extend(op_errors)

    return result


def db_update_lob(errors: list[str],
                  lob_table: str,
                  lob_column: str,
                  pk_columns: list[str],
                  pk_vals: tuple,
                  lob_file: str | Path,
                  chunk_size: int,
                  engine: str = None,
                  connection: Any = None,
                  committable: bool = True,
                  logger: Logger = None) -> None:
    """
    Update a large binary objects (LOB) in the given table and column.

    :param errors: incidental error messages
    :param lob_table: the table to be update with the new LOB
    :param lob_column: the column to be updated with the new LOB
    :param pk_columns: columns making up a primary key, or a unique identifier for the tuple
    :param pk_vals: values with which to locate the tuple to be updated
    :param lob_file: file holding the LOB (a file object or a valid path)
    :param chunk_size: size in bytes of the data chunk to read/write, or 0 or None for no limit
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'connection' to be provided)
    :param logger: optional logger
    :return: number of LOBs effectively copied
    """
    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(op_errors, engine)
    
    if curr_engine == "mysql":
        pass
    elif curr_engine == "oracle":
        from . import oracle_pomes
        oracle_pomes.update_lob(errors=op_errors,
                                lob_table=lob_table,
                                lob_column=lob_column,
                                pk_columns=pk_columns,
                                pk_vals=pk_vals,
                                lob_file=lob_file,
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
                                  lob_file=lob_file,
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
                                   lob_file=lob_file,
                                   chunk_size=chunk_size,
                                   conn=connection,
                                   committable=committable,
                                   logger=logger)

    # acknowledge eventual local errors
    errors.extend(op_errors)


def db_execute(errors: list[str] | None,
               exc_stmt: str,
               bind_vals: tuple = None,
               engine: str = None,
               connection: Any = None,
               committable: bool = True,
               logger: Logger = None) -> int:
    """
    Execute the command *exc_stmt* on the database.

    This command might be a DML ccommand modifying the database, such as
    inserting, updating or deleting tuples, or it might be a DDL statement,
    or it might even be an environment-related command.
    The optional bind values for this operation are in *bind_vals*.
    The value returned is the value obtained from the execution of *exc_stmt*.
    It might be the number of inserted, modified, or deleted tuples,
    ou None if an error occurred.

    :param errors: incidental error messages
    :param exc_stmt: the command to execute
    :param bind_vals: optional bind values
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'connection' to be provided)
    :param logger: optional logger
    :return: the return value from the command execution
    """
    # initialize the return variable
    result: int | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(op_errors, engine)
    
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

    # acknowledge eventual local errors
    errors.extend(op_errors)

    return result


def db_call_function(errors: list[str] | None,
                     func_name: str,
                     func_vals: tuple = None,
                     engine: str = None,
                     connection: Any = None,
                     committable: bool = True,
                     logger: Logger = None) -> list[tuple]:
    """
    Execute the stored function *func_name* in the database, with the parameters given in *func_vals*.

    The target database engine, default or specified, must have been previously configured.

    :param errors: incidental error messages
    :param func_name: name of the stored function
    :param func_vals: parameters for the stored function
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'connection' to be provided)
    :param logger: optional logger
    :return: the data returned by the function
    """
    # initialize the return variable
    result: Any = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(op_errors, engine)
    
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

    # acknowledge eventual local errors
    errors.extend(op_errors)

    return result


def db_call_procedure(errors: list[str] | None,
                      proc_name: str,
                      proc_vals: tuple = None,
                      engine: str = None,
                      connection: Any = None,
                      committable: bool = True,
                      logger: Logger = None) -> list[tuple]:
    """
    Execute the stored procedure *proc_name* in the database, with the parameters given in *proc_vals*.

    The target database engine, default or specified, must have been previously configured.

    :param errors: incidental error messages
    :param proc_name: name of the stored procedure
    :param proc_vals: parameters for the stored procedure
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'connection' to be provided)
    :param logger: optional logger
    :return: the data returned by the procedure
    """
    # initialize the return variable
    result: Any = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(op_errors, engine)
    
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

    # acknowledge eventual local errors
    errors.extend(op_errors)

    return result
