import oracledb
from logging import Logger
from oracledb import Connection, init_oracle_client, makedsn
from pathlib import Path
from typing import BinaryIO

from .db_common import (
    _DB_CONN_DATA,
    _assert_query_quota, _build_query_msg, _get_params, _except_msg
)
def get_connection_string() -> str:
    """
    Build and return the connection string for connecting to the database.

    :return: the connection string
    """
    # retrieve the connection parameters
    name, user, pwd, host, port = _get_params("oracle")

    # build and return the connection string
    dsn: str = makedsn(host=host,
                       port=port,
                       service_name=name)
    return f"oracle+oracledb://{user}:{pwd}@{dsn}"


def connect(errors: list[str],
            autocommit: bool | None,
            logger: Logger | None) -> Connection:
    """
    Obtain and return a connection to the database.

    Return *None* if the connection could not be obtained.

    :param errors: incidental error messages
    :param autocommit: whether the connection is to be in autocommit mode (defaults to False)
    :param logger: optional logger
    :return: the connection to the database
    """
    # initialize the return variable
    result: Connection | None = None

    # retrieve the connection parameters
    name, user, pwd, host, port = _get_params("oracle")

    # obtain a connection to the database
    err_msg: str | None = None
    try:
        result = oracledb.connect(service_name=name,
                                  host=host,
                                  port=port,
                                  user=user,
                                  password=pwd)
        # establish the connection's autocommit mode
        result.autocommit = isinstance(autocommit, bool) and autocommit
    except Exception as e:
        err_msg = _except_msg(exception=e,
                              engine="oracle")
    # log eventual errors
    if err_msg:
        if isinstance(errors, list):
            errors.append(err_msg)
        if logger:
            logger.error(msg=f"Error connecting to '{name}' at '{host}'")

    return result


def select(errors: list[str] | None,
           sel_stmt: str,
           where_vals: tuple | None,
           min_count: int | None,
           max_count: int | None,
           require_count: int | None,
           conn: Connection | None,
           committable: bool | None,
           logger: Logger | None) -> list[tuple]:
    """
    Search the database and return all tuples that satisfy the *sel_stmt* search command.

    The command can optionally contain search criteria, with respective values given in *where_vals*.
    If not positive integers, *min_count*, *max_count*, and *require_count* are ignored.
    If *require_count* is specified, then exactly that number of tuples must be
    returned by the query. If the search is empty, an empty list is returned.
    If the search is empty, an empty list is returned.
    The parameter *committable* is relevant only if *conn* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param sel_stmt: SELECT command for the search
    :param where_vals: the values to be associated with the search criteria
    :param min_count: optionally defines the minimum number of tuples to be returned
    :param max_count: optionally defines the maximum number of tuples to be returned
    :param require_count: number of tuples that must exactly satisfy the query (overrides 'min_count' and 'max_count')
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: list of tuples containing the search result, '[]' if the search was empty, or 'None' if there was an error
    """
    # initialize the return variable
    result: list[tuple] = []

    # make sure to have a connection
    curr_conn: Connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)
    if curr_conn:
        # establish the right query cardinality
        if isinstance(require_count, int) and require_count > 0:
            min_count = require_count
            max_count = require_count + 1

        if isinstance(max_count, int) and max_count > 0:
            sel_stmt: str = f"{sel_stmt} FETCH NEXT {max_count} ROWS ONLY"

        err_msg: str | None = None
        try:
            # obtain a cursor and perform the operation
            with curr_conn.cursor() as cursor:
                # execute the query
                cursor.execute(statement=sel_stmt,
                               parameters=where_vals)
                rows: list[tuple] = cursor.fetchall()
                # obtain the number of tuples returned
                count: int = len(rows)

                # has the query quota been satisfied ?
                if _assert_query_quota(errors=errors,
                                       engine="oracle",
                                       query=sel_stmt,
                                       where_vals=where_vals,
                                       count=count,
                                       min_count=min_count,
                                       max_count=max_count,
                                       require_count=require_count):
                    # yes, retrieve the returned tuples
                    if count == 1 and sel_stmt.upper().startswith("SELECT DBMS_METADATA.GET_DDL"):
                        # in this instance, a CLOB may be returned
                        result = [(str(rows[0][0]),)]
                    else:
                        result = rows
            # commit the transaction, if appropriate
            if committable or not conn:
                curr_conn.commit()
        except Exception as e:
            if curr_conn:
                curr_conn.rollback()
            err_msg = _except_msg(exception=e,
                                  engine="oracle")
        finally:
            # close the connection, if locally acquired
            if curr_conn and not conn:
                curr_conn.close()

        # log eventual errors
        if err_msg:
            if isinstance(errors, list):
                errors.append(err_msg)
            if logger:
                logger.error(msg=_build_query_msg(query_stmt=sel_stmt,
                                                  engine="oracle",
                                                  bind_vals=where_vals))
    return result


def execute(errors: list[str] | None,
            exc_stmt: str,
            bind_vals: tuple | None,
            conn: Connection | None,
            committable: bool | None,
            logger: Logger | None) -> int:
    """
    Execute the command *exc_stmt* on the database.

    This command might be a DML ccommand modifying the database, such as
    inserting, updating or deleting tuples, or it might be a DDL statement,
    or it might even be an environment-related command.
    The optional bind values for this operation are in *bind_vals*.
    The value returned is the value obtained from the execution of *exc_stmt*.
    It might be the number of inserted, modified, or deleted tuples,
    ou None if an error occurred.
    The parameter *committable* is relevant only if *conn* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param exc_stmt: the command to execute
    :param bind_vals: optional bind values
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: the return value from the command execution
    """
    # initialize the return variable
    result: int | None = None

    # make sure to have a connection
    curr_conn: Connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)
    if curr_conn:
        err_msg: str | None = None
        try:
            # obtain a cursor and execute the operation
            with curr_conn.cursor() as cursor:
                cursor.execute(statement=exc_stmt,
                               parameters=bind_vals)
                result = cursor.rowcount

            # commit the transaction, if appropriate
            if committable or not conn:
                curr_conn.commit()
        except Exception as e:
            if curr_conn:
                curr_conn.rollback()
            err_msg = _except_msg(exception=e,
                                  engine="oracle")
        finally:
            # close the connection, if locally acquired
            if curr_conn and not conn:
                curr_conn.close()

        # log eventual errors
        if err_msg and isinstance(errors, list):
            errors.append(err_msg)
        if logger:
            logger.debug(msg=_build_query_msg(query_stmt=exc_stmt,
                                              engine="oracle",
                                              bind_vals=bind_vals))
    return result


def bulk_execute(errors: list[str] | None,
                 exc_stmt: str,
                 exc_vals: list[tuple],
                 conn: Connection | None,
                 committable: bool | None,
                 logger: Logger | None) -> int:
    """
    Bulk-update the database with the statement defined in *execute_stmt*, and the values in *execute_vals*.

    The binding is done by position. Thus, the binding clauses in *execute_stmt* must contain
    as many ':n' placeholders as there are elements in the tuples found in the list provided in
    *execute_vals*, where 'n' is the 1-based position of the data in the tuple.
    Note that, in *UPDATE* operations, the placeholders in the *WHERE* clause will follow
    the ones in the *SET* clause.
    The parameter *committable* is relevant only if *conn* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param exc_stmt: the command to update the database with
    :param exc_vals: the list of values for tuple identification, and to update the database with
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: the number of inserted or updated tuples, or None if an error occurred
    """
    # initialize the return variable
    result: int | None = None

    # make sure to have a connection
    curr_conn: Connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)
    if curr_conn:
        err_msg: str | None = None
        try:
            # obtain a cursor and perform the operation
            with curr_conn.cursor() as cursor:
                cursor.executemany(statement=exc_stmt,
                                   parameters=exc_vals)
                result = len(exc_vals)

            # commit the transaction, if appropriate
            if committable or not conn:
                curr_conn.commit()
        except Exception as e:
            if curr_conn:
                curr_conn.rollback()
            err_msg = _except_msg(exception=e,
                                  engine="oracle")
        finally:
            # close the connection, if locally acquired
            if curr_conn and not conn:
                curr_conn.close()

        # log eventual errors
        if err_msg:
            if isinstance(errors, list):
                errors.append(err_msg)
            if logger:
                logger.error(msg=_build_query_msg(query_stmt=exc_stmt,
                                                  engine="oracle",
                                                  bind_vals=exc_vals[0]))
    return result


def update_lob(errors: list[str],
               lob_table: str,
               lob_column: str,
               pk_columns: list[str],
               pk_vals: tuple,
               lob_data: bytes | str | Path | BinaryIO,
               chunk_size: int,
               conn: Connection | None,
               committable: bool | None,
               logger: Logger | None) -> None:
    """
    Update a large binary object (LOB) in the given table and column.

    The data for the update may come from *bytes*, from a *Path* or its string representation,
    or from a pointer obtained from *BytesIO* or *Path.open()* in binary mode.
    The parameter *committable* is relevant only if *conn* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param lob_table: the table to be update with the new LOB
    :param lob_column: the column to be updated with the new LOB
    :param pk_columns: columns making up a primary key, or a unique identifier for the tuple
    :param pk_vals: values with which to locate the tuple to be updated
    :param lob_data: the LOB data (bytes, a file path, or a file pointer)
    :param chunk_size: size in bytes of the data chunk to read/write, or 0 or None for no limit
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    """
    # make sure to have a connection
    curr_conn: Connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)
    if curr_conn:
        if isinstance(lob_data, str):
            lob_data = Path(lob_data)

        # normalize the chunk size
        if not chunk_size:
            chunk_size = -1

        # build the UPDATE query
        where_clause: str = " AND ".join([f"{column} = :{inx}"
                                          for column, inx in enumerate(iterable=pk_columns,
                                                                       start=2)])
        update_stmt: str = (f"UPDATE {lob_table} "
                            f"SET {lob_column} = :1 "
                            f"WHERE {where_clause}")

        err_msg: str | None = None
        try:
            # obtain a cursor and execute the operation
            with curr_conn.cursor() as cursor:

                # retrieve the lob data and write to the database
                if isinstance(lob_data, bytes):
                    cursor.execute(statement=update_stmt,
                                   parameters=(lob_data, *pk_vals))
                elif isinstance(lob_data, Path):
                    data_bytes: bytes
                    with lob_data.open("rb") as file:
                        data_bytes = file.read(chunk_size)
                        while data_bytes:
                            cursor.execute(statement=update_stmt,
                                           parameters=(data_bytes, *pk_vals))
                            data_bytes = file.read(chunk_size)
                else:
                    data_bytes: bytes = lob_data.read(chunk_size)
                    while data_bytes:
                        cursor.execute(statement=update_stmt,
                                       parameters=(data_bytes, *pk_vals))
                        data_bytes = lob_data.read(chunk_size)
                    lob_data.close()


            # commit the transaction, if appropriate
            if committable or not conn:
                curr_conn.commit()
        except Exception as e:
            if curr_conn:
                curr_conn.rollback()
            err_msg = _except_msg(exception=e,
                                  engine="oracle")
        finally:
            # close the connection, if locally acquired
            if curr_conn and not conn:
                curr_conn.close()

        # log eventual errors
        if err_msg:
            if isinstance(errors, list):
                errors.append(err_msg)
            if logger:
                logger.error(msg=_build_query_msg(query_stmt=update_stmt,
                                                  engine="oracle",
                                                  bind_vals=pk_vals))


# TODO: see https://python-oracledb.readthedocs.io/en/latest/user_guide/plsql_execution.html
# noinspection PyUnusedLocal
def call_function(errors: list[str] | None,
                  func_name: str,
                  func_vals: tuple | None,
                  conn: Connection | None,
                  committable: bool | None,
                  logger: Logger | None) -> list[tuple]:
    """
    Execute the stored function *func_name* in the database, with the parameters given in *func_vals*.

    The parameter *committable* is relevant only if *conn* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param func_name: name of the stored function
    :param func_vals: parameters for the stored function
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: the data returned by the function
    """
    # initialize the return variable
    result: list[tuple] = []

    return result


# TODO: see https://python-oracledb.readthedocs.io/en/latest/user_guide/plsql_execution.html
def call_procedure(errors: list[str] | None,
                   proc_name: str,
                   proc_vals: tuple | None,
                   conn: Connection | None,
                   committable: bool | None,
                   logger: Logger | None) -> list[tuple]:
    """
    Execute the stored procedure *proc_name* in the database, with the parameters given in *proc_vals*.

    The parameter *committable* is relevant only if *conn* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param proc_name: name of the stored procedure
    :param proc_vals: parameters for the stored procedure
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: the data returned by the procedure
    """
    # initialize the return variable
    result: list[tuple] = []

    # make sure to have a connection
    curr_conn: Connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)
    if curr_conn:
        # execute the stored procedure
        err_msg: str | None = None
        try:
            # obtain a cursor and perform the operation
            with curr_conn.cursor() as cursor:
                cursor.callproc(name=proc_name,
                                parameters=proc_vals)

                # retrieve the returned tuples
                result = list(cursor)

            # commit the transaction, if appropriate
            if committable or not conn:
                curr_conn.commit()
        except Exception as e:
            if curr_conn:
                curr_conn.rollback()
            err_msg = _except_msg(exception=e,
                                  engine="oracle")
        finally:
            # close the connection, if locally acquired
            if curr_conn and not conn:
                curr_conn.close()

        # log eventual errors
        if err_msg:
            if isinstance(errors, list):
                errors.append(err_msg)
            if logger:
                logger.error(msg=_build_query_msg(query_stmt=proc_name,
                                                  engine="oracle",
                                                  bind_vals=proc_vals))
    return result


__is_initialized: str | None = None

def initialize(errors: list[str],
               logger: Logger) -> bool:
    """
    Prepare the oracle engine to access the database throught the installed client software.

    :param errors: incidental error messages
    :param logger: optional logger
    :return: False if an error happened, True otherwise
    """
    # initialize the return variable
    result: bool = True

    global __is_initialized
    if not __is_initialized:
        err_msg: str | None = None
        client: Path = _DB_CONN_DATA["oracle"]["client"]
        try:
            init_oracle_client(client.as_posix())
            __is_initialized = True
        except Exception as e:
            result = False
            err_msg = _except_msg(exception=e,
                                  engine="oracle")
        # log the results
        if err_msg and isinstance(errors, list):
            errors.append(err_msg)
        if logger:
            logger.debug(msg="Initializing the client")

    return result
