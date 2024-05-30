# noinspection DuplicatedCode
import oracledb
from logging import Logger
from oracledb import Connection, init_oracle_client
from pathlib import Path

from .db_common import (
    _DB_CONN_DATA,
    _assert_query_quota, _get_params, _log, _except_msg
)


def connect(errors: list[str],
            autocommit: bool,
            logger: Logger) -> Connection:
    """
    Obtain and return a connection to the database, or *None* if the connection could not be obtained.

    :param errors: incidental error messages
    :param autocommit: whether the connection is to be in autocommit mode
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
        result.autocommit = autocommit
    except Exception as e:
        err_msg = _except_msg(exception=e,
                              engine="oracle")

    # log the results
    _log(logger=logger,
         engine="oracle",
         err_msg=err_msg,
         errors=errors,
         stmt=f"Connecting to '{name}' at '{host}'")

    return result


def select_all(errors: list[str],
               sel_stmt: str,
               where_vals: tuple,
               require_min: int,
               require_max: int,
               conn: Connection,
               committable: bool,
               logger: Logger) -> list[tuple]:
    """
    Search the database and return all tuples that satisfy the *sel_stmt* search command.

    The command can optionally contain search criteria, with respective values given
    in *where_vals*. The list of values for an attribute with the *IN* clause must be contained
    in a specific tuple. If not positive integers, *require_min* and *require_max* are ignored.
    If the search is empty, an empty list is returned.

    :param errors: incidental error messages
    :param sel_stmt: SELECT command for the search
    :param where_vals: the values to be associated with the search criteria
    :param require_min: optionally defines the minimum number of tuples to be returned
    :param require_max: optionally defines the maximum number of tuples to be returned
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'conn' to be provided)
    :param logger: optional logger
    :return: tuple containing the search result, [] if the search was empty, or None if there was an error
    """
    # initialize the return variable
    result: list[tuple] = []

    # make sure to have a connection
    curr_conn: Connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)

    if isinstance(require_max, int) and require_max > 0:
        sel_stmt: str = f"{sel_stmt} FETCH NEXT {require_max} ROWS ONLY"

    err_msg: str | None = None
    try:
        # obtain a cursor and perform the operation
        with curr_conn.cursor() as cursor:
            # execute the query
            cursor.execute(statement=sel_stmt,
                           parameters=where_vals)
            # obtain the number of tuples returned
            count: int = cursor.rowcount

            # has the query quota been satisfied ?
            if _assert_query_quota(errors=errors,
                                   engine="oracle",
                                   query=sel_stmt,
                                   where_vals=where_vals,
                                   count=count,
                                   require_min=require_min,
                                   require_max=require_max):
                # yes, retrieve the returned tuples
                rows: list = cursor.fetchall()
                result = [tuple(row) for row in rows]
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
    if errors or err_msg:
        _log(logger=logger,
             engine="oracle",
             err_msg=err_msg,
             errors=errors,
             stmt=sel_stmt,
             bind_vals=where_vals)

    return result


def execute(errors: list[str],
            exc_stmt: str,
            bind_vals: tuple,
            conn: Connection,
            committable: bool,
            logger: Logger) -> int:
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
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'conn' to be provided)
    :param logger: optional logger
    :return: the return value from the command execution
    """
    # initialize the return variable
    result: int | None = None

    # make sure to have a connection
    curr_conn: Connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)

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
    if errors or err_msg:
        _log(logger=logger,
             engine="oracle",
             err_msg=err_msg,
             errors=errors,
             stmt=exc_stmt,
             bind_vals=bind_vals)

    return result


def bulk_execute(errors: list[str],
                 exc_stmt: str,
                 exc_vals: list[tuple],
                 conn: Connection,
                 committable: bool,
                 logger: Logger) -> int:
    """
    Bulk-update the database with the statement defined in *execute_stmt*, and the values in *execute_vals*.

    The binding is done by position. Thus, the binding clauses in *execute_stmt* must contain
    as many ':n' placeholders as there are elements in the tuples found in the list provided in
    *execute_vals*, where 'n' is the 1-based position of the data in the tuple.
    Note that, in *UPDATE* operations, the placeholders in the *WHERE* clause will follow
    the ones in the *SET* clause.

    :param errors: incidental error messages
    :param exc_stmt: the command to update the database with
    :param exc_vals: the list of values for tuple identification, and to update the database with
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'conn' to be provided)
    :param logger: optional logger
    :return: the number of inserted or updated tuples, or None if an error occurred
    """
    # initialize the return variable
    result: int | None = None

    # make sure to have a connection
    curr_conn: Connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)

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
    if errors or err_msg:
        _log(logger=logger,
             engine="oracle",
             err_msg=err_msg,
             errors=errors,
             stmt=exc_stmt,
             bind_vals=exc_vals[0])

    return result


def update_lob(errors: list[str],
               lob_table: str,
               lob_column: str,
               pk_columns: list[str],
               pk_vals: tuple,
               lob_file: str | Path,
               chunk_size: int,
               conn: Connection,
               committable: bool,
               logger: Logger) -> None:
    """
    Update a large binary objects (LOB) in the given table and column.

    :param errors: incidental error messages
    :param lob_table: the table to be update with the new LOB
    :param lob_column: the column to be updated with the new LOB
    :param pk_columns: columns making up a primary key, or a unique identifier for the tuple
    :param pk_vals: values with which to locate the tuple to be updated
    :param lob_file: file holding the LOB (a file object or a valid path)
    :param chunk_size: size in bytes of the data chunk to read/write, or 0 or None for no limit
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'conn' to be provided)
    :param logger: optional logger
    """
    # make sure to have a connection
    curr_conn: Connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)

    # make sure to have a data file
    data_file: Path = Path(lob_file) if isinstance(lob_file, str) else lob_file

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

            # retrieve the lob data from file in chunks and write to the file
            lob_data : bytes
            with data_file.open("rb") as file:
                lob_data = file.read(chunk_size)
                while lob_data:
                    cursor.execute(statement=update_stmt,
                                   parameters=(lob_data, *pk_vals))
                    lob_data = file.read(chunk_size)

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
    if errors or err_msg:
        _log(logger=logger,
             err_msg=err_msg,
             engine="oracle",
             errors=errors,
             stmt=update_stmt,
             bind_vals=pk_vals)


# TODO: see https://python-oracledb.readthedocs.io/en/latest/user_guide/plsql_execution.html
# noinspection PyUnusedLocal
def call_function(errors: list[str],
                  func_name: str,
                  func_vals: tuple,
                  conn: Connection,
                  committable: bool,
                  logger: Logger) -> list[tuple]:
    """
    Execute the stored function *func_name* in the database, with the parameters given in *func_vals*.

    :param errors: incidental error messages
    :param func_name: name of the stored function
    :param func_vals: parameters for the stored function
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'conn' to be provided)
    :param logger: optional logger
    :return: the data returned by the function
    """
    # initialize the return variable
    result: list[tuple] = []

    return result


# TODO: see https://python-oracledb.readthedocs.io/en/latest/user_guide/plsql_execution.html
def call_procedure(errors: list[str],
                   proc_name: str,
                   proc_vals: tuple,
                   conn: Connection,
                   committable: bool,
                   logger: Logger) -> list[tuple]:
    """
    Execute the stored procedure *proc_name* in the database, with the parameters given in *proc_vals*.

    :param errors: incidental error messages
    :param proc_name: name of the stored procedure
    :param proc_vals: parameters for the stored procedure
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'conn' to be provided)
    :param logger: optional logger
    :return: the data returned by the procedure
    """
    # initialize the return variable
    result: list[tuple] = []

    # make sure to have a connection
    curr_conn: Connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)

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
    if errors or err_msg:
        _log(logger=logger,
             engine="oracle",
             err_msg=err_msg,
             errors=errors,
             stmt=proc_name,
             bind_vals=proc_vals)

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
        client: str = _DB_CONN_DATA["oracle"]["client"]
        try:
            init_oracle_client(client)
            __is_initialized = True
        except Exception as e:
            result = False
            err_msg = _except_msg(exception=e,
                                  engine="oracle")
        # log the results
        _log(logger=logger,
             engine="oracle",
             err_msg=err_msg,
             errors=errors,
             stmt="Initializing the client")

    return result
