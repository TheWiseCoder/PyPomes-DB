import psycopg2
from logging import WARNING, Logger
from pathlib import Path
from psycopg2 import Binary
from psycopg2.extras import execute_values
# noinspection PyProtectedMember
from psycopg2._psycopg import connection
from typing import Any

from .db_common import (
    _assert_query_quota, _get_params,
    _log, _except_msg, _remove_nulls
)


def get_connection_string() -> str:
    """
    Build and return the connection string for connecting to the database.

    :return: the connection string
    """
    # retrieve the connection parameters
    name, user, pwd, host, port = _get_params("postgres")

    # build and return the connection string
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}"


def connect(errors: list[str],
            autocommit: bool | None,
            logger: Logger | None) -> connection:
    """
    Obtain and return a connection to the database.

    Return *None* if the connection could not be obtained.

    :param errors: incidental error messages
    :param autocommit: whether the connection is to be in autocommit mode (defaults to False)
    :param logger: optional logger
    :return: the connection to the database
    """
    # initialize the return variable
    result: connection | None = None

    # retrieve the connection parameters
    name, user, pwd, host, port = _get_params("postgres")

    # obtain a connection to the database
    err_msg: str | None = None
    try:
        result = psycopg2.connect(host=host,
                                  port=port,
                                  database=name,
                                  user=user,
                                  password=pwd)
        # establish the connection's autocommit mode
        result.autocommit = isinstance(autocommit, bool) and autocommit
    except Exception as e:
        err_msg = _except_msg(e, "postgres")

    # log the results
    _log(logger=logger,
         engine="postgres",
         err_msg=err_msg,
         errors=errors,
         stmt=f"Connecting to '{name}' at '{host}'")

    return result


def select(errors: list[str] | None,
           sel_stmt: str,
           where_vals: tuple | None,
           min_count: int | None,
           max_count: int | None,
           require_count: int | None,
           conn: connection | None,
           committable: bool | None,
           logger: Logger | None) -> list[tuple]:
    """
    Search the database and return all tuples that satisfy the *sel_stmt* search command.

    The command can optionally contain search criteria, with respective values given in
    *where_vals*. The list of values for an attribute with the *IN* clause must be contained in a
    specific tuple. If not positive integers, *min_count*, *max_count*, and *require_count" are ignored.
    If *require_count* is specified, then exactly that number of touples must be
    returned by the query. If the search is empty, an empty list is returned.
    If the search is empty, an empty list is returned.

    :param errors: incidental error messages
    :param sel_stmt: SELECT command for the search
    :param where_vals: the values to be associated with the search criteria
    :param min_count: optionally defines the minimum number of tuples to be returned
    :param max_count: optionally defines the maximum number of tuples to be returned
    :param require_count: number of touples that must exactly satisfy the query (overrides 'min_count' and 'max_count')
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'conn' to be provided)
    :param logger: optional logger
    :return: list of tuples containing the search result, '[]' if the search was empty, or 'None' if there was an error
    """
    # initialize the return variable
    result: list[tuple] = []

    # make sure to have a connection
    curr_conn: connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)

    # make sure to establish the right query cardinality
    if isinstance(require_count, int) and require_count > 0:
        min_count = require_count
        max_count = require_count + 1

    if isinstance(max_count, int) and max_count >= 0:
        sel_stmt += f" LIMIT {max_count}"

    err_msg: str | None = None
    try:
        # obtain a cursor and execute the operation
        with curr_conn.cursor() as cursor:
            cursor.execute(query=f"{sel_stmt};",
                           vars=where_vals)
            # obtain the number of tuples returned
            count: int = cursor.rowcount

            # has the query quota been satisfied ?
            if _assert_query_quota(errors=errors,
                                   engine="postgres",
                                   query=sel_stmt,
                                   where_vals=where_vals,
                                   count=count,
                                   min_count=min_count,
                                   max_count=max_count,
                                   require_count=require_count):
                # yes, retrieve the returned tuples
                result = list(cursor)

        # commit the transaction, if appropriate
        if committable or not conn:
            curr_conn.commit()
    except Exception as e:
        if curr_conn:
            curr_conn.rollback()
        err_msg = _except_msg(exception=e,
                              engine="postgres")
    finally:
        # close the connection, if locally acquired
        if curr_conn and not conn:
            curr_conn.close()

    # log eventual errors
    if errors or err_msg:
        _log(logger=logger,
             engine="postgres",
             err_msg=err_msg,
             errors=errors,
             stmt=sel_stmt,
             bind_vals=where_vals)

    return result


def execute(errors: list[str] | None,
            exc_stmt: str,
            bind_vals: tuple | None,
            conn: connection | None,
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
    curr_conn: connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)

    err_msg: str | None = None
    try:
        # obtain a cursor and execute the operation
        with curr_conn.cursor() as cursor:
            cursor.execute(query=f"{exc_stmt};",
                           vars=bind_vals)
            result = cursor.rowcount

        # commit the transaction, if appropriate
        if committable or not conn:
            curr_conn.commit()
    except Exception as e:
        if curr_conn:
            curr_conn.rollback()
        err_msg = _except_msg(exception=e,
                              engine="postgres")
    finally:
        # close the connection, if locally acquired
        if curr_conn and not conn:
            curr_conn.close()

    # log eventual errors
    if errors or err_msg:
        _log(logger=logger,
             engine="postgres",
             err_msg=err_msg,
             errors=errors,
             stmt=exc_stmt,
             bind_vals=bind_vals)

    return result


def bulk_execute(errors: list[str],
                 exc_stmt: str,
                 exc_vals: list[tuple],
                 conn: connection,
                 committable: bool,
                 logger: Logger) -> int:
    """
    Bulk-update the database with the statement defined in *execute_stmt*, and the values in *execute_vals*.

    For *INSERT* operations, the *VALUES* clause must be simply *VALUES %s*. *UPDATE* operations
    require a sepcial syntax, with *VALUES %s* combined with a *FROM* clause.

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
    curr_conn: connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)

    # execute the bulk insert
    err_msg: str | None = None
    try:
        # obtain a cursor and perform the operation
        with curr_conn.cursor() as cursor:
            # after the successful execution of 'execute_values', 'cursor.rowcount' has the value 1 (one)
            execute_values(cur=cursor,
                           sql=exc_stmt,
                           argslist=exc_vals)
            result = len(exc_vals)

        # commit the transaction, if appropriate
        if committable or not conn:
            curr_conn.commit()
    except ValueError as e:
        curr_conn.rollback()
        # is the exception ValueError("A string literal cannot contain NUL (0x00) characters.") ?
        if "contain NUL" in e.args[0]:
            # yes, log the occurrence, remove the NULLs, and try again
            _log(logger=logger,
                 engine="postgres",
                 level=WARNING,
                 stmt=f"Found NULLs in values for {exc_stmt}")
            # search for NULLs in input data
            cleaned_rows: list[tuple[int, list]] = []
            for inx, vals in enumerate(exc_vals):
                cleaned_row: list[Any] = _remove_nulls(vals)
                # has the row been cleaned ?
                if cleaned_row:
                    # yes, register it
                    cleaned_rows.append((inx, cleaned_row))
            # replace the cleaned rows
            for cleaned_row in cleaned_rows:
                exc_vals[cleaned_row[0]] = tuple(cleaned_row[1])
            # bulk insert the cleaned data
            bulk_execute(errors=errors,
                         exc_stmt=exc_stmt,
                         exc_vals=exc_vals,
                         conn=conn,
                         committable=committable,
                         logger=logger)
    except Exception as e:
        if curr_conn:
            curr_conn.rollback()
        err_msg = _except_msg(exception=e,
                              engine="postgres")
    finally:
        # close the connection, if locally acquired
        if curr_conn and not conn:
            curr_conn.close()

    # log eventual errors
    if errors or err_msg:
        _log(logger=logger,
             engine="postgres",
             err_msg=err_msg,
             errors=errors,
             stmt=exc_stmt)

    return result


def update_lob(errors: list[str],
               lob_table: str,
               lob_column: str,
               pk_columns: list[str],
               pk_vals: tuple,
               lob_file: str | Path,
               chunk_size: int,
               conn: connection | None,
               committable: bool | None,
               logger: Logger | None) -> None:
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
    curr_conn: connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)

    # make sure to have a data file
    data_file: Path = Path(lob_file) if isinstance(lob_file, str) else lob_file

    # normalize the chunk size
    if not chunk_size:
        chunk_size = -1

    # build the UPDATE query
    where_clause: str = " AND ".join([f"{column} = %s" for column in pk_columns])
    update_stmt: str = (f"UPDATE {lob_table} "
                        f"SET {lob_column} = %s "
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
                    cursor.execute(update_stmt, (Binary(lob_data), *pk_vals))
                    lob_data = file.read(chunk_size)

        # commit the transaction, if appropriate
        if committable or not conn:
            curr_conn.commit()
    except Exception as e:
        if curr_conn:
            curr_conn.rollback()
        err_msg = _except_msg(exception=e,
                              engine="postgres")
    finally:
        # close the connection, if locally acquired
        if curr_conn and not conn:
            curr_conn.close()

    # log eventual errors
    if errors or err_msg:
        _log(logger=logger,
             engine="postgres",
             err_msg=err_msg,
             errors=errors,
             stmt=update_stmt,
             bind_vals=pk_vals)


def call_procedure(errors: list[str] | None,
                   proc_name: str,
                   proc_vals: tuple | None,
                   conn: connection | None,
                   committable: bool | None,
                   logger: Logger | None) -> list[tuple]:
    """
    Execute the stored procedure *proc_name*, with the arguments given in *proc_vals*.

    :param errors:  incidental error messages
    :param proc_name: the name of the sotred procedure
    :param proc_vals: the arguments to be passed
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'conn' to be provided)
    :param logger: optional logger
    :return: the data returned by the procedure
    """
    # initialize the return variable
    result: list[tuple] = [()]

    # make sure to have a connection
    curr_conn: connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)

    # build the command
    proc_stmt: str = f"{proc_name}(" + "%s, " * (len(proc_vals) - 1) + "%s)"

    # execute the stored procedure
    err_msg: str | None = None
    try:
        # obtain a cursor and perform the operation
        with curr_conn.cursor() as cursor:
            cursor.execute(query=proc_stmt,
                           vars=proc_vals)
            # retrieve the returned tuples
            result = list(cursor)

        # commit the transaction, if appropriate
        if committable or not conn:
            curr_conn.commit()
    except Exception as e:
        if curr_conn:
            curr_conn.rollback()
        err_msg = _except_msg(exception=e,
                              engine="postgres")
    finally:
        # close the connection, if locally acquired
        if curr_conn and not conn:
            curr_conn.close()

    # log eventual errors
    if errors or err_msg:
        _log(logger=logger,
             engine="postgres",
             err_msg=err_msg,
             errors=errors,
             stmt=proc_stmt,
             bind_vals=proc_vals)

    return result
