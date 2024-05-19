# noinspection DuplicatedCode
from logging import Logger
from pyodbc import Binary, Connection, Row, connect
from pathlib import Path

from .db_common import (
    _db_assert_query_quota, _db_get_params, _db_log, _db_except_msg
)


def _connect(errors: list[str],
             logger: Logger = None) -> Connection:
    """
    Obtain and return a connection to the database, or *None* if the connection could not be obtained.

    :param errors: incidental error messages
    :param logger: optional logger
    :return: the connection to the database
    """
    # initialize the return valiable
    result: Connection | None = None

    # retrieve the connection parameters and build the connection string
    name, user, pwd, host, port, driver = _db_get_params("sqlserver")
    connection_kwargs: str = (
        f"DRIVER={{{driver}}};SERVER={host},{port};"
        f"DATABASE={name};UID={user};PWD={pwd};TrustServerCertificate=yes;"
    )

    # obtain a connection to the database
    err_msg: str | None = None
    try:
        result = connect(connection_kwargs)
        # make sure the connection is not in autocommit mode
        result.autocommit = False
    except Exception as e:
        err_msg = _db_except_msg(exception=e,
                                 engine="sqlserver")

    # log the results
    _db_log(logger=logger,
            engine="sqlserver",
            err_msg=err_msg,
            errors=errors,
            stmt=f"Connecting to '{name}' at '{host}'")

    return result


def _select_all(errors: list[str],
                sel_stmt: str,
                where_vals: tuple,
                require_min: int,
                require_max: int,
                conn: Connection,
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
    :param logger: optional logger
    :return: tuple containing the search result, [] if the search was empty, or None if there was an error
    """
    # initialize the return variable
    result: list[tuple] = []

    # make sure to have a connection
    curr_conn: Connection = conn or _connect(errors=errors,
                                             logger=logger)

    err_msg: str | None = None
    if isinstance(require_max, int) and require_max > 0:
        sel_stmt: str = sel_stmt.replace("SELECT", f"SELECT TOP {require_max}", 1)

    try:
        # obtain a cursor and execute the operation
        with curr_conn.cursor() as cursor:
            cursor.execute(sel_stmt, where_vals)
            # obtain the number of tuples returned
            count: int = cursor.rowcount

            # has the query quota been satisfied ?
            if _db_assert_query_quota(errors=errors,
                                      engine="sqlserver",
                                      query=sel_stmt,
                                      where_vals=where_vals,
                                      count=count,
                                      require_min=require_min,
                                      require_max=require_max):
                # yes, retrieve the returned tuples
                rows: list[Row] = cursor.fetchall()
                result = [tuple(row) for row in rows]
        # commit the transaction
        curr_conn.commit()
    except Exception as e:
        if curr_conn:
            curr_conn.rollback()
        err_msg = _db_except_msg(exception=e,
                                 engine="sqlserver")
    finally:
        # close the connection, if locally acquired
        if curr_conn and not conn:
            curr_conn.close()

    # log eventual errors
    if errors or err_msg:
        _db_log(logger=logger,
                engine="sqlserver",
                err_msg=err_msg,
                errors=errors,
                stmt=sel_stmt,
                bind_vals=where_vals)

    return result


def _bulk_insert(errors: list[str],
                 insert_stmt: str,
                 insert_vals: list[tuple],
                 conn: Connection,
                 logger: Logger) -> int:
    """
    Insert the tuples, with values defined in *insert_vals*, into the database.

    The *VALUES* clause in *insert_stmt* must contain as many '%s' placeholders
    as there are elements in the tuples found in the list provided in *insert_vals*.

    :param errors: incidental error messages
    :param insert_stmt: the INSERT command
    :param insert_vals: the list of values to be inserted
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param logger: optional logger
    :return: the number of inserted tuples, or None if an error occurred
    """
    # initialize the return variable
    result: int | None = None

    # make sure to have a connection
    curr_conn: Connection = conn or _connect(errors=errors,
                                             logger=logger)

    err_msg: str | None = None
    try:
        # obtain a cursor and perform the operation
        with curr_conn.cursor() as cursor:
            cursor.fast_executemany = True
            try:
                cursor.executemany(insert_stmt, insert_vals)
                result = len(insert_vals)
            except Exception:
                conn.rollback()
                raise
        curr_conn.commit()
    except Exception as e:
        if curr_conn:
            curr_conn.rollback()
        err_msg = _db_except_msg(exception=e,
                                 engine="sqlserver")
    finally:
        # close the connection, if locally acquired
        if curr_conn and not conn:
            curr_conn.close()

    # log eventual errors
    if errors or err_msg:
        _db_log(logger=logger,
                engine="sqlserver",
                err_msg=err_msg,
                errors=errors,
                stmt=insert_stmt,
                bind_vals=insert_vals[0])

    return result


def _update_lob(errors: list[str],
                lob_table: str,
                lob_column: str,
                pk_columns: list[str],
                pk_vals: tuple,
                lob_file: str | Path,
                chunk_size: int,
                conn: Connection,
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
    :param logger: optional logger
    """
    # make sure to have a connection
    curr_conn: Connection = conn or _connect(errors=errors,
                                             logger=logger)

    # make sure to have a data file
    data_file: Path = Path(lob_file) if isinstance(lob_file, str) else lob_file

    # normalize the chunk size
    if not chunk_size:
        chunk_size = -1

    # build the UPDATE query
    where_clause: str = " AND ".join([f"{column} = ?" for column in pk_columns])
    update_stmt: str = (f"UPDATE {lob_table} "
                        f"SET {lob_column} = ? "
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

        # commit the transaction
        curr_conn.commit()
    except Exception as e:
        if curr_conn:
            curr_conn.rollback()
        err_msg = _db_except_msg(exception=e,
                                 engine="sqlserver")
    finally:
        # close the connection, if locally acquired
        if curr_conn and not conn:
            curr_conn.close()

    # log eventual errors
    if errors or err_msg:
        _db_log(logger=logger,
                engine="sqlserver",
                err_msg=err_msg,
                errors=errors,
                stmt=update_stmt,
                bind_vals=pk_vals)


def _execute(errors: list[str],
             exc_stmt: str,
             bind_vals: tuple,
             conn: Connection,
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
    :param logger: optional logger
    :return: the return value from the command execution
    """
    # initialize the return variable
    result: int | None = None

    # make sure to have a connection
    curr_conn: Connection = conn or _connect(errors=errors,
                                             logger=logger)

    err_msg: str | None = None
    try:
        # obtain a cursor and execute the operation
        with curr_conn.cursor() as cursor:
            cursor.execute(exc_stmt, bind_vals)
            result = cursor.rowcount
        # commit the transaction
        curr_conn.commit()
    except Exception as e:
        if curr_conn:
            curr_conn.rollback()
        err_msg = _db_except_msg(exception=e,
                                 engine="sqlserver")
    finally:
        # close the connection, if locally acquired
        if curr_conn and not conn:
            curr_conn.close()

    # log eventual errors
    if errors or err_msg:
        _db_log(logger=logger,
                engine="sqlserver",
                err_msg=err_msg,
                errors=errors,
                stmt=exc_stmt,
                bind_vals=bind_vals)

    return result


def _call_procedure(errors: list[str],
                    proc_name: str,
                    proc_vals: tuple,
                    conn: Connection,
                    logger: Logger = None) -> list[tuple]:
    """
    Execute the stored procedure *proc_name* in the database, with the parameters given in *proc_vals*.

    :param errors: incidental error messages
    :param proc_name: name of the stored procedure
    :param proc_vals: parameters for the stored procedure
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param logger: optional logger
    :return: the data returned by the procedure
    """
    # initialize the return variable
    result: list[tuple] | None = None

    # make sure to have a connection
    curr_conn: Connection = conn or _connect(errors=errors,
                                             logger=logger)

    # build the command
    proc_stmt: str | None = None

    # execute the stored procedure
    err_msg: str | None = None
    try:
        # obtain a cursor and execute the operation
        with curr_conn.cursor() as cursor:
            proc_stmt = f"SET NOCOUNT ON; EXEC {proc_name} {','.join(('?',) * len(proc_vals))}"
            cursor.execute(proc_stmt, proc_vals)
            # retrieve the returned tuples
            rows: list[Row] = cursor.fetchall()
            result = [tuple(row) for row in rows]
        # commit the transaction
        curr_conn.commit()
    except Exception as e:
        if curr_conn:
            curr_conn.rollback()
        err_msg = _db_except_msg(exception=e,
                                 engine="sqlserver")
    finally:
        # close the connection, if locally acquired
        if curr_conn and not conn:
            curr_conn.close()

    # log eventual errors
    if errors or err_msg:
        _db_log(logger=logger,
                engine="sqlserver",
                err_msg=err_msg,
                errors=errors,
                stmt=proc_stmt,
                bind_vals=proc_vals)

    return result
