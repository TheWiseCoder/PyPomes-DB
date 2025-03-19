import pyodbc
from contextlib import suppress
from datetime import date, datetime
from logging import Logger
from pyodbc import Binary, Connection, Row
from pypomes_core import DATE_FORMAT_INV, DATETIME_FORMAT_INV
from pathlib import Path
from pypomes_core import str_between
from typing import Any, BinaryIO

from .db_common import (
    DbEngine, DbParam,
    _assert_query_quota, _build_query_msg, _get_params, _except_msg
)


def get_connection_string() -> str:
    """
    Build and return the connection string for connecting to the database.

    :return: the connection string
    """
    # retrieve the connection parameters
    db_params: dict[DbParam, Any] = _get_params(DbEngine.SQLSERVER)

    # build and return the connection string
    return (
        f"mssql+pyodbc://{db_params.get(DbParam.USER)}:"
        f"{db_params.get(DbParam.PWD)}@{db_params.get(DbParam.HOST)}:"
        f"{db_params.get(DbParam.PORT)}/{db_params.get(DbParam.NAME)}?driver={db_params.get(DbParam.DRIVER)}"
    )


def get_version() -> str | None:
    """
    Obtain and return the current version of the database engine.

    :return: the engine's current version, or *None* if error
    """
    reply: list[tuple] = select(errors=None,
                                sel_stmt="SELECT @@VERSION",
                                where_vals=None,
                                min_count=None,
                                max_count=None,
                                require_count=None,
                                offset_count=None,
                                limit_count=1,
                                conn=None,
                                committable=None,
                                logger=None)

    return reply[0][0] if reply else None


def connect(errors: list[str],
            autocommit: bool | None,
            logger: Logger | None) -> Connection | None:
    """
    Obtain and return a connection to the database.

    Return *None* if the connection could not be obtained.

    :param errors: incidental error messages
    :param autocommit: whether the connection is to be in autocommit mode (defaults to *False*)
    :param logger: optional logger
    :return: the connection to the database, or *None* if error
    """
    # initialize the return valiable
    result: Connection | None = None

    # retrieve the connection parameters and build the connection string
    db_params: dict[DbParam, Any] = _get_params(DbEngine.SQLSERVER)
    connection_kwargs: str = (
        f"DRIVER={{{db_params.get(DbParam.DRIVER)}}};"
        f"SERVER={db_params.get(DbParam.HOST)},{db_params.get(DbParam.PORT)};"
        f"DATABASE={db_params.get(DbParam.NAME)};"
        f"UID={db_params.get(DbParam.USER)};PWD={db_params.get(DbParam.PWD)};TrustServerCertificate=yes;"
    )

    # obtain a connection to the database
    err_msg: str | None = None
    try:
        result = pyodbc.connect(connection_kwargs)
        # establish the connection's autocommit mode
        result.autocommit = isinstance(autocommit, bool) and autocommit
    except Exception as e:
        err_msg = _except_msg(exception=e,
                              engine=DbEngine.SQLSERVER)
    # log eventual errors
    if err_msg:
        if isinstance(errors, list):
            errors.append(err_msg)
        if logger:
            logger.error(err_msg)
            logger.error(msg="Error connecting to "
                             f"'{db_params.get(DbParam.NAME)}' at '{db_params.get(DbParam.HOST)}'")
    return result


def bind_arguments(stmt: str,
                   bind_vals: list[Any]) -> str:
    """
    Replace the placeholders in *query_stmt* with the values in *bind_vals*, and return the modified query statement.

    Note that using a statement in a situation where values for types other than *bool*, *str*, *int*, *float*,
    *date*, or *datetime* were replaced, may bring about undesirable consequences, as the standard string
    representations for these other types would be used.

    The third parameter in the *CONVERT()* function is the style code to be used. Refer to SQLServer's
    documentation for details about this function.

    :param stmt: the query statement
    :param bind_vals: the values to replace the placeholders with
    :return: the query statement with the placeholders replaced with their corresponding values
    """
    # initialize the return variable
    result: str = stmt

    # bind the arguments
    for bind_val in bind_vals:
        val: str
        if isinstance(bind_val, bool):
            val = "1" if bind_val else "0"
        elif isinstance(bind_val, int | float):
            val = f"{bind_val}"
        elif isinstance(bind_val, date):
            val = f"CONVERT(DATE, '{bind_val.strftime(format=DATE_FORMAT_INV)}', 23)"
        elif isinstance(bind_val, datetime):
            val = f"CONVERT(DATETIME, '{bind_val.strftime(format=DATETIME_FORMAT_INV)}', 120)"
        else:
            val = f"'{bind_val}'"
        result = result.replace("?", val, 1)

    return result


def select(errors: list[str] | None,
           sel_stmt: str,
           where_vals: tuple | None,
           min_count: int | None,
           max_count: int | None,
           require_count: int | None,
           offset_count: int | None,
           limit_count: int | None,
           conn: Connection | None,
           committable: bool | None,
           logger: Logger | None) -> list[tuple] | None:
    """
    Query the database and return all tuples that satisfy the *sel_stmt* command.

    The command can optionally contain selection criteria, with respective values given in *where_vals*.
    If not positive integers, *min_count*, *max_count*, and *require_count* are ignored.
    If *require_count* is specified, then exactly that number of tuples must be returned by the query.
    If the search is empty, an empty list is returned.

    The parameter *committable* is relevant only if *conn* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param sel_stmt: SELECT command for the search
    :param where_vals: the values to be associated with the selection criteria
    :param min_count: optionally defines the minimum number of tuples expected
    :param max_count: optionally defines the maximum number of tuples expected
    :param require_count: number of tuples that must exactly satisfy the query (overrides *min_count* and *max_count*)
    :param offset_count: number of tuples to skip
    :param limit_count: limit to the number of tuples returned, to be specified in the query statement itself
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable:whether to commit operation upon errorless completion
    :param logger: optional logger
    :return: list of tuples containing the search result, *[]* on empty search, or *None* if error
    """
    # initialize the return variable
    result: list[tuple] | None = []

    # make sure to have a connection
    curr_conn: Connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)
    if curr_conn:
        # establish the appropriate query cardinality
        if isinstance(require_count, int) and require_count > 0:
            min_count = require_count
            max_count = require_count

        # establish an offset into the result set
        if isinstance(offset_count, int) and offset_count > 0:
            sel_stmt += f" OFFSET {offset_count}"

        # establish a limit to the number of tuples returned
        if isinstance(limit_count, int) and limit_count > 0:
            sel_stmt = sel_stmt.replace("SELECT ", f"SELECT TOP {limit_count} ", 1)

        err_msg: str | None = None
        try:
            # obtain a cursor and execute the operation
            with curr_conn.cursor() as cursor:
                if where_vals:
                    cursor.execute(sel_stmt, where_vals)
                else:
                    cursor.execute(sel_stmt)
                rows: list[Row] = cursor.fetchall()
                # obtain the number of tuples returned
                count: int = len(rows)

                # has the query quota been satisfied ?
                if _assert_query_quota(errors=errors,
                                       engine=DbEngine.SQLSERVER,
                                       query=sel_stmt,
                                       where_vals=where_vals,
                                       count=count,
                                       min_count=min_count,
                                       max_count=max_count,
                                       require_count=require_count):
                    # yes, retrieve the returned tuples
                    result = [tuple(row) for row in rows]

            # commit the transaction, if appropriate
            if committable or not conn:
                curr_conn.commit()
        except Exception as e:
            if curr_conn:
                with suppress(Exception):
                    curr_conn.rollback()
            err_msg = _except_msg(exception=e,
                                  engine=DbEngine.SQLSERVER)
            result = None
        finally:
            # close the connection, if locally acquired
            if curr_conn and not conn:
                curr_conn.close()

        # log eventual errors
        if err_msg:
            if isinstance(errors, list):
                errors.append(err_msg)
            if logger:
                logger.error(err_msg)
                logger.error(msg=_build_query_msg(query_stmt=sel_stmt,
                                                  engine=DbEngine.SQLSERVER,
                                                  bind_vals=where_vals))
    return result


def execute(errors: list[str] | None,
            exc_stmt: str,
            bind_vals: tuple | None,
            conn: Connection | None,
            committable: bool | None,
            logger: Logger | None) -> int | None:
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
    :param committable:whether to commit operation upon errorless completion
    :param logger: optional logger
    :return: the return value from the command execution, or *None* if error
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
                # SQLServer understands 'None' value as an effective bind value
                if bind_vals:
                    cursor.execute(exc_stmt, bind_vals)
                else:
                    cursor.execute(exc_stmt)
                result = cursor.rowcount

            # commit the transaction, if appropriate
            if committable or not conn:
                curr_conn.commit()
        except Exception as e:
            if curr_conn:
                with suppress(Exception):
                    curr_conn.rollback()
            err_msg = _except_msg(exception=e,
                                  engine=DbEngine.SQLSERVER)
        finally:
            # close the connection, if locally acquired
            if curr_conn and not conn:
                curr_conn.close()

        # log eventual errors
        if err_msg:
            if isinstance(errors, list):
                errors.append(err_msg)
            if logger:
                logger.error(err_msg)
                logger.error(msg=_build_query_msg(query_stmt=exc_stmt,
                                                  engine=DbEngine.SQLSERVER,
                                                  bind_vals=bind_vals))
    return result


def bulk_execute(errors: list[str] | None,
                 exc_stmt: str,
                 exc_vals: list[tuple],
                 conn: Connection | None,
                 committable: bool | None,
                 logger: Logger | None) -> int | None:
    """
    Bulk-update the database with the statement defined in *execute_stmt*, and the values in *execute_vals*.

    The binding is done by position. Thus, the binding clauses in *execute_stmt* must contain as many '?'
    placeholders as there are elements in the tuples found in the list provided in *execute_vals*.
    Note that, in *UPDATE* operations, the placeholders in the *WHERE* clause will follow
    the ones in the *SET* clause.

    The parameter *committable* is relevant only if *conn* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param exc_stmt: the command to update the database with
    :param exc_vals: the list of values for tuple identification, and to update the database with
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable:whether to commit operation upon errorless completion
    :param logger: optional logger
    :return: the number of inserted or updated tuples, or *None* if error
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
                cursor.fast_executemany = True
                cursor.executemany(exc_stmt, exc_vals)
                result = len(exc_vals)

            # commit the transaction, if appropriate
            if committable or not conn:
                curr_conn.commit()
        except Exception as e:
            if curr_conn:
                with suppress(Exception):
                    curr_conn.rollback()
            err_msg = _except_msg(exception=e,
                                  engine=DbEngine.SQLSERVER)
        finally:
            # close the connection, if locally acquired
            if curr_conn and not conn:
                curr_conn.close()

        # log eventual errors
        if err_msg:
            if isinstance(errors, list):
                errors.append(err_msg)
            if logger:
                logger.error(err_msg)
                logger.error(msg=_build_query_msg(query_stmt=exc_stmt,
                                                  engine=DbEngine.SQLSERVER,
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
    Update a large binary objects (LOB) in the given table and column.

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
    :param committable:whether to commit operation upon errorless completion
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
        where_clause: str = " AND ".join([f"{column} = ?" for column in pk_columns])
        update_stmt: str = (f"UPDATE {lob_table} "
                            f"SET {lob_column} = ? "
                            f"WHERE {where_clause}")

        err_msg: str | None = None
        try:
            # obtain a cursor and execute the operation
            with curr_conn.cursor() as cursor:

                # retrieve the lob data and write to the database
                if isinstance(lob_data, bytes):
                    cursor.execute(update_stmt,
                                   (Binary(lob_data), *pk_vals))
                elif isinstance(lob_data, Path):
                    data_bytes: bytes
                    with lob_data.open("rb") as file:
                        data_bytes = file.read(chunk_size)
                        while data_bytes:
                            cursor.execute(update_stmt,
                                           (Binary(data_bytes), *pk_vals))
                            data_bytes = file.read(chunk_size)
                else:
                    data_bytes: bytes = lob_data.read(chunk_size)
                    while data_bytes:
                        cursor.execute(update_stmt,
                                       (Binary(data_bytes), *pk_vals))
                        data_bytes = lob_data.read(chunk_size)
                    lob_data.close()

            # commit the transaction, if appropriate
            if committable or not conn:
                curr_conn.commit()
        except Exception as e:
            if curr_conn:
                with suppress(Exception):
                    curr_conn.rollback()
            err_msg = _except_msg(exception=e,
                                  engine=DbEngine.SQLSERVER)
        finally:
            # close the connection, if locally acquired
            if curr_conn and not conn:
                curr_conn.close()

        # log eventual errors
        if err_msg:
            if isinstance(errors, list):
                errors.append(err_msg)
            if logger:
                logger.error(err_msg)
                logger.error(msg=_build_query_msg(query_stmt=update_stmt,
                                                  engine=DbEngine.SQLSERVER,
                                                  bind_vals=pk_vals))


def call_procedure(errors: list[str] | None,
                   proc_name: str,
                   proc_vals: tuple | None,
                   conn: Connection | None,
                   committable: bool | None,
                   logger: Logger | None) -> list[tuple] | None:
    """
    Execute the stored procedure *proc_name* in the database, with the parameters given in *proc_vals*.

    The parameter *committable* is relevant only if *conn* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param proc_name: name of the stored procedure
    :param proc_vals: parameters for the stored procedure
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable:whether to commit operation upon errorless completion
    :param logger: optional logger
    :return: the data returned by the procedure, or *None* if error
    """
    # initialize the return variable
    result: list[tuple] | None = None

    # make sure to have a connection
    curr_conn: Connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)
    if curr_conn:
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

            # commit the transaction, if appropriate
            if committable or not conn:
                curr_conn.commit()
        except Exception as e:
            if curr_conn:
                with suppress(Exception):
                    curr_conn.rollback()
            err_msg = _except_msg(exception=e,
                                  engine=DbEngine.SQLSERVER)
        finally:
            # close the connection, if locally acquired
            if curr_conn and not conn:
                curr_conn.close()

        # log eventual errors
        if err_msg:
            if isinstance(errors, list):
                errors.append(err_msg)
            if logger:
                logger.error(err_msg)
                logger.error(msg=_build_query_msg(query_stmt=proc_stmt,
                                                  engine=DbEngine.SQLSERVER,
                                                  bind_vals=proc_vals))
    return result


def _identity_pre_insert(errors: list[str] | None,
                         insert_stmt: str,
                         conn: Connection,
                         logger: Logger) -> None:
    """
    Handle the pre-insert for tables with identity columns.

    Identity columns are columns whose values are generated directly by the database engine,
    and as such, require special handling before and after bulk inserts.

    :param errors: incidental error messages
    :param insert_stmt: the INSERT command
    :param conn: the connection to use
    :param logger: optional logger
    """
    table_name: str = str_between(source=insert_stmt.upper(),
                                  from_str=" INTO ",
                                  to_str=" ")
    execute(errors=errors,
            exc_stmt=f"SET IDENTITY_INSERT {table_name.lower()} ON",
            bind_vals=None,
            conn=conn,
            committable=False,
            logger=logger)


def _identity_post_insert(errors: list[str] | None,
                          insert_stmt: str,
                          conn: Connection,
                          committable: bool,
                          identity_column: str,
                          logger: Logger) -> None:
    """
    Handle the post-insert for tables with identity columns.

    Identity columns are columns whose values are generated directly by the database engine,
    and as such, require special handling before and after bulk inserts.

    :param errors: incidental error messages
    :param insert_stmt: the INSERT command
    :param conn: the connection to use
    :param committable:whether to commit operation upon errorless completion
    :param identity_column: column whose values are generated by the database
    :param logger: optional logger
    """
    # obtain the maximum value inserted
    table_name: str = str_between(source=insert_stmt.upper(),
                                  from_str=" INTO ",
                                  to_str=" ")
    recs: list[tuple[int]] = select(errors=errors,
                                    sel_stmt=(f"SELECT MAX({identity_column}) "
                                              f"FROM {table_name}"),
                                    where_vals=None,
                                    min_count=None,
                                    max_count=None,
                                    require_count=None,
                                    offset_count=None,
                                    limit_count=None,
                                    conn=conn,
                                    committable=False,
                                    logger=logger)
    if not errors:
        execute(errors=errors,
                exc_stmt=f"SET IDENTITY_INSERT {table_name} OFF",
                bind_vals=None,
                conn=conn,
                committable=False,
                logger=logger)
        if not errors:
            execute(errors=errors,
                    exc_stmt=f"DBCC CHECKIDENT ('{table_name}', RESEED, {recs[0][0]})",
                    bind_vals=None,
                    conn=conn,
                    committable=committable,
                    logger=logger)
