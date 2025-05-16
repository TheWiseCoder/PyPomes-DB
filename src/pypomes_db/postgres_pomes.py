import psycopg2
from contextlib import suppress
from datetime import date, datetime
from logging import Logger
from pathlib import Path
from pypomes_core import DateFormat, DatetimeFormat
from psycopg2 import Binary
from psycopg2.extras import execute_values
# noinspection PyProtectedMember
from psycopg2._psycopg import connection
from pypomes_core import str_between
from typing import Any, BinaryIO, Final

from .db_common import (
    DbEngine, DbParam,
    _assert_query_quota, _build_query_msg, _get_params, _except_msg
)

RESERVED_WORDS: Final[list[str]] = [
    "ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "ASYMMETRIC", "AUTHORIZATION",
    "BINARY", "BOTH", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "CONSTRAINT", "CREATE",
    "CROSS", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME",
    "CURRENT_TIMESTAMP", "CURRENT_USER", "DEFAULT", "DEFERRABLE", "DESC", "DISTINCT",
    "DO", "ELSE", "END", "EXCEPT", "FALSE", "FETCH", "FOR", "FOREIGN", "FREEZE", "FROM", "FULL",
    "GRANT", "GROUP", "HAVING", "ILIKE", "IN", "INITIALLY", "INNER", "INTERSECT", "INTO", "IS", "ISNULL",
    "JOIN", "LATERAL", "LEADING", "LEFT", "LIKE", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP",
    "NATURAL", "NOT", "NOTNULL", "NULL", "OFFSET", "ON", "ONLY", "OR", "ORDER", "OUTER", "OVER", "OVERLAPS",
    "PLACING", "PRIMARY", "REFERENCES", "RETURNING", "RIGHT", "SELECT", "SESSION_USER", "SIMILAR",
    "SOME", "SYMMETRIC", "TABLE", "THEN", "TO", "TRAILING", "TRUE", "UNION", "UNIQUE", "USER", "USING",
    "VARIADIC", "VERBOSE", "WHEN", "WHERE", "WINDOW", "WITH"
]


def get_connection_string() -> str:
    """
    Build and return the connection string for connecting to the database.

    :return: the connection string
    """
    # retrieve the connection parameters
    db_params: dict[DbParam, Any] = _get_params(DbEngine.POSTGRES)

    # build and return the connection string
    return (f"postgresql+psycopg2://{db_params.get(DbParam.USER)}:"
            f"{db_params.get(DbParam.PWD)}@{db_params.get(DbParam.HOST)}:"
            f"{db_params.get(DbParam.PORT)}/{db_params.get(DbParam.NAME)}")


def get_version() -> str | None:
    """
    Obtain and return the current version of the database engine.

    :return: the engine's current version, or *None* if error
    """
    reply: list[tuple] = select(errors=None,
                                sel_stmt="SELECT version()",
                                where_vals=None,
                                min_count=None,
                                max_count=None,
                                offset_count=None,
                                limit_count=None,
                                conn=None,
                                committable=None,
                                logger=None)

    return reply[0][0] if reply else None


def connect(errors: list[str],
            autocommit: bool | None,
            logger: Logger | None) -> connection | None:
    """
    Obtain and return a connection to the database.

    Return *None* if the connection could not be obtained.

    :param errors: incidental error messages
    :param autocommit: whether the connection is to be in autocommit mode (defaults to *False*)
    :param logger: optional logger
    :return: the connection to the database, or *None* if error
    """
    # initialize the return variable
    result: connection | None = None

    # retrieve the connection parameters
    db_params: dict[DbParam, Any] = _get_params(DbEngine.POSTGRES)

    # obtain a connection to the database
    err_msg: str | None = None
    try:
        result = psycopg2.connect(host=db_params.get(DbParam.HOST),
                                  port=db_params.get(DbParam.PORT),
                                  database=db_params.get(DbParam.NAME),
                                  user=db_params.get(DbParam.USER),
                                  password=db_params.get(DbParam.PWD))
        # establish the connection's autocommit mode
        result.autocommit = isinstance(autocommit, bool) and autocommit
    except Exception as e:
        err_msg = _except_msg(exception=e,
                              engine=DbEngine.POSTGRES)
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
            val = "TRUE" if bind_val else "FALSE"
        elif isinstance(bind_val, int | float):
            val = f"{bind_val}"
        elif isinstance(bind_val, date):
            val = f"TO_TIMESTAMP('{bind_val.strftime(format=DateFormat.INV)}', 'YYYY-MM-DD')"
        elif isinstance(bind_val, datetime):
            val = f"TO_TIMESTAMP('{bind_val.strftime(format=DatetimeFormat.INV)}', 'YYYY-MM-DD H24:MI:SS')"
        else:
            val = f"'{bind_val}'"
        result = result.replace("%s", val, 1)

    return result


def select(errors: list[str] | None,
           sel_stmt: str,
           where_vals: tuple | None,
           min_count: int | None,
           max_count: int | None,
           offset_count: int | None,
           limit_count: int | None,
           conn: connection | None,
           committable: bool | None,
           logger: Logger | None) -> list[tuple] | None:
    """
    Query the database and return all tuples that satisfy the *sel_stmt* command.

    The command can optionally contain selection criteria, with respective values given in *where_vals*.
    Care should be exercised if *where_clause* contains *IN* directives. In PostgreSQL, the list of values
    for an attribute with the *IN* directive must be contained in a specific tuple, and the operation will
    break for a list of values containing only 1 element. The safe way to specify *IN* directives is
    to add them to *where_data*, as the specifics for PostgreSQL will then be properly dealt with.

    If not positive integers, *min_count*, *max_count*, *offset_count*, and *limit_count* are ignored.
    If both *min_count* and *max_count* are specified with equal values, then exactly that number of
    tuples must be returned by the query. The parameter *offset_count* is used to offset the retrieval
    of tuples. If the search is empty, an empty list is returned.

    The parameter *committable* is relevant only if *conn* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param sel_stmt: SELECT command for the search
    :param where_vals: the values to be associated with the selection criteria
    :param min_count: optionally defines the minimum number of tuples expected
    :param max_count: optionally defines the maximum number of expected
    :param offset_count: number of tuples to skip
    :param limit_count: limit to the number of tuples returned, to be specified in the query statement itself
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable:whether to commit operation upon errorless completion
    :param logger: optional logger
    :return: list of tuples containing the search result, *[]* on empty search, or *None* if error
    """
    # initialize the return variable
    result: list[tuple] = []

    # make sure to have a connection
    curr_conn: connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)
    if curr_conn:
        # establish an offset into the result set
        if isinstance(offset_count, int) and offset_count > 0:
            sel_stmt += f" OFFSET {offset_count}"

        # establish a limit to the number of tuples returned
        if isinstance(limit_count, int) and limit_count > 0:
            sel_stmt += f" LIMIT {limit_count}"

        err_msg: str | None = None
        try:
            # obtain a cursor and execute the operation
            with curr_conn.cursor() as cursor:
                cursor.execute(query=f"{sel_stmt};",
                               vars=where_vals)
                # obtain the number of tuples returned
                rows: list[tuple] = list(cursor)
                count: int = len(rows)

                # has the query quota been satisfied ?
                if _assert_query_quota(errors=errors,
                                       engine=DbEngine.POSTGRES,
                                       query=sel_stmt,
                                       where_vals=where_vals,
                                       count=count,
                                       min_count=min_count,
                                       max_count=max_count):
                    # yes, retrieve the returned tuples
                    result = rows

            # commit the transaction, if appropriate
            if committable or not conn:
                curr_conn.commit()
        except Exception as e:
            if curr_conn:
                with suppress(Exception):
                    curr_conn.rollback()
            err_msg = _except_msg(exception=e,
                                  engine=DbEngine.POSTGRES)
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
                                                  engine=DbEngine.POSTGRES,
                                                  bind_vals=where_vals))

    return result


def execute(errors: list[str] | None,
            exc_stmt: str,
            bind_vals: tuple | None,
            return_cols: dict[str, type] | None,
            min_count: int | None,
            max_count: int | None,
            conn: connection | None,
            committable: bool | None,
            logger: Logger | None) -> tuple | int | None:
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

    The parameter *committable* is relevant only if *conn* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param exc_stmt: the command to execute
    :param bind_vals: optional bind values
    :param return_cols: optional columns and respective types, whose values are to be returned on *INSERT* or *UPDATE*
    :param min_count: optionally defines the minimum number of tuples to be affected
    :param max_count: optionally defines the maximum number of tuples to be affected
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable:whether to commit operation upon errorless completion
    :param logger: optional logger
    :return: the values of *return_cols*, the value returned by the operation, or *None* if error
    """
    # initialize the return variable
    result: tuple | int | None = None

    # make sure to have a connection
    curr_conn: connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)
    if curr_conn:
        err_msg: str | None = None
        if return_cols:
            exc_stmt += F" RETURNING {', '.join(return_cols.keys())}"
        try:
            # obtain a cursor and execute the operation
            with curr_conn.cursor() as cursor:
                cursor.execute(query=f"{exc_stmt};",
                               vars=bind_vals)

                # has the query quota been satisfied ?
                count: int = cursor.rowcount
                if _assert_query_quota(errors=errors,
                                       engine=DbEngine.ORACLE,
                                       query=exc_stmt,
                                       where_vals=None,
                                       count=count,
                                       min_count=min_count,
                                       max_count=max_count):
                    if return_cols:
                        result = tuple(cursor.fetchone())
                    else:
                        result = cursor.rowcount

            # commit the transaction, if appropriate
            if committable or not conn:
                curr_conn.commit()
        except Exception as e:
            if curr_conn:
                with suppress(Exception):
                    curr_conn.rollback()
            err_msg = _except_msg(exception=e,
                                  engine=DbEngine.POSTGRES)
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
                                                  engine=DbEngine.POSTGRES,
                                                  bind_vals=bind_vals))
    return result


def bulk_execute(errors: list[str],
                 exc_stmt: str,
                 exc_vals: list[tuple],
                 conn: connection,
                 committable: bool,
                 logger: Logger) -> int | None:
    """
    Bulk-update the database with the statement defined in *execute_stmt*, and the values in *execute_vals*.

    For *INSERT* operations, the *VALUES* clause must be simply *VALUES %s*:
        INSERT INTO my_tb (id, v1, v2) VALUES %s

    *UPDATE* operations require a special syntax, with *VALUES %s* combined with a *FROM* clause:
        UPDATE my_tb SET v1 = data.v1, v2 = data.v2 FROM (VALUES %s) AS data (id, v1, v2) WHERE my_tb.id = data.id

    *DELETE* operations require a special syntax using a *IN* clause:
        DELETE FROM my_tb WHERE (id1, id2) IN (%s)

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
    curr_conn: connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)
    if curr_conn:
        # execute the bulk insert
        err_msg: str | None = None
        try:
            # obtain a cursor and perform the operation
            with curr_conn.cursor() as cursor:
                # 'cursor.rowcount' might end up with a wrong value
                execute_values(cur=cursor,
                               sql=exc_stmt,
                               argslist=exc_vals)
                result = len(exc_vals)

            # commit the transaction, if appropriate
            if committable or not conn:
                curr_conn.commit()
        except Exception as e:
            if curr_conn:
                with suppress(Exception):
                    curr_conn.rollback()
            err_msg = _except_msg(exception=e,
                                  engine=DbEngine.POSTGRES)
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
                logger.error(msg=exc_stmt)

    return result


def update_lob(errors: list[str],
               lob_table: str,
               lob_column: str,
               pk_columns: list[str],
               pk_vals: tuple,
               lob_data: bytes | str | Path | BinaryIO,
               chunk_size: int,
               conn: connection | None,
               committable: bool | None,
               logger: Logger | None) -> None:
    """
    Update a large binary object (LOB) in the given table and column.

    The data for the update may come from *bytes*, from a *Path* or its string representation, or from
    a pointer obtained from *BytesIO* or *Path.open()* in binary mode.

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
    curr_conn: connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)
    if curr_conn:
        if isinstance(lob_data, str):
            lob_data = Path(lob_data)

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
                                  engine=DbEngine.POSTGRES)
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
                                                  engine=DbEngine.POSTGRES,
                                                  bind_vals=pk_vals))


def call_procedure(errors: list[str] | None,
                   proc_name: str,
                   proc_vals: tuple | None,
                   conn: connection | None,
                   committable: bool | None,
                   logger: Logger | None) -> list[tuple] | None:
    """
    Execute the stored procedure *proc_name*, with the arguments given in *proc_vals*.

    The parameter *committable* is relevant only if *conn* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors:  incidental error messages
    :param proc_name: the name of the sotred procedure
    :param proc_vals: the arguments to be passed
    :param conn: optional connection to use (obtains a new one, if not provided)
    :param committable:whether to commit operation upon errorless completion
    :param logger: optional logger
    :return: the data returned by the procedure, or *None* if error
    """
    # initialize the return variable
    result: list[tuple] | None = None

    # make sure to have a connection
    curr_conn: connection = conn or connect(errors=errors,
                                            autocommit=False,
                                            logger=logger)
    if curr_conn:
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
                with suppress(Exception):
                    curr_conn.rollback()
            err_msg = _except_msg(exception=e,
                                  engine=DbEngine.POSTGRES)
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
                                                  engine=DbEngine.POSTGRES,
                                                  bind_vals=proc_vals))

    return result


def _identity_post_insert(errors: list[str] | None,
                          insert_stmt: str,
                          conn: connection,
                          committable: bool,
                          identity_column: str,
                          logger: Logger) -> None:
    """
    Handle the post-insert for tables with identity columns.

    Identity columns are columns whose values are generated directly by the database, and as such,
    require special handling before and after bulk inserts.

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
                                    offset_count=None,
                                    limit_count=None,
                                    conn=conn,
                                    committable=False,
                                    logger=logger)
    if not errors:
        sel_stmt: str = (f"SELECT setval("
                         f"pg_get_serial_sequence('{table_name}', '{identity_column}'), "
                         f"{recs[0][0]})")
        execute(errors=errors,
                exc_stmt=sel_stmt,
                bind_vals=None,
                return_cols=None,
                min_count=None,
                max_count=None,
                conn=conn,
                committable=committable,
                logger=logger)
