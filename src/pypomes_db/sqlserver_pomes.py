import pyodbc
from contextlib import suppress
from datetime import date, datetime
from pathlib import Path
from pyodbc import Binary, Connection, Row
from pypomes_core import (
    DateFormat, DatetimeFormat,  str_between, str_splice
)
from typing import Any, BinaryIO, Final

from .db_common import (
    _DB_LOGGERS, DbEngine, DbParam,
    _assert_query_quota, _build_query_msg, _get_params, _except_msg
)

RESERVED_WORDS: Final[list[str]] = [
    "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUTHORIZATION", "BACKUP", "BEGIN", "BETWEEN",
    "BREAK", "BROWSE", "BULK", "BY", "CASE", "CHECK", "CHECKPOINT", "CLOSE", "CLUSTERED", "COALESCE",
    "COLUMN", "COMMIT", "COMPUTE", "CONSTRAINT", "CONTAINS", "CONTAINSTABLE", "CONTINUE", "CONVERT",
    "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
    "CURSOR", "DATABASE", "DBCC", "DEALLOCATE", "DECLARE", "DEFAULT", "DELETE", "DENY", "DESC", "DISK",
    "DISTINCT", "DISTRIBUTED", "DOUBLE", "DROP", "DUMP", "ELSE", "END", "ERRLVL", "ESCAPE", "EXCEPT",
    "EXEC", "EXECUTE", "EXISTS", "EXIT", "EXTERNAL", "FETCH", "FILE", "FILLFACTOR", "FOR", "FOREIGN",
    "FREETEXT", "FREETEXTTABLE", "FROM", "FULL", "FUNCTION", "GOTO", "GRANT", "GROUP", "HAVING", "HOLDLOCK",
    "IDENTITY", "IDENTITY_INSERT", "IDENTITYCOL", "IF", "IN", "INDEX", "INNER", "INSERT", "INTERSECT",
    "INTO", "IS", "JOIN", "KEY", "KILL", "LEFT", "LIKE", "LINENO", "LOAD", "MERGE", "NATIONAL", "NOCHECK",
    "NONCLUSTERED", "NOT", "NULL", "NULLIF", "OF", "OFF", "OFFSETS", "ON", "OPEN", "OPENDATASOURCE",
    "OPENQUERY", "OPENROWSET", "OPENXML", "OPTION", "OR", "ORDER", "OUTER", "OVER", "PERCENT", "PIVOT",
    "PLAN", "PRECISION", "PRIMARY", "PRINT", "PROC", "PROCEDURE", "PUBLIC", "RAISERROR", "READ",
    "READTEXT", "RECONFIGURE", "REFERENCES", "REPLICATION", "RESTORE", "RESTRICT", "RETURN", "REVERT",
    "REVOKE", "RIGHT", "ROLLBACK", "ROWCOUNT", "ROWGUIDCOL", "RULE", "SAVE", "SCHEMA", "SECURITYAUDIT",
    "SELECT", "SEMANTICKEYPHRASETABLE", "SEMANTICSIMILARITYDETAILSTABLE", "SEMANTICSIMILARITYTABLE",
    "SESSION_USER", "SET", "SETUSER", "SHUTDOWN", "SOME", "STATISTICS", "SYSTEM_USER", "TABLE",
    "TABLESAMPLE", "TEXTSIZE", "THEN", "TO", "TOP", "TRAN", "TRANSACTION", "TRIGGER", "TRUNCATE",
    "TRY_CONVERT", "TSEQUAL", "UNION", "UNIQUE", "UNPIVOT", "UPDATE", "UPDATETEXT", "USE", "USER",
    "VALUES", "VARYING", "VIEW", "WAITFOR", "WHEN", "WHERE", "WHILE", "WITH", "WITHIN GROUP", "WRITETEXT"
]


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


def connect(autocommit: bool = None,
            errors: list[str] = None) -> Connection | None:
    """
    Obtain and return a connection to the database.

    Return *None* if the connection could not be obtained.

    :param autocommit: whether the connection is to be in autocommit mode (defaults to *False*)
    :param errors: incidental error messages (must be *[]* or *None*)
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
    try:
        result = pyodbc.connect(connection_kwargs)
        # establish the connection's autocommit mode
        result.autocommit = isinstance(autocommit, bool) and autocommit
    except Exception as e:
        msg: str = _except_msg(exception=e,
                               connection=None,
                               engine=DbEngine.SQLSERVER)
        if _DB_LOGGERS[DbEngine.SQLSERVER]:
            _DB_LOGGERS[DbEngine.SQLSERVER].error(msg=msg)
        if isinstance(errors, list):
            errors.append(msg)

    # log errors
    if _DB_LOGGERS[DbEngine.SQLSERVER] and errors:
        _DB_LOGGERS[DbEngine.SQLSERVER].error(msg=f"Error connecting to '{db_params.get(DbParam.NAME)}' "
                                                  f"at '{db_params.get(DbParam.HOST)}'")
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
            val = f"CONVERT(DATE, '{bind_val.strftime(format=DateFormat.INV)}', 23)"
        elif isinstance(bind_val, datetime):
            val = f"CONVERT(DATETIME, '{bind_val.strftime(format=DatetimeFormat.INV)}', 120)"
        else:
            val = f"'{bind_val}'"
        result = result.replace("?", val, 1)

    return result


def select(sel_stmt: str,
           where_vals: tuple | None,
           min_count: int | None,
           max_count: int | None,
           offset_count: int | None,
           limit_count: int | None,
           connection: Connection | None,
           committable: bool | None,
           errors: list[str] = None) -> list[tuple] | None:
    """
    Query the database and return all tuples that satisfy the *sel_stmt* command.

    The command can optionally contain selection criteria, with respective values given in *where_vals*.
    If not positive integers, *min_count*, *max_count*, *offset_count*, and *limit_count* are ignored.
    If both *min_count* and *max_count* are specified with equal values, then exactly that number of
    tuples must be returned by the query. The parameter *offset_count* is used to offset the retrieval
    of tuples. For both *offset_count* and *limit_count* to be used together, an *ORDER BY* clause must
    have been specifed, otherwise a runtime error is raised. If the search is empty, an empty list is returned.

    The parameter *committable* defines whether a commit or rollback is performed on the provided *connection*.

    :param sel_stmt: SELECT command for the search
    :param where_vals: the values to be associated with the selection criteria
    :param min_count: optionally defines the minimum number of tuples expected
    :param max_count: optionally defines the maximum number of tuples expected
    :param offset_count: number of tuples to skip
    :param limit_count: limit to the number of tuples returned, to be specified in the query statement itself
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit or rollback the operation, upon completion
    :param errors: incidental error messages (must be *[]* or *None*)
    :return: list of tuples containing the search result, *[]* on empty search, or *None* if error
    """
    # initialize the return variable
    result: list[tuple] | None = None

    # make sure to have an errors list
    if not isinstance(errors, list):
        errors = []

    # make sure to have a connection
    curr_conn: Connection = connection or connect(autocommit=False,
                                                  errors=errors)
    if not errors:
        # establish offset and limit
        if offset_count or limit_count:
            sel_stmt = add_query_limits(sel_stmt=sel_stmt,
                                        offset_count=offset_count,
                                        limit_count=limit_count)
        try:
            # obtain a cursor and execute the operation
            with curr_conn.cursor() as cursor:
                if where_vals:
                    cursor.execute(sel_stmt,
                                   where_vals)
                else:
                    cursor.execute(sel_stmt)
                rows: list[Row] = cursor.fetchall()
                # obtain the number of tuples returned
                count: int = len(rows)

                # log the retrieval operation
                if _DB_LOGGERS[DbEngine.SQLSERVER]:
                    from_table: str = str_splice(sel_stmt + " ",
                                                 seps=(" FROM ", " "))[1]
                    _DB_LOGGERS[DbEngine.SQLSERVER].debug(msg=f"Read {count} tuples from "
                                                              f"{DbEngine.SQLSERVER}.{from_table}, offset "
                                                              f"{offset_count}, connection {id(curr_conn)}")

                if _assert_query_quota(query=sel_stmt,
                                       engine=DbEngine.SQLSERVER,
                                       where_vals=where_vals,
                                       count=count,
                                       min_count=min_count,
                                       max_count=max_count,
                                       errors=errors):
                    # the query quota been satisfied, retrieve the returned tuples
                    result = [tuple(row) for row in rows]

            # commit the transaction
            if committable or not connection:
                curr_conn.commit()
        except Exception as e:
            if committable or not connection:
                with suppress(Exception):
                    curr_conn.rollback()
            msg: str = _except_msg(exception=e,
                                   connection=curr_conn,
                                   engine=DbEngine.SQLSERVER)
            if _DB_LOGGERS[DbEngine.SQLSERVER]:
                _DB_LOGGERS[DbEngine.SQLSERVER].error(msg=msg)
            if isinstance(errors, list):
                errors.append(msg)
        finally:
            # close the connection, if locally acquired
            if curr_conn and not connection:
                curr_conn.close()
        # log errors
        if _DB_LOGGERS[DbEngine.SQLSERVER] and errors:
            _DB_LOGGERS[DbEngine.SQLSERVER].error(msg=_build_query_msg(query_stmt=sel_stmt,
                                                  engine=DbEngine.SQLSERVER,
                                                  bind_vals=where_vals))
    return result


def execute(exc_stmt: str,
            bind_vals: tuple | None,
            return_cols: dict[str, type] | None,
            min_count: int | None,
            max_count: int | None,
            connection: Connection | None,
            committable: bool | None,
            errors: list[str] = None) -> tuple | int | None:
    """
    Execute the command *exc_stmt* on the database.

    This command might be a DML ccommand modifying the database, such as inserting, updating or
    deleting tuples, or it might be a DDL statement, or it might even be an environment-related command.

    The optional bind values for this operation are in *bind_vals*. The optional *return_cols* indicate that
    the values of the columns therein should be returned upon execution of *exc_stmt*. This is typical for
    *INSERT* or *UPDATE* statements on tables with *identity-type* columns, which are columns whose values
    are generated by the database itself. Otherwise, the value returned is the number of inserted, modified,
    or deleted tuples, or *None* if an error occurred.

    If not positive integers, *min_count* and *max_count* are ignored. If both *min_count* and *max_count*
    are specified with equal values, then exactly that number of tuples must have been affected by the query,
    as reported by *cursor.rowcount*.

    The parameter *committable* defines whether a commit or rollback is performed on the provided *connection*.

    :param exc_stmt: the command to execute
    :param bind_vals: optional bind values
    :param return_cols: optional columns and respective types, whose values are to be returned on *INSERT* or *UPDATE*
    :param min_count: optionally defines the minimum number of tuples to be affected
    :param max_count: optionally defines the maximum number of tuples to be affected
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit or rollback the operation, upon completion
    :param errors: incidental error messages (must be *[]* or *None*)
    :return: the values of *return_cols*, the value returned by the operation, or *None* if error
    """
    # initialize the return variable
    result: tuple | int | None = None

    # make sure to have an errors list
    if not isinstance(errors, list):
        errors = []

    # make sure to have a connection
    curr_conn: Connection = connection or connect(autocommit=False,
                                                  errors=errors)
    if not errors:
        # handle return columns
        if return_cols:
            # 'DELETED' and 'INSERTED' refer to the old and new values, respectively
            cols: list[str] = [f"INSERTED.{col}" for col in return_cols]
            pos: int = exc_stmt.find(" VALUES(")
            if pos < 0:
                pos = exc_stmt.find(" WHERE ")
            if pos > 0:
                exc_stmt = exc_stmt[:pos] + f" OUTPUT {', '.join(cols)}" + exc_stmt[pos:]
            else:
                exc_stmt += f" OUTPUT {', '.join(cols)}"
        try:
            # obtain a cursor and execute the operation
            with curr_conn.cursor() as cursor:
                # SQLServer understands 'None' value as an effective bind value
                if bind_vals:
                    cursor.execute(exc_stmt,
                                   bind_vals)
                else:
                    cursor.execute(exc_stmt)

                # check whether the query quota has been satisfied
                count: int = cursor.rowcount
                if _assert_query_quota(query=exc_stmt,
                                       engine=DbEngine.SQLSERVER,
                                       where_vals=None,
                                       count=count,
                                       min_count=min_count,
                                       max_count=max_count,
                                       errors=errors):
                    if return_cols:
                        result = tuple(cursor.fetchone())
                    else:
                        result = count

            # commit the transaction
            if committable or not connection:
                curr_conn.commit()
        except Exception as e:
            if committable or not connection:
                with suppress(Exception):
                    curr_conn.rollback()
            msg: str = _except_msg(exception=e,
                                   connection=curr_conn,
                                   engine=DbEngine.SQLSERVER)
            if _DB_LOGGERS[DbEngine.SQLSERVER]:
                _DB_LOGGERS[DbEngine.SQLSERVER].error(msg=msg)
            if isinstance(errors, list):
                errors.append(msg)
        finally:
            # close the connection, if locally acquired
            if curr_conn and not connection:
                curr_conn.close()
        # log errors
        if _DB_LOGGERS[DbEngine.SQLSERVER] and errors:
            _DB_LOGGERS[DbEngine.SQLSERVER].error(msg=_build_query_msg(query_stmt=exc_stmt,
                                                                       engine=DbEngine.SQLSERVER,
                                                                       bind_vals=bind_vals))
    return result


def bulk_execute(exc_stmt: str,
                 exc_vals: list[tuple],
                 connection: Connection | None,
                 committable: bool | None,
                 errors: list[str] = None) -> int | None:
    """
    Bulk-update the database with the statement defined in *execute_stmt*, and the values in *execute_vals*.

    The binding is done by position. Thus, the binding clauses in *execute_stmt* must contain as many '?'
    placeholders as there are elements in the tuples found in the list provided in *execute_vals*.
    Note that, in *UPDATE* operations, the placeholders in the *WHERE* clause will follow
    the ones in the *SET* clause.

    The parameter *committable* defines whether a commit or rollback is performed on the provided *connection*.

    :param exc_stmt: the command to update the database with
    :param exc_vals: the list of values for tuple identification, and to update the database with
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit or rollback the operation, upon completion
    :param errors: incidental error messages (must be *[]* or *None*)
    :return: the number of inserted or updated tuples, or *None* if error
    """
    # initialize the return variable
    result: int | None = None

    # make sure to have an errors list
    if not isinstance(errors, list):
        errors = []

    # make sure to have a connection
    curr_conn: Connection = connection or connect(autocommit=False,
                                                  errors=errors)
    if not errors:
        try:
            # obtain a cursor and perform the operation
            with curr_conn.cursor() as cursor:
                cursor.fast_executemany = True
                cursor.executemany(exc_stmt, exc_vals)
                result = len(exc_vals)

            # commit the transaction
            if committable or not connection:
                curr_conn.commit()
        except Exception as e:
            if committable or not connection:
                with suppress(Exception):
                    curr_conn.rollback()
            msg: str = _except_msg(exception=e,
                                   connection=curr_conn,
                                   engine=DbEngine.SQLSERVER)
            if _DB_LOGGERS[DbEngine.SQLSERVER]:
                _DB_LOGGERS[DbEngine.SQLSERVER].error(msg=msg)
            if isinstance(errors, list):
                errors.append(msg)
        finally:
            # close the connection, if locally acquired
            if curr_conn and not connection:
                curr_conn.close()
        if _DB_LOGGERS[DbEngine.SQLSERVER] and errors:
            _DB_LOGGERS[DbEngine.SQLSERVER].error(msg=_build_query_msg(query_stmt=exc_stmt,
                                                                       engine=DbEngine.SQLSERVER,
                                                                       bind_vals=exc_vals[0]))
    return result


def update_lob(lob_table: str,
               lob_column: str,
               pk_columns: list[str],
               pk_vals: tuple,
               lob_data: bytes | str | Path | BinaryIO,
               chunk_size: int,
               connection: Connection | None,
               committable: bool | None,
               errors: list[str] = None) -> None:
    """
    Update a large binary objects (LOB) in the given table and column.

    The data for the update may come from *bytes*, from a *Path* or its string representation,
    or from a pointer obtained from *BytesIO* or *Path.open()* in binary mode.

    The parameter *committable* defines whether a commit or rollback is performed on the provided *connection*.

    :param lob_table: the table to be update with the new LOB
    :param lob_column: the column to be updated with the new LOB
    :param pk_columns: columns making up a primary key, or a unique identifier for the tuple
    :param pk_vals: values with which to locate the tuple to be updated
    :param lob_data: the LOB data (bytes, a file path, or a file pointer)
    :param chunk_size: size in bytes of the data chunk to read/write, or 0 or *None* for no limit
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit or rollback the operation, upon completion
    :param errors: incidental error messages (must be *[]* or *None*)
    """
    # make sure to have an errors list
    if not isinstance(errors, list):
        errors = []

    # make sure to have a connection
    curr_conn: Connection = connection or connect(autocommit=False,
                                                  errors=errors)
    if not errors:
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

            # commit the transaction
            if committable or not connection:
                curr_conn.commit()
        except Exception as e:
            if committable or not connection:
                with suppress(Exception):
                    curr_conn.rollback()
            msg: str = _except_msg(exception=e,
                                   connection=curr_conn,
                                   engine=DbEngine.SQLSERVER)
            if _DB_LOGGERS[DbEngine.SQLSERVER]:
                _DB_LOGGERS[DbEngine.SQLSERVER].error(msg=msg)
            if isinstance(errors, list):
                errors.append(msg)
        finally:
            # close the connection, if locally acquired
            if curr_conn and not connection:
                curr_conn.close()
        # log errors
        if _DB_LOGGERS[DbEngine.SQLSERVER] and errors:
            _DB_LOGGERS[DbEngine.SQLSERVER].error(msg=_build_query_msg(query_stmt=update_stmt,
                                                                       engine=DbEngine.SQLSERVER,
                                                                       bind_vals=pk_vals))


def add_query_limits(sel_stmt: str,
                     offset_count: int | None,
                     limit_count: int | None) -> str:
    """
    Add offset and limit to *sel_stmt*.

    If not positive integers, *offset_count*, and *limit_count* are ignored. The parameter *offset_count*
    is used to offset the retrieval of tuples, whereas *limit_count* establishes a ceiling on the number
    of tuples returned.

    :param sel_stmt: the query statement to modify
    :param offset_count: number of tuples to skip
    :param limit_count: limit to the number of tuples returned
    :return: the modified query statement
    """
    # initialize the return variable
    result: str = sel_stmt

    # establish offset and limit for query (TOP and OFFSET clauses cannot be used together)
    if isinstance(offset_count, int) and offset_count > 0:
        # establish an offset into the result set
        result += f" OFFSET {offset_count} ROWS"
        if isinstance(limit_count, int) and limit_count > 0:
            # establish a limit to the number of tuples returned
            result += f" FETCH NEXT {limit_count} ROWS ONLY"
    elif isinstance(limit_count, int) and limit_count > 0:
        # establish a limit to the number of tuples returned
        if "SELECT DISTINCT " in result:
            # 1. to get the top N distinct rows:
            #    'DISTINCT' is applied before the 'TOP' clause -
            #    the top N rows are selected, and then duplicates are removed
            # 2. to get the distinct rows first, and then pick the top N from that set:
            #    a subquery has to be used -
            #    SELECT TOP (N) *
            #    FROM (
            #        SELECT DISTINCT <coumns>
            #        FROM <table
            #    ) AS distinct_rows
            #    ...
            result = result.replace("SELECT DISTINCT ", f"SELECT DISTINCT TOP {limit_count} ", 1)
        else:
            result = result.replace("SELECT ", f"SELECT TOP {limit_count} ", 1)

    return result


def call_procedure(proc_name: str,
                   proc_vals: tuple | None,
                   connection: Connection | None,
                   committable: bool | None,
                   errors: list[str] = None) -> list[tuple] | None:
    """
    Execute the stored procedure *proc_name* in the database, with the parameters given in *proc_vals*.

    The parameter *committable* defines whether a commit or rollback is performed on the provided *connection*.

    :param proc_name: name of the stored procedure
    :param proc_vals: parameters for the stored procedure
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit or rollback the operation, upon completion
    :param errors: incidental error messages (must be *[]* or *None*)
    :return: the data returned by the procedure, or *None* if error
    """
    # initialize the return variable
    result: list[tuple] | None = None

    # make sure to have an errors list
    if not isinstance(errors, list):
        errors = []

    # make sure to have a connection
    curr_conn: Connection = connection or connect(autocommit=False,
                                                  errors=errors)
    if not errors:
        # build the command
        proc_stmt: str | None = None

        # execute the stored procedure
        try:
            # obtain a cursor and execute the operation
            with curr_conn.cursor() as cursor:
                proc_stmt = f"SET NOCOUNT ON; EXEC {proc_name} {','.join(('?',) * len(proc_vals))}"
                cursor.execute(proc_stmt,
                               proc_vals)
                # retrieve the returned tuples
                rows: list[Row] = cursor.fetchall()
                result = [tuple(row) for row in rows]

            # commit the transaction
            if committable or not connection:
                curr_conn.commit()
        except Exception as e:
            if committable or not connection:
                with suppress(Exception):
                    curr_conn.rollback()
            msg: str = _except_msg(exception=e,
                                   connection=curr_conn,
                                   engine=DbEngine.SQLSERVER)
            if _DB_LOGGERS[DbEngine.SQLSERVER]:
                _DB_LOGGERS[DbEngine.SQLSERVER].error(msg=msg)
            if isinstance(errors, list):
                errors.append(msg)
        finally:
            # close the connection, if locally acquired
            if curr_conn and not connection:
                curr_conn.close()
        # log errors
        if _DB_LOGGERS[DbEngine.SQLSERVER] and errors:
            _DB_LOGGERS[DbEngine.SQLSERVER].error(msg=_build_query_msg(query_stmt=proc_stmt,
                                                                       engine=DbEngine.SQLSERVER,
                                                                       bind_vals=proc_vals))
    return result


def identity_pre_insert(insert_stmt: str,
                        connection: Connection,
                        errors: list[str] = None) -> None:
    """
    Handle the pre-insert for tables with identity columns.

    Identity columns are columns whose values are generated directly by the database engine,
    and as such, require special handling before and after bulk inserts.

    :param insert_stmt: the INSERT command
    :param connection: the connection to use
    :param errors: incidental error messages (must be *[]* or *None*)
    """
    table_name: str = str_between(insert_stmt.upper(),
                                  from_str=" INTO ",
                                  to_str=" ")
    execute(exc_stmt=f"SET IDENTITY_INSERT {table_name.lower()} ON",
            bind_vals=None,
            return_cols=None,
            min_count=None,
            max_count=None,
            connection=connection,
            committable=False,
            errors=errors)


def identity_post_insert(insert_stmt: str,
                         connection: Connection,
                         committable: bool,
                         identity_column: str,
                         errors: list[str] = None) -> None:
    """
    Handle the post-insert for tables with identity columns.

    Identity columns are columns whose values are generated directly by the database engine,
    and as such, require special handling before and after bulk inserts.

    :param insert_stmt: the INSERT command
    :param connection: the connection to use
    :param committable: whether to commit or rollback the operation, upon completion
    :param identity_column: column whose values are generated by the database
    :param errors: incidental error messages (must be *[]* or *None*)
    """
    # make sure to have an errors list
    if not isinstance(errors, list):
        errors = []

    # obtain the maximum value inserted
    table_name: str = str_between(insert_stmt.upper(),
                                  from_str=" INTO ",
                                  to_str=" ")
    recs: list[tuple[int]] = select(sel_stmt=(f"SELECT MAX({identity_column}) "
                                              f"FROM {table_name}"),
                                    where_vals=None,
                                    min_count=None,
                                    max_count=None,
                                    offset_count=None,
                                    limit_count=None,
                                    connection=connection,
                                    committable=False,
                                    errors=errors)
    if not errors:
        execute(exc_stmt=f"SET IDENTITY_INSERT {table_name} OFF",
                bind_vals=None,
                return_cols=None,
                min_count=None,
                max_count=None,
                connection=connection,
                committable=False,
                errors=errors)
        if not errors:
            execute(exc_stmt=f"DBCC CHECKIDENT ('{table_name}', RESEED, {recs[0][0]})",
                    bind_vals=None,
                    return_cols=None,
                    min_count=None,
                    max_count=None,
                    connection=connection,
                    committable=committable,
                    errors=errors)
