import oracledb
import re
from contextlib import suppress
from datetime import date, datetime
from oracledb import Connection, init_oracle_client, makedsn
from pathlib import Path
from pypomes_core import (
    DateFormat, DatetimeFormat, str_splice
)
from typing import Any, BinaryIO, Final

from .db_common import (
    _DB_LOGGERS, DbEngine, DbParam,
    _assert_query_quota, _build_query_msg,
    _get_param, _get_params, _except_msg
)

RESERVED_WORDS: Final[list[str]] = [
    "ACCESS", "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUDIT", "BETWEEN", "BY",
    "CHAR", "CHECK", "CLUSTER", "COLUMN", "COMMENT", "COMPRESS", "CONNECT", "CREATE", "CURRENT",
    "DATE", "DECIMAL", "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP", "ELSE", "EXCLUSIVE", "EXISTS",
    "FILE", "FLOAT", "FOR", "FROM", "GRANT", "GROUP", "HAVING", "IDENTIFIED", "IMMEDIATE", "IN", "INCREMENT",
    "INDEX", "INITIAL", "INSERT", "INTEGER", "INTERSECT", "INTO", "IS", "LEVEL", "LIKE", "LOCK", "LONG",
    "MAXEXTENTS", "MINUS", "MODE", "MODIFY", "NOAUDIT", "NOCOMPRESS", "NOT", "NOWAIT", "NULL", "NUMBER",
    "OF", "OFFLINE", "ON", "ONLINE", "OPTION", "OR", "ORDER", "PCTFREE", "PRIOR", "PUBLIC", "RAW", "RENAME",
    "RESOURCE", "REVOKE", "ROW", "ROWID", "ROWNUM", "ROWS", "SELECT", "SESSION", "SET", "SHARE", "SIZE",
    "SMALLINT", "START", "SUCCESSFUL", "SYNONYM", "SYSDATE", "TABLE", "THEN", "TO", "TRIGGER", "UID", "UNION",
    "UNIQUE", "UPDATE", "USER", "VALIDATE", "VALUES", "VARCHAR", "VARCHAR2", "VIEW", "WHENEVER", "WHERE", "WITH"
]


def get_connection_string() -> str:
    """
    Build and return the connection string for connecting to the database.

    :return: the connection string
    """
    # retrieve the connection parameters
    db_params: dict[DbParam, Any] = _get_params(DbEngine.ORACLE)

    # build and return the connection string
    dsn: str = makedsn(host=db_params.get(DbParam.HOST),
                       port=db_params.get(DbParam.PORT),
                       service_name=db_params.get(DbParam.NAME))
    return f"oracle+oracledb://{db_params.get(DbParam.USER)}:{db_params.get(DbParam.PWD)}@{dsn}"


def connect(autocommit: bool = None,
            errors: list[str] = None) -> Connection | None:
    """
    Obtain and return a connection to the database.

    Return *None* if the connection could not be obtained.

    :param autocommit: whether the connection is to be in autocommit mode (defaults to *False*)
    :param errors: incidental error messages (must be *[]* or *None*)
    :return: the connection to the database, or *None* if error
    """
    # initialize the return variable
    result: Connection | None = None

    # retrieve the connection parameters
    db_params: dict[DbParam, Any] = _get_params(DbEngine.ORACLE)

    # obtain a connection to the database
    try:
        result = oracledb.connect(service_name=db_params.get(DbParam.NAME),
                                  host=db_params.get(DbParam.HOST),
                                  port=db_params.get(DbParam.PORT),
                                  user=db_params.get(DbParam.USER),
                                  password=db_params.get(DbParam.PWD))
        # establish the connection's autocommit mode
        result.autocommit = isinstance(autocommit, bool) and autocommit
    except Exception as e:
        msg = _except_msg(exception=e,
                          connection=None,
                          engine=DbEngine.ORACLE)
        if _DB_LOGGERS[DbEngine.ORACLE]:
            _DB_LOGGERS[DbEngine.ORACLE].error(msg=msg)
        if isinstance(errors, list):
            errors.append(msg)

    # log errors
    if _DB_LOGGERS[DbEngine.ORACLE] and errors:
        _DB_LOGGERS[DbEngine.ORACLE].error(msg=f"Error connecting to '{db_params.get(DbParam.NAME)} '"
                                               f"at '{db_params.get(DbParam.NAME)}'")
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
            val = "1" if bind_val else "0"
        elif isinstance(bind_val, int | float):
            val = f"{bind_val}"
        elif isinstance(bind_val, date):
            val = f"TO_DATE('{bind_val.strftime(format=DateFormat.INV)}', 'YYYY-MM-DD')"
        elif isinstance(bind_val, datetime):
            val = f"TO_DATE('{bind_val.strftime(format=DatetimeFormat.INV)}', 'YYYY-MM-DD H24:MI:SS')"
        else:
            val = f"'{bind_val}'"

        # make sure there is a bind mark
        if re.search(pattern=r":[1-9]\d*",
                     string=result):
            # bind to the lowest available index
            inx: int = 1
            while result.find(f":{inx}") < 0:
                inx += 1
            result = result.replace(f":{inx}", val, 1)
        else:
            # no more bind marks
            break

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
    of tuples. If the search is empty, an empty list is returned.

    The parameter *committable* defines whether a commit or rollback is performed on the provided *connection*.

    :param sel_stmt: SELECT command for the search
    :param where_vals: the values to be associated with the selection criteria
    :param min_count: optionally defines the minimum number of tuples expected
    :param max_count: optionally defines the maximum number of tuples expected
    :param offset_count: number of tuples to skip (ignored if *sel_stmt* does not contain an *ORDER BY* clause)
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
            # obtain a cursor and perform the operation
            with curr_conn.cursor() as cursor:
                # execute the query
                cursor.execute(statement=sel_stmt,
                               parameters=where_vals)
                rows: list[tuple] = cursor.fetchall()
                # obtain the number of tuples returned
                count: int = len(rows)

                # log the retrieval operation
                if _DB_LOGGERS[DbEngine.ORACLE]:
                    from_table: str = str_splice(sel_stmt + " ",
                                                 seps=(" FROM ", " "))[1]
                    _DB_LOGGERS[DbEngine.ORACLE].debug(msg=f"Read {count} tuples from {DbEngine.ORACLE}.{from_table}, "
                                                           f"offset {offset_count}, connection {id(curr_conn)}")

                # has the query quota been satisfied ?
                if _assert_query_quota(query=sel_stmt,
                                       engine=DbEngine.ORACLE,
                                       where_vals=where_vals,
                                       count=count,
                                       min_count=min_count,
                                       max_count=max_count,
                                       errors=errors):
                    # yes, retrieve the returned tuples
                    if count == 1 and sel_stmt.upper().startswith("SELECT DBMS_METADATA.GET_DDL"):
                        # in this instance, a CLOB may be returned
                        result = [(str(rows[0][0]),)]
                    else:
                        result = rows
            # commit the transaction, if appropriate
            if committable or not connection:
                curr_conn.commit()
        except Exception as e:
            if committable or not connection:
                with suppress(Exception):
                    curr_conn.rollback()
            msg: str = _except_msg(exception=e,
                                   connection=curr_conn,
                                   engine=DbEngine.ORACLE)
            if _DB_LOGGERS[DbEngine.ORACLE]:
                _DB_LOGGERS[DbEngine.ORACLE].error(msg=msg)
            if isinstance(errors, list):
                errors.append(msg)
        finally:
            # close the connection, if locally acquired
            if curr_conn and not connection:
                curr_conn.close()
        # log errors
        if _DB_LOGGERS[DbEngine.ORACLE] and errors:
            _DB_LOGGERS[DbEngine.ORACLE].error(msg=_build_query_msg(query_stmt=sel_stmt,
                                                                    engine=DbEngine.ORACLE,
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
            inx: int = __last_placeholder(stmt=exc_stmt) + 1
            binds: list[str] = [f":{i!s}" for i in range(inx, inx+len(return_cols))]
            exc_stmt += f" RETURNING {', '.join(return_cols.keys())} INTO {', '.join(binds)}"
        try:
            # obtain a cursor and execute the operation
            with curr_conn.cursor() as cursor:
                bind_vars: tuple = ()
                if return_cols:
                    bind_vars = tuple([cursor.var(v) for v in return_cols.values()])
                    bind_vals += bind_vars
                cursor.execute(statement=exc_stmt,
                               parameters=bind_vals)

                # has the query quota been satisfied ?
                count: int = cursor.rowcount
                if _assert_query_quota(query=exc_stmt,
                                       engine=DbEngine.ORACLE,
                                       where_vals=None,
                                       count=count,
                                       min_count=min_count,
                                       max_count=max_count,
                                       errors=errors):
                    if bind_vars:
                        result = tuple([var.getvalue() for var in bind_vars])
                    else:
                        result = count

            # commit the transaction, if appropriate
            if committable or not connection:
                curr_conn.commit()
        except Exception as e:
            if committable or not connection:
                with suppress(Exception):
                    curr_conn.rollback()
            msg: str = _except_msg(exception=e,
                                   connection=curr_conn,
                                   engine=DbEngine.ORACLE)
            if _DB_LOGGERS[DbEngine.ORACLE]:
                _DB_LOGGERS[DbEngine.ORACLE].error(msg=msg)
            if isinstance(errors, list):
                errors.append(msg)
        finally:
            # close the connection, if locally acquired
            if curr_conn and not connection:
                curr_conn.close()
        # log errors
        if _DB_LOGGERS[DbEngine.ORACLE] and errors:
            _DB_LOGGERS[DbEngine.ORACLE].debug(msg=_build_query_msg(query_stmt=exc_stmt,
                                                                    engine=DbEngine.ORACLE,
                                                                    bind_vals=bind_vals))
    return result


def bulk_execute(exc_stmt: str,
                 exc_vals: list[tuple],
                 connection: Connection | None,
                 committable: bool | None,
                 errors: list[str] = None) -> int | None:
    """
    Bulk-update the database with the statement defined in *execute_stmt*, and the values in *execute_vals*.

    The binding is done by position. Thus, the binding clauses in *execute_stmt* must contain
    as many ':n' placeholders as there are elements in the tuples found in the list provided in
    *execute_vals*, where 'n' is the 1-based position of the data in the tuple.
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
                cursor.executemany(statement=exc_stmt,
                                   parameters=exc_vals)
                result = len(exc_vals)

            # commit the transaction, if appropriate
            if committable or not connection:
                curr_conn.commit()
        except Exception as e:
            if committable or not connection:
                with suppress(Exception):
                    curr_conn.rollback()
            msg: str = _except_msg(exception=e,
                                   connection=curr_conn,
                                   engine=DbEngine.ORACLE)
            if _DB_LOGGERS[DbEngine.ORACLE]:
                _DB_LOGGERS[DbEngine.ORACLE].error(msg=msg)
            if isinstance(errors, list):
                errors.append(msg)
        finally:
            # close the connection, if locally acquired
            if curr_conn and not connection:
                curr_conn.close()
        # log errors
        if _DB_LOGGERS[DbEngine.ORACLE] and errors:
            _DB_LOGGERS[DbEngine.ORACLE].error(msg=_build_query_msg(query_stmt=exc_stmt,
                                                                    engine=DbEngine.ORACLE,
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
    Update a large binary object (LOB) in the given table and column.

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
        where_clause: str = " AND ".join([f"{column} = :{inx}"
                                          for column, inx in enumerate(iterable=pk_columns,
                                                                       start=2)])
        update_stmt: str = (f"UPDATE {lob_table} "
                            f"SET {lob_column} = :1 "
                            f"WHERE {where_clause}")
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
            if committable or not connection:
                curr_conn.commit()
        except Exception as e:
            if committable or not connection:
                with suppress(Exception):
                    curr_conn.rollback()
            msg: str = _except_msg(exception=e,
                                   connection=curr_conn,
                                   engine=DbEngine.ORACLE)
            if _DB_LOGGERS[DbEngine.ORACLE]:
                _DB_LOGGERS[DbEngine.ORACLE].error(msg=msg)
            if isinstance(errors, list):
                errors.append(msg)
        finally:
            # close the connection, if locally acquired
            if curr_conn and not connection:
                curr_conn.close()
        # log errors
        if _DB_LOGGERS[DbEngine.ORACLE] and errors:
            _DB_LOGGERS[DbEngine.ORACLE].error(msg=_build_query_msg(query_stmt=update_stmt,
                                                                    engine=DbEngine.ORACLE,
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

    # establish an offset into the result set
    if isinstance(offset_count, int) and offset_count > 0:
        result += f" OFFSET {offset_count} ROWS"

    # establish a limit to the number of tuples returned
    if isinstance(limit_count, int) and limit_count > 0:
        if isinstance(offset_count, int) and offset_count > 0:
            result += " FETCH NEXT"
        else:
            result += " FETCH FIRST"
        result += f" {limit_count} ROWS ONLY"

    return result


# see https://python-oracledb.readthedocs.io/en/latest/user_guide/plsql_execution.html (TODO)
# noinspection PyUnusedLocal
# ruff: noqa: ARG001
def call_function(func_name: str,
                  func_vals: tuple | None,
                  connection: Connection | None,
                  committable: bool | None,
                  errors: list[str] = None) -> list[tuple] | None:
    """
    Execute the stored function *func_name* in the database, with the parameters given in *func_vals*.

    The parameter *committable* defines whether a commit or rollback is performed on the provided *connection*.

    :param func_name: name of the stored function
    :param func_vals: parameters for the stored function
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit or rollback the operation, upon completion
    :param errors: incidental error messages (must be *[]* or *None*)
    :return: the data returned by the function, or *None* if error
    """
    # initialize the return variable
    result: list[tuple] = []

    return result


# see https://python-oracledb.readthedocs.io/en/latest/user_guide/plsql_execution.html (TODO)
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
    result: list[tuple] = []

    # make sure to have an errors list
    if not isinstance(errors, list):
        errors = []

    # make sure to have a connection
    curr_conn: Connection = connection or connect(autocommit=False,
                                                  errors=errors)
    if not errors:
        # execute the stored procedure
        try:
            # obtain a cursor and perform the operation
            with curr_conn.cursor() as cursor:
                cursor.callproc(name=proc_name,
                                parameters=proc_vals)

                # retrieve the returned tuples
                result = list(cursor)

            # commit the transaction, if appropriate
            if committable or not connection:
                curr_conn.commit()
        except Exception as e:
            if committable or not connection:
                with suppress(Exception):
                    curr_conn.rollback()
            msg: str = _except_msg(exception=e,
                                   connection=curr_conn,
                                   engine=DbEngine.ORACLE)
            if _DB_LOGGERS[DbEngine.ORACLE]:
                _DB_LOGGERS[DbEngine.ORACLE].error(msg=msg)
            if isinstance(errors, list):
                errors.append(msg)
        finally:
            # close the connection, if locally acquired
            if curr_conn and not connection:
                curr_conn.close()
        # log errors
        if _DB_LOGGERS[DbEngine.ORACLE] and errors:
            _DB_LOGGERS[DbEngine.ORACLE].error(msg=_build_query_msg(query_stmt=proc_name,
                                                                    engine=DbEngine.ORACLE,
                                                                    bind_vals=proc_vals))
    return result


def initialize(errors: list[str] = None) -> None:
    """
    Prepare the oracle engine to access the database throught the installed client software.

    :param errors: incidental error messages
    """
    client: Path = _get_param(engine=DbEngine.ORACLE,
                              param=DbParam.CLIENT)
    if client:
        try:
            init_oracle_client(client.as_posix())
            if _DB_LOGGERS[DbEngine.ORACLE]:
                _DB_LOGGERS[DbEngine.ORACLE].debug(msg="Oracle client initialized")
        except Exception as e:
            if isinstance(errors, list):
                errors.append(_except_msg(exception=e,
                                          connection=None,
                                          engine=DbEngine.ORACLE))


def __last_placeholder(stmt: str) -> int:
    """
    Retrieve the value of the last placeholer in *stmt*.

    :param stmt: the stament to inspect
    :return: the last placeholder, or *0* if no placeholder exists.
    """
    # retrieve the placeholders
    placeholders: list[str] = re.findall(pattern=r":(\d+)",
                                         string=stmt)
    return max(map(int, placeholders)) if placeholders else 0
