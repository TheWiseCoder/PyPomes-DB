from logging import Logger
from psycopg2 import connect
from psycopg2.extras import execute_values
# noinspection PyProtectedMember
from psycopg2._psycopg import connection
from typing import Any

from .db_common import (
    _assert_query_quota, _db_get_params, _db_log, _db_except_msg
)
from .db_pomes import db_bulk_insert as db_bulk_insert_to


def db_connect(errors: list[str],
               logger: Logger = None) -> connection:
    """
    Obtain and return a connection to the database, or *None* if the connection could not be obtained.

    :param errors: incidental error messages
    :param logger: optional logger
    :return: the connection to the database
    """
    # initialize the return variable
    result: connection | None = None

    # retrieve the connection parameters
    name, user, pwd, host, port = _db_get_params("postgres")

    # obtain a connection to the database
    err_msg: str | None = None
    try:
        result = connect(host=host,
                         port=port,
                         database=name,
                         user=user,
                         password=pwd)
        # make sure the connection is not in autocommit mode
        result.autocommit = False
    except Exception as e:
        err_msg = _db_except_msg(e, "postgres")

    # log the results
    _db_log(errors=errors,
            err_msg=err_msg,
            logger=logger,
            query_stmt=f"Connecting to '{name}' at '{host}'")

    return result


def db_select_all(errors: list[str],
                  sel_stmt: str,
                  where_vals: tuple,
                  require_min: int,
                  require_max: int,
                  conn: connection,
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
    :param conn: optional connection to use (obtains a new one, if not specified)
    :param logger: optional logger
    :return: list of tuples containing the search result, or [] if the search is empty
    """
    # initialize the return variable
    result: list[tuple] = []

    # make sure to have a connection
    curr_conn: connection = conn or db_connect(errors=errors,
                                               logger=logger)

    if isinstance(require_max, int) and require_max >= 0:
        sel_stmt += f" LIMIT {require_max}"

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
                                   query=sel_stmt,
                                   where_vals=where_vals,
                                   count=count,
                                   require_min=require_min,
                                   require_max=require_max):
                # yes, retrieve the returned tuples
                result = list(cursor)

        # commit the transaction
        curr_conn.commit()
    except Exception as e:
        curr_conn.rollback()
        err_msg = _db_except_msg(exception=e,
                                 engine="postgres")
    finally:
        # close the connection, if locally acquired
        if not conn:
            curr_conn.close()

    # log the results
    _db_log(errors=errors,
            err_msg=err_msg,
            logger=logger,
            query_stmt=sel_stmt,
            bind_vals=where_vals)

    return result


def db_bulk_insert(errors: list[str],
                   insert_stmt: str,
                   insert_vals: list[tuple],
                   conn: connection,
                   logger: Logger) -> int:
    """
    Insert the tuples, with values defined in *insert_vals*, into the database.

    The *VALUES* clause in *insert_stmt* must be simply *VALUES %s*.
    Note that, after the execution of *execute_values*, the *cursor.rowcount* property
    will not contain a total result, and thus the value 1 (one) is returned on success.

    :param errors: incidental error messages
    :param insert_stmt: the INSERT command
    :param insert_vals: the list of values to be inserted
    :param conn: optional connection to use (obtains a new one, if not specified)
    :param logger: optional logger
    :return: the number of inserted tuples, or None if an error occurred
    """
    # initialize the return variable
    result: int | None = None

    # make sure to have a connection
    curr_conn: connection = conn or db_connect(errors=errors,
                                               logger=logger)

    # execute the bulk insert
    err_msg: str | None = None
    try:
        # obtain a cursor and perform the operation
        with curr_conn.cursor() as cursor:
            execute_values(cur=cursor,
                           sql=insert_stmt,
                           argslist=insert_vals)
            result = len(insert_vals)
        # commit the transaction
        curr_conn.commit()
    except Exception as e:
        curr_conn.rollback()
        err_msg = _db_except_msg(exception=e,
                                 engine="postgres")
    finally:
        # close the connection, if locally acquired
        if not conn:
            curr_conn.close()

    # log the results
    _db_log(errors=errors,
            err_msg=err_msg,
            logger=logger,
            query_stmt=insert_stmt)

    return result


def db_bulk_copy(errors: list[str],
                 sel_stmt: str,
                 insert_stmt: str,
                 target_engine: str,
                 batch_size: int,
                 where_vals: tuple,
                 target_conn: Any,
                 conn: connection,
                 logger: Logger) -> int:
    """
    Bulk copy data from a Oracle database to another database.

    The destination database brand must be in the list of databases configured and supported by this package.

    :param errors: incidental error messages
    :param sel_stmt: SELECT command for the search
    :param insert_stmt: the insert statement to use for bulk-inserting
    :param target_engine: the destination database engine type
    :param batch_size: number of tuples in the batch, or 0 or None for no limit
    :param where_vals: the values to be associated with the search criteria
    :param target_conn: the connection to the destination database
    :param conn: optional connection to use (obtains a new one, if not specified)
    :param logger: optional logger
    :return: number of tuples effectively copied
    """
    # initialize the return variable
    result: int | None = None

    # make sure to have a connection
    curr_conn: connection = conn or db_connect(errors=errors,
                                               logger=logger)

    err_msg: str | None = None
    try:
        # noinspection DuplicatedCode
        with curr_conn.cursor() as cursor:

            # execute the query
            cursor.execute(statement=sel_stmt,
                           parameters=where_vals)

            # fetch rows in batches/all rows
            result = 0
            op_errors: list[str] = []
            rows: list[tuple]
            if batch_size:
                rows = cursor.fetchmany(batch_size)
            else:
                rows = cursor.fetchall()
            while rows:
                result += db_bulk_insert_to(errors=op_errors,
                                            insert_stmt=insert_stmt,
                                            insert_vals=rows,
                                            engine=target_engine,
                                            conn=target_conn,
                                            logger=logger) or 0
                # errors ?
                if op_errors:
                    # yes, register them and abort the operation
                    errors.extend(op_errors)
                    break
                if not batch_size:
                    break
                rows = cursor.fetchmany(batch_size)

        # commit the source and target transactions
        curr_conn.commit()
        target_conn.commit()
    except Exception as e:
        err_msg = _db_except_msg(exception=e,
                                 engine="postgres")
        curr_conn.rollback()
        target_conn.rollback()
    finally:
        # close the connection, if locally acquired
        if not conn:
            curr_conn.close()

    # log the results
    _db_log(errors=errors,
            err_msg=err_msg,
            logger=logger,
            query_stmt=sel_stmt,
            bind_vals=where_vals)

    return result


def db_execute(errors: list[str],
               exc_stmt: str,
               bind_vals: tuple,
               conn: connection,
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
    :param conn: optional connection to use (obtains a new one, if not specified)
    :param logger: optional logger
    :return: the return value from the command execution
    """
    # initialize the return variable
    result: int | None = None

    # make sure to have a connection
    curr_conn: connection = conn or db_connect(errors=errors,
                                               logger=logger)

    err_msg: str | None = None
    try:
        # obtain a cursor and execute the operation
        with curr_conn.cursor() as cursor:
            cursor.execute(query=f"{exc_stmt};",
                           vars=bind_vals)
            result = cursor.rowcount
        # commit the transaction
        curr_conn.commit()
    except Exception as e:
        curr_conn.rollback()
        err_msg = _db_except_msg(exception=e,
                                 engine="postgres")
    finally:
        # close the connection, if locally acquired
        if not conn:
            curr_conn.close()

    # log the results
    _db_log(errors=errors,
            err_msg=err_msg,
            logger=logger,
            query_stmt=exc_stmt,
            bind_vals=bind_vals)

    return result


def db_call_procedure(errors: list[str],
                      proc_name: str,
                      proc_vals: tuple,
                      conn: connection,
                      logger: Logger) -> list[tuple]:
    """
    Execute the stored procedure *proc_name*, with the arguments given in *proc_vals*.

    :param errors:  incidental error messages
    :param proc_name: the name of the sotred procedure
    :param proc_vals: the arguments to be passed
    :param conn: optional connection to use (obtains a new one, if not specified)
    :param logger: optional logger
    :return: the data returned by the procedure
    """
    # initialize the return variable
    result: list[tuple] = [()]

    # make sure to have a connection
    curr_conn: connection = conn or db_connect(errors=errors,
                                               logger=logger)

    # build the command
    proc_stmt: str = f"{proc_name}(" + "%s, " * (len(proc_vals) - 1) + "%s)"

    # execute the stored procedure
    err_msg: str | None = None
    try:
        # obtain a cursor and perform the operation
        with curr_conn.cursor() as cursor:
            cursor.execute(query=proc_stmt,
                           argslist=proc_vals)
            # retrieve the returned tuples
            result = list(cursor)
        # commit the transaction
        curr_conn.commit()
    except Exception as e:
        curr_conn.rollback()
        err_msg = _db_except_msg(exception=e,
                                 engine="postgres")
    finally:
        # close the connection, if locally acquired
        if not conn:
            curr_conn.close()

    # log the results
    _db_log(errors=errors,
            err_msg=err_msg,
            logger=logger,
            query_stmt=proc_stmt,
            bind_vals=proc_vals)

    return result
