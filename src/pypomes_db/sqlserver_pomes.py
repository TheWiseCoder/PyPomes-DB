from logging import Logger
from pyodbc import connect, Connection, Cursor, Row
from pypomes_core import APP_PREFIX, env_get_str
from typing import Final

from .db_common import (
    DB_NAME, DB_HOST, DB_PORT, DB_PWD, DB_USER,
    _db_log, _db_except_msg, _db_build_query_msg
)

__db_driver: Final[str] = env_get_str(f"{APP_PREFIX}_DB_DRIVER")
__CONNECTION_KWARGS: Final[str] = (
    f"DRIVER={{{__db_driver}}};SERVER={DB_HOST},{DB_PORT};"
    f"DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PWD};TrustServerCertificate=yes;"
)


def db_connect(errors: list[str] | None, logger: Logger = None) -> Connection:
    """
    Obtain and return a connection to the database, or *None* if the connection cannot be obtained.

    :param errors: incidental error messages
    :param logger: optional logger
    :return: the connection to the database
    """
    # inicialize the return valiable
    result: Connection | None = None

    # Obtain a connection to the database
    err_msg: str | None = None
    try:
        result = connect(__CONNECTION_KWARGS)
    except Exception as e:
        err_msg = _db_except_msg(e)

    # log the results
    _db_log(errors, err_msg, logger, f"Connected to '{DB_NAME}'")

    return result


def db_exists(errors: list[str] | None, table: str,
              where_attrs: list[str], where_vals: tuple, logger: Logger = None) -> bool:
    """
    Determine whether the table *table* in the database contains at least one tuple.

    For this determination, the where *where_attrs* are made equal to the
    *where_values* in the query, respectively.
    If more than one, the attributes are concatenated by the *AND* logical connector.

    :param errors: incidental error messages
    :param table: the table to be searched
    :param where_attrs: the search attributes
    :param where_vals: the values for the search attributes
    :param logger: optional logger
    :return: True if at least one tuple was found
    """
    # noinspection PyDataSource
    sel_stmt: str = "SELECT * FROM " + table
    if len(where_attrs) > 0:
        sel_stmt += " WHERE " + "".join(f"{attr} = ? AND " for attr in where_attrs)[0:-5]
    rec: tuple = db_select_one(errors, sel_stmt, where_vals, False, logger)
    result: bool = rec is not None

    return result


def db_select_one(errors: list[str] | None, sel_stmt: str, where_vals: tuple,
                  require_nonempty: bool = False, logger: Logger = None) -> tuple:
    """
    Search the database and return the first tuple that satisfies the *sel_stmt* search command.

    The command can optionally contain search criteria, with respective values given
    in *where_vals*. The list of values for an attribute with the *IN* clause must be contained
    in a specific tuple. In case of error, or if the search is empty, *None* is returned.

    :param errors: incidental error messages
    :param sel_stmt: SELECT command for the search
    :param where_vals: values to be associated with the search criteria
    :param require_nonempty: defines whether an empty search should be considered an error
    :param logger: optional logger
    :return: tuple containing the search result, or None if there was an error, or if the search was empty
    """
    require_min: int = 1 if require_nonempty else None
    reply: list[tuple] = db_select_all(errors, sel_stmt, where_vals, require_min, 1, logger)

    return reply[0] if reply else None


def db_select_all(errors: list[str] | None, sel_stmt: str,  where_vals: tuple,
                  require_min: int = None, require_max: int = None, logger: Logger = None) -> list[tuple]:
    """
    Search the database and return all tuples that satisfy the *sel_stmt* search command.

    The command can optionally contain search criteria, with respective values given
    in *where_vals*. The list of values for an attribute with the *IN* clause must be contained
    in a specific tuple. If the search is empty, an empty list is returned.

    :param errors: incidental error messages
    :param sel_stmt: SELECT command for the search
    :param where_vals: the values to be associated with the search criteria
    :param require_min: optionally defines the minimum number of tuples to be returned
    :param require_max: optionally defines the maximum number of tuples to be returned
    :param logger: optional logger
    :return: list of tuples containing the search result, or [] if the search is empty
    """
    # initialize the return variable
    result: list[tuple] = []

    err_msg: str | None = None
    if isinstance(require_max, int):
        sel_stmt: str = sel_stmt.replace("SELECT", f"SELECT TOP {require_max}", 1)

    try:
        with connect(__CONNECTION_KWARGS) as conn:
            # make sure the connection is not in autocommit mode
            conn.autocommit = False
            # obtain the cursor and execute the operation
            with conn.cursor() as cursor:
                cursor.execute(sel_stmt, where_vals)

                # has an exact number of tuples been defined but not returned ?
                if isinstance(require_min, int) and isinstance(require_max, int) and \
                        require_min == require_max and require_min != cursor.rowcount:
                    # yes, report the error
                    err_msg = (
                       f"{cursor.rowcount} tuples returned, exactly {require_min} expected, "
                       f"for '{_db_build_query_msg(sel_stmt, where_vals)}'"
                    )

                # has a minimum number of tuples been defined but not returned ?
                elif isinstance(require_min, int) and cursor.rowcount < require_min:
                    # yes, report the error
                    err_msg = (
                        f"{cursor.rowcount} tuples returned, at least {require_min} expected, "
                        f"for '{_db_build_query_msg(sel_stmt, where_vals)}'"
                    )

                else:
                    # obtain the returned tuples
                    rows: list[Row] = cursor.fetchall()
                    result = [tuple(row) for row in rows]
            conn.commit()
    except Exception as e:
        err_msg = _db_except_msg(e)

    # log the results
    _db_log(errors, err_msg, logger, sel_stmt, where_vals)

    return result


def db_insert(errors: list[str] | None, insert_stmt: str,
              insert_vals: tuple, logger: Logger = None) -> int:
    """
    Insert a tuple, with values defined in *insert_vals*, into the database.

    :param errors: incidental error messages
    :param insert_stmt: the INSERT command
    :param insert_vals: the values to be inserted
    :param logger: optional logger
    :return: the number of inserted tuples (0 ou 1), or None if an error occurred
    """
    return __db_modify(errors, insert_stmt, insert_vals, logger)


def db_update(errors: list[str] | None, update_stmt: str,
              update_vals: tuple, where_vals: tuple, logger: Logger = None) -> int:
    """
    Update one or more tuples in the database, as defined by the command *update_stmt*.

    The values for this update are in *update_vals*.
    The values for selecting the tuples to be updated are in *where_vals*.

    :param errors: incidental error messages
    :param update_stmt: the UPDATE command
    :param update_vals: the values for the update operation
    :param where_vals: the values to be associated with the search criteria
    :param logger: optional logger
    :return: the number of updated tuples, or None if an error occurred
    """
    values: tuple = update_vals + where_vals
    return __db_modify(errors, update_stmt, values, logger)


def db_delete(errors: list[str] | None, delete_stmt: str,
              where_vals: tuple, logger: Logger = None) -> int:
    """
    Delete one or more tuples in the database, as defined by the *delete_stmt* command.

    The values for selecting the tuples to be deleted are in *where_vals*.

    :param errors: incidental error messages
    :param delete_stmt: the DELETE command
    :param where_vals: the values to be associated with the search criteria
    :param logger: optional logger
    :return: the number of deleted tuples, or None if an error occurred
    """
    return __db_modify(errors, delete_stmt, where_vals, logger)


def db_bulk_insert(errors: list[str] | None, insert_stmt: str,
                   insert_vals: list[tuple], logger: Logger = None) -> int:
    """
    Insert the tuples, with values defined in *insert_vals*, into the database.

    :param errors: incidental error messages
    :param insert_stmt: the INSERT command
    :param insert_vals: the list of values to be inserted
    :param logger: optional logger
    :return: the number of inserted tuples, or None if an error occurred
    """
    # initialize the return variable
    result: int | None = None

    err_msg: str | None = None
    try:
        with connect(__CONNECTION_KWARGS) as conn:
            # make sure the connection is not in autocommit mode
            conn.autocommit = False
            # obtain the cursor and execute the operation
            cursor: Cursor = conn.cursor()
            cursor.fast_executemany = True
            try:
                cursor.executemany(insert_stmt, insert_vals)
                cursor.close()
                result = len(insert_vals)
            except Exception:
                conn.rollback()
                raise
            conn.commit()
    except Exception as e:
        err_msg = _db_except_msg(e)

    # log the results
    _db_log(errors, err_msg, logger, insert_stmt, insert_vals[0])

    return result


def db_exec_stored_procedure(errors: list[str] | None, proc_name: str, proc_vals: tuple,
                             require_nonempty: bool = False, require_count: int = None,
                             logger: Logger = None) -> list[tuple]:
    """
    Execute the stored procedure *proc_name* in the database, with the parameters given in *proc_vals*.

    :param errors: incidental error messages
    :param proc_name: name of the stored procedure
    :param proc_vals: parameters for the stored procedure
    :param require_nonempty: defines whether an empty search should be considered an error
    :param require_count: optionally defines the number of tuples required to be returned
    :param logger: optional logger
    :return: list of tuples containing the search result, or [] if the search is empty
    """
    # initialize the return variable
    result: list[tuple] = []

    # build the command
    proc_stmt: str | None = None

    # execute the stored procedure
    err_msg: str | None = None
    try:
        with connect(__CONNECTION_KWARGS) as conn:
            # make sure the connection is not in autocommit mode
            conn.autocommit = False
            # obtain the cursor and execute the operation
            with conn.cursor() as cursor:
                proc_stmt = f"SET NOCOUNT ON; EXEC {proc_name} {','.join(('?',) * len(proc_vals))}"
                cursor.execute(proc_stmt, proc_vals)

                # has 'require_nonempty' been defined, and the search is empty ?
                if require_nonempty and cursor.rowcount == 0:
                    # yes, report the error
                    err_msg = (
                        f"No tuple returned in '{DB_NAME}' at '{DB_HOST}', "
                        f"for stored procedure '{proc_name}', with values '{proc_vals}'"
                    )

                # has 'require_count' been defined, and a different number of tuples was returned ?
                elif isinstance(require_count, int) and require_count != cursor.rowcount:
                    # yes, report the error
                    err_msg = (
                        f"{cursor.rowcount} tuples returned, "
                        f"but {require_count} expected, in '{DB_NAME}' at '{DB_HOST}', "
                        f"for stored procedure '{proc_name}', with values '{proc_vals}'"
                    )
                else:
                    # obtain the returned tuples
                    rows: list[Row] = cursor.fetchall()
                    result = [tuple(row) for row in rows]
            # commit the transaction
            conn.commit()
    except Exception as e:
        err_msg = _db_except_msg(e)

    # log the results
    _db_log(errors, err_msg, logger, proc_stmt, proc_vals)

    return result


def __db_modify(errors: list[str] | None, modify_stmt: str, bind_vals: tuple, logger: Logger = None) -> int:
    """
    Modify the database, inserting, updating or deleting tuples, according to the *modify_stmt* command definitions.

    The values for this modification, followed by the values for selecting tuples are in *bind_vals*.

    :param errors: incidental error messages
    :param modify_stmt: INSERT, UPDATE, or DELETE command
    :param bind_vals: values for database modification, and for tuples selection
    :param logger: optional logger
    :return: the number of inserted, modified, or deleted tuples, ou None if an error occurred
    """
    # initialize the return variable
    result: int | None = None

    err_msg: str | None = None
    try:
        with connect(__CONNECTION_KWARGS) as conn:
            # make sure the connection is not in autocommit mode
            conn.autocommit = False
            # obtain the cursor and execute the operation
            with conn.cursor() as cursor:
                cursor.execute(modify_stmt, bind_vals)
                result = cursor.rowcount
            # commit the transaction
            conn.commit()
    except Exception as e:
        err_msg = _db_except_msg(e)

    # log the results
    _db_log(errors, err_msg, logger, modify_stmt, bind_vals)

    return result
