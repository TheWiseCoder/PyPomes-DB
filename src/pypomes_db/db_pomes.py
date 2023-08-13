from logging import Logger
from pyodbc import connect, Connection, Cursor, Row
from typing import Final
from pypomes_core import APP_PREFIX, env_get_int, env_get_str

DB_DRIVER: Final[str] = env_get_str(f"{APP_PREFIX}_DB_DRIVER")
DB_NAME: Final[str] = env_get_str(f"{APP_PREFIX}_DB_NAME")
DB_HOST: Final[str] = env_get_str(f"{APP_PREFIX}_DB_HOST")
DB_PORT: Final[int] = env_get_int(f"{APP_PREFIX}_DB_PORT")
DB_PWD: Final[str] = env_get_str(f"{APP_PREFIX}_DB_PWD")
DB_USER: Final[str] = env_get_str(f"{APP_PREFIX}_DB_USER")

__CONNECTION_KWARGS: Final[str] = (
    f"DRIVER={{{DB_DRIVER}}};SERVER={DB_HOST},{DB_PORT};"
    f"DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PWD};TrustServerCertificate=yes;"
)


def db_connect(errors: list[str] | None, logger: Logger = None) -> Connection:
    """
    Obtain and return a connection to the database, or *None* if the connection cannot be obtained.

    :param errors: incidental error messages
    :param logger: optional logger
    :return: the connection to the database
    """
    # inicializa a variável de retorno
    result: Connection | None = None

    # Obtém a conexão com o BD
    err_msg: str | None = None
    try:
        result = connect(__CONNECTION_KWARGS)
    except Exception as e:
        err_msg = __db_except_msg(e)

    __db_log(errors, err_msg, logger, f"Connected to '{DB_NAME}'")

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
    rec: tuple = db_select_one(errors, sel_stmt, where_vals, False, False, logger)
    result: bool = rec is not None

    return result


def db_select_one(errors: list[str] | None, sel_stmt: str, where_vals: tuple,
                  require_nonempty: bool = False, require_singleton: bool = False, logger: Logger = None) -> tuple:
    """
    Search the database and return the first tuple that satisfies the *sel_stmt* search command.

    The command can optionally contain search criteria, with respective values given
    in *where_vals*. The list of values for an attribute with the *IN* clause must be contained
    in a specific tuple. In case of error, or if the search is empty, *None* is returned.

    :param errors: incidental error messages
    :param sel_stmt: SELECT command for the search
    :param where_vals: values to be associated with the search criteria
    :param require_nonempty: defines whether an empty search should be considered an error
    :param require_singleton: defines whether a non-singleton search should be considered an error
    :param logger: optional logger
    :return: tuple containing the search result, or None if there was an error, or if the search was empty
    """
    # initialize the return variable
    result: tuple | None = None

    err_msg: str | None = None
    try:
        with connect(__CONNECTION_KWARGS) as conn:
            # make sure the connection is not in autocommit mode
            conn.autocommit = False
            # obtain the cursor and execute the operation
            with conn.cursor() as cursor:
                sel_stmt = sel_stmt.replace("SELECT", "SELECT TOP 1", 1)
                cursor.execute(sel_stmt, where_vals)

                # has 'require_nonempty' been defined, and the search is empty ?
                if require_nonempty and cursor.rowcount == 0:
                    # yes, report the error
                    err_msg = (
                        f"No tuple returned in '{DB_NAME}' at '{DB_HOST}', "
                        f"for '{__db_build_query_msg(sel_stmt, where_vals)}'"
                    )

                # has 'require_singleton' been defined, and more the one tuple was returned ?
                elif require_singleton and cursor.rowcount > 0:
                    # yes, report the error
                    err_msg = (
                        f"Singleton expected, but {cursor.rowcount} tuples returned "
                        f"('{DB_NAME}' at '{DB_HOST}', for '{__db_msg_clean(sel_stmt)}')"
                    )

                else:
                    # obtain the first tuple returned (None if no tuple was returned)
                    rec: Row = cursor.fetchone()
                    if rec is not None:
                        result = tuple(rec)
            conn.commit()
    except Exception as e:
        err_msg = __db_except_msg(e)

    __db_log(errors, err_msg, logger, f"To '{DB_NAME}': {__db_build_query_msg(sel_stmt, where_vals)}")

    return result


def db_select_all(errors: list[str] | None, sel_stmt: str,  where_vals: tuple,
                  require_nonempty: bool = False, require_count: int = None, logger: Logger = None) -> list[tuple]:
    """
    Search the database and return all tuples that satisfy the *sel_stmt* search command.

    The command can optionally contain search criteria, with respective values given
    in *where_vals*. The list of values for an attribute with the *IN* clause must be contained
    in a specific tuple. If the search is empty, an empty list is returned.

    :param errors: incidental error messages
    :param sel_stmt: SELECT command for the search
    :param where_vals: the values to be associated with the search criteria
    :param require_nonempty: defines whether an empty search should be considered an error
    :param require_count: optionally defines the number of tuples required to be returned
    :param logger: optional logger
    :return: list of tuples containing the search result, or [] if the search is empty
    """
    # initialize the return variable
    result: list[tuple] = []

    err_msg: str | None = None
    try:
        with connect(__CONNECTION_KWARGS) as conn:
            # make sure the connection is not in autocommit mode
            conn.autocommit = False
            # obtain the cursor and execute the operation
            with conn.cursor() as cursor:
                cursor.execute(sel_stmt, where_vals)

                # has 'require_nonempty' been defined, and the search is empty ?
                if require_nonempty and cursor.rowcount == 0:
                    # yes, report the error
                    err_msg = (
                        f"No tuple returned in '{DB_NAME}' at '{DB_HOST}', "
                        f"for '{__db_build_query_msg(sel_stmt, where_vals)}'"
                    )

                # has 'require_count' been defined, and a different number of tuples was returned ?
                elif isinstance(require_count, int) and require_count != cursor.rowcount:
                    # yes, report the error
                    err_msg = (
                       f"{cursor.rowcount} tuples returned, "
                       f"but {require_count} expected, in '{DB_NAME}' at '{DB_HOST}', "
                       f"for '{__db_build_query_msg(sel_stmt, where_vals)}'"
                   )

                else:
                    # obtain the returned tuples
                    rows: list[Row] = cursor.fetchall()
                    result = [tuple(row) for row in rows]
            conn.commit()
    except Exception as e:
        err_msg = __db_except_msg(e)

    __db_log(errors, err_msg, logger, f"To '{DB_NAME}': {__db_build_query_msg(sel_stmt, where_vals)}")

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
        err_msg = __db_except_msg(e)

    __db_log(errors, err_msg, logger, f"To '{DB_NAME}': {__db_msg_clean(insert_stmt)}")

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

    err_msg: str | None = None
    proc_stmt: str | None = None
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
            conn.commit()
    except Exception as e:
        err_msg = __db_except_msg(e)

    __db_log(errors, err_msg, logger, f"To '{DB_NAME}': {__db_build_query_msg(proc_stmt, proc_vals)}")

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
            conn.commit()
    except Exception as e:
        err_msg = __db_except_msg(e)

    __db_log(errors, err_msg, logger, f"To '{DB_NAME}': {__db_build_query_msg(modify_stmt, bind_vals)}")

    return result


def __db_msg_clean(msg: str) -> str:
    """
    Clean the given *msg* string.

    The cleaning is carriedc out by replacing double quotes with single quotes,
    and newlines and tabs with whitespace, and by removing backslashes.

    :param msg: the string to be cleaned
    :return: the cleaned string
    """
    return msg.replace('"', "'") \
              .replace("\n", " ") \
              .replace("\t", " ") \
              .replace("\\", "")


def __db_except_msg(exception: Exception) -> str:
    """
    Format and return the error message corresponding to the exception raised while accessing the database.

    :param exception: the exception raised
    :return:the formatted error message
    """
    return f"Error accessing '{DB_NAME}' at '{DB_HOST}': {__db_msg_clean(f'{exception}')}"


def __db_build_query_msg(query_stmt: str, bind_vals: tuple) -> str:
    """
    Format and return the message indicative of an empty search.

    :param query_stmt: the query command
    :param bind_vals: values associated with the query command
    :return: message indicative of empty search
    """
    result: str = __db_msg_clean(query_stmt)

    for val in bind_vals:
        if isinstance(val, str):
            sval: str = f"'{val}'"
        else:
            sval: str = str(val)
        result = result.replace("?", sval, 1)

    return result


def __db_log(errors: list[str] | None, err_msg: str, logger: Logger, debug_msg):

    if err_msg:
        if errors:
            errors.append(err_msg)
        if logger:
            logger.error(err_msg)
    elif logger:
        logger.debug(debug_msg)