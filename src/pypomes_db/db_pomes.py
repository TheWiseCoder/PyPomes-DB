# noinspection PyProtectedMember
from pyodbc import connect, Connection, Cursor, Row
from typing import Final
from pypomes_core import APP_PREFIX, env_get_int, env_get_str

DB_DRIVER: Final[str] = env_get_str(f"{APP_PREFIX}_DB_DRIVER")
DB_NAME: Final[str] = env_get_str(f"{APP_PREFIX}_DB_NAME")
DB_HOST: Final[str] = env_get_str(f"{APP_PREFIX}_DB_HOST")
DB_PORT: Final[int] = env_get_int(f"{APP_PREFIX}_DB_PORT")
DB_PWD: Final[str] = env_get_str(f"{APP_PREFIX}_DB_PWD")
DB_USER: Final[str] = env_get_str(f"{APP_PREFIX}_DB_USER")

__CONNECTION_KWARGS: Final[str] = f"DRIVER={{{DB_DRIVER}}};SERVER={DB_HOST},{DB_PORT};" \
                                  f"DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PWD};TrustServerCertificate=yes;"


def db_connect(errors: list[str]) -> Connection:
    """
    Obtains and returns a connection to the database, or *None* if the connection cannot be obtained.

    :param errors: incidental error messages
    :return: the connection to the database
    """
    # inicializa a variável de retorno
    result: Connection | None = None

    # Obtém a conexão com o BD
    try:
        result = connect(__CONNECTION_KWARGS)
    except Exception as e:
        errors.append(__db_except_msg(e))

    return result


def db_exists(errors: list[str], table: str, where_attrs: list[str], where_vals: tuple) -> bool:
    """
    Determines whether the table *table* in the database contains at least one tuple where *where_attrs* equals
    *where_values*, respectively. If more than one, the attributes are concatenated by the *AND* logical connector.
    Returns *None* if there was an error in querying the database.

    :param errors: incidental error messages
    :param table: the table to be searched
    :param where_attrs: the search attributes
    :param where_vals: the values for the search attributes
    :return: True if at least one tuple was found
    """
    sel_stmt: str = f"SELECT * FROM {table}"  # noqa
    if len(where_attrs) > 0:
        sel_stmt += " WHERE " + "".join(f"{attr} = ? AND " for attr in where_attrs)[0:-5]
    rec: tuple = db_select_one(errors, sel_stmt, where_vals)
    result: bool = None if len(errors) > 0 else rec is not None

    return result


def db_select_one(errors: list[str], sel_stmt: str, where_vals: tuple,
                  require_nonempty: bool = False, require_singleton: bool = False) -> tuple:
    """
    Searches the database and returns the first tuple that satisfies the *sel_stmt* search command.
    The command can optionally contain search criteria, with respective values given
    in *where_vals*. The list of values for an attribute with the *IN* clause must be contained
    in a specific tuple. In case of error, or if the search is empty, *None* is returned.

    :param errors: incidental error messages
    :param sel_stmt: SELECT command for the search
    :param where_vals: values to be associated with the search criteria
    :param require_nonempty: defines whether an empty search should be considered an error
    :param require_singleton: defines whether a non-singleton search should be considered an error
    :return: tuple containing the search result, or None if there was an error, or if the search was empty
    """
    # initialize the return variable
    result: tuple | None = None

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
                    errors.append(f"No tuple returned in '{DB_NAME}' at '{DB_HOST}', "
                                  f"for '{__db_build_query_msg(sel_stmt, where_vals)}'")

                # has 'require_singleton' been defined, and more the one tuple was returned ?
                elif require_singleton and cursor.rowcount > 0:
                    # yes, report the error
                    errors.append(f"Singleton expected, but {cursor.rowcount} tuples returned "
                                  f"('{DB_NAME}' at '{DB_HOST}', for '{__db_msg_clean(sel_stmt)}')")

                else:
                    # obtain the first tuple returned (None if no tuple was returned)
                    rec: Row = cursor.fetchone()
                    if rec is not None:
                        result = tuple(rec)
            conn.commit()
    except Exception as e:
        errors.append(__db_except_msg(e))

    return result


def db_select_all(errors: list[str], sel_stmt: str,  where_vals: tuple,
                  require_nonempty: bool = False, require_count: int = None) -> list[tuple]:
    """
    Searches the database and returns all tuples that satisfy the *sel_stmt* search command.
    The command can optionally contain search criteria, with respective values given
    in *where_vals*. The list of values for an attribute with the *IN* clause must be contained
    in a specific tuple. If the search is empty, an empty list is returned.

    :param errors: incidental error messages
    :param sel_stmt: SELECT command for the search
    :param where_vals: the values to be associated with the search criteria
    :param require_nonempty: defines whether an empty search should be considered an error
    :param require_count: optionally defines the number of tuples required to be returned
    :return: list of tuples containing the search result, or [] if the search is empty
    """
    # initialize the return variable
    result: list[tuple] = []

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
                    errors.append(f"No tuple returned in '{DB_NAME}' at '{DB_HOST}', "
                                  f"for '{__db_build_query_msg(sel_stmt, where_vals)}'")

                # has 'require_count' been defined, and a different number of tuples was returned ?
                elif isinstance(require_count, int) and require_count != cursor.rowcount:
                    # yes, report the error
                    errors.append(f"{cursor.rowcount} tuples returned, "
                                  f"but {require_count} expected, in '{DB_NAME}' at '{DB_HOST}', "
                                  f"for '{__db_build_query_msg(sel_stmt, where_vals)}'")

                else:
                    # obtain the returned tuples
                    rows: list[Row] = cursor.fetchall()
                    for row in rows:
                        result.append(tuple(row))
            conn.commit()
    except Exception as e:
        errors.append(__db_except_msg(e))

    return result


def db_insert(errors: list[str], insert_stmt: str, insert_vals: tuple) -> int:
    """
    Inserts a tuple, with values defined in *insert_vals*, into the database.

    :param errors: incidental error messages
    :param insert_stmt: the INSERT command
    :param insert_vals: the values to be inserted
    :return: the number of inserted tuples (0 ou 1), or None if an error occurred
    """
    return __db_modify(errors, insert_stmt, insert_vals)


def db_update(errors: list[str], update_stmt: str,
              update_vals: tuple, where_vals: tuple) -> int:
    """
    Updates one or more tuples in the database, as defined by the command
    *update_stmt*. The values for this update are in *update_vals*.
    The values for selecting the tuples to be updated are in *where_vals*.

    :param errors: incidental error messages
    :param update_stmt: the UPDATE command
    :param update_vals: the values for the update operation
    :param where_vals: the values to be associated with the search criteria
    :return: the number of updated tuples, or None if an error occurred
    """
    values: tuple = update_vals + where_vals
    return __db_modify(errors, update_stmt, values)


def db_delete(errors: list[str], delete_stmt: str, where_vals: tuple) -> int:
    """
    Deletes one or more tuples in the database, as defined by the *delete_stmt* command.
    The values for selecting the tuples to be deleted are in *where_vals*.

    :param errors: incidental error messages
    :param delete_stmt: the DELETE command
    :param where_vals: the values to be associated with the search criteria
    :return: the number of deleted tuples, or None if an error occurred
    """
    return __db_modify(errors, delete_stmt, where_vals)


def db_bulk_insert(errors: list[str], insert_stmt: str, insert_vals: list[tuple]) -> int:
    """
    Inserts the tuples, with values defined in *insert_vals*, into the database.

    :param errors: incidental error messages
    :param insert_stmt: the INSERT command
    :param insert_vals: the list of values to be inserted
    :return: the number of inserted tuples, or None if an error occurred
    """
    # initialize the return variable
    result: int | None = None

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
        errors.append(__db_except_msg(e))

    return result


def db_exec_stored_procedure(errors: list[str], proc_name: str, proc_vals: tuple,
                             require_nonempty: bool = False, require_count: int = None) -> list[tuple]:
    """
    Execute the stored procedure *proc_name* in the database, with the parameters given in *proc_vals*.

    :param errors: incidental error messages
    :param proc_name: name of the stored procedure
    :param proc_vals: parameters for the stored procedure
    :param require_nonempty: defines whether an empty search should be considered an error
    :param require_count: optionally defines the number of tuples required to be returned
    :return: list of tuples containing the search result, or [] if the search is empty
    """
    # initialize the return variable
    result: list[tuple] = []

    try:
        with connect(__CONNECTION_KWARGS) as conn:
            # make sure the connection is not in autocommit mode
            conn.autocommit = False
            # obtain the cursor and execute the operation
            with conn.cursor() as cursor:
                stmt = f"SET NOCOUNT ON; EXEC {proc_name} {','.join(('?',) * len(proc_vals))}"
                cursor.execute(stmt, proc_vals)

                # has 'require_nonempty' been defined, and the search is empty ?
                if require_nonempty and cursor.rowcount == 0:
                    # yes, report the error
                    errors.append(f"No tuple returned in '{DB_NAME}' at '{DB_HOST}', "
                                  f"for stored procedure '{proc_name}', with values '{proc_vals}'")

                # has 'require_count' been defined, and a different number of tuples was returned ?
                elif isinstance(require_count, int) and require_count != cursor.rowcount:
                    # yes, report the error
                    errors.append(f"{cursor.rowcount} tuples returned, "
                                  f"but {require_count} expected, in '{DB_NAME}' at '{DB_HOST}', "
                                  f"for stored procedure '{proc_name}', with values '{proc_vals}'")
                else:
                    # obtain the returned tuples
                    rows: list[Row] = cursor.fetchall()
                    for row in rows:
                        result.append(tuple(row))
            conn.commit()
    except Exception as e:
        errors.append(__db_except_msg(e))

    return result


def __db_modify(errors: list[str], modify_stmt: str, bind_vals: tuple) -> int:
    """
    Modifies the database, inserting, updating or deleting tuples, according to the
    *modify_stmt* command definitions. The values for this modification, followed by the
    values for selecting tuples are in *bind_vals*.

    :param errors: incidental error messages
    :param modify_stmt: INSERT, UPDATE, or DELETE command
    :param bind_vals: values for database modification, and for tuples selection
    :return: the number of inserted, modified, or deleted tuples, ou None if an error occurred
    """
    # initialize the return variable
    result: int | None = None

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
        errors.append(__db_except_msg(e))

    return result


def __db_msg_clean(msg: str) -> str:
    """
    Clean the given *msg* string, by replacing double quotes with single quotes,
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
    Formats and returns the error message corresponding to the exception raised
    while accessing the database.

    :param exception: the exception raised
    :return:the formatted error message
    """
    return f"Error accessing {DB_NAME} at {DB_HOST}: {__db_msg_clean(f'{exception}')}"


def __db_build_query_msg(sel_stmt: str, where_vals: tuple) -> str:
    """
    Formats and returns the message indicative of an empty search.

    :param sel_stmt: the search command
    :param where_vals: values associated with the search criteria
    :return: message indicative of empty search
    """
    result: str = __db_msg_clean(sel_stmt)

    for val in where_vals:
        if isinstance(val, str):
            val = f"'{val}'"
        else:
            val = str(val)
        result = result.replace("?", val, 1)

    return result
