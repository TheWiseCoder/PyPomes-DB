# noinspection PyProtectedMember
from pyodbc import connect, Connection, Cursor, Row
from typing import Final
from pypomes_core import APP_PREFIX, env_get_int, env_get_str

# dados para acesso ao BD
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
    Determines whether the *table* table in the database contains at least one tuple where *where_attrs* equals
    *where_values*, respectively. If more than one, the attributes are concatenated by the *AND* logical connector.
    Returns *None* if there was an error in querying the database.

    :param errors: incidental error messages
    :param table: the table to be searched
    :param where_attrs: the search attributes
    :param where_vals: the values for the search attributes
    :return: True if at least one tuple exists
    """
    sel_stmt: str = f"SELECT * FROM {table}"  # noqa
    if len(where_attrs) > 0:
        sel_stmt += " WHERE " + "".join(f"{attr} = ? AND " for attr in where_attrs)[0:-5]
    rec: tuple = db_select_one(errors, sel_stmt, where_vals)
    result: bool = None if len(errors) > 0 else rec is not None

    return result


def db_select_one(errors: list[str], sel_stmt: str,
                  where_vals: tuple, required: bool = False) -> tuple:
    """
    Searches the database and returns the first tuple that satisfies the *sel_stmt* search command.
    The command can optionally contain search criteria, with respective values given
    in *where_vals*. The list of values for an attribute with the *IN* clause must be contained
    in a specific tuple. In case of error, or if the search is empty, *None* is returned.

    :param errors: incidental error messages
    :param sel_stmt: SELECT command for the search
    :param where_vals: list of values to be associated with the search criteria
    :param required: defines whether an empty search should be considered an error
    :return: tuple containing the search result, or None if there was an error, or if the search was empty
    """
    # inicializa a variável de retorno
    result: tuple | None = None

    exc: bool = False
    try:
        with connect(__CONNECTION_KWARGS) as conn:
            # obtem o cursor e executa a operação
            with conn.cursor() as cursor:
                sel_stmt = sel_stmt.replace("SELECT", "SELECT TOP 1")
                cursor.execute(sel_stmt, where_vals)
                # obtem a primeira tupla retornada pelo SELECT (None se nenhuma foi retornada)
                result = cursor.fetchone()
    except Exception as e:
        exc = True
        errors.append(__db_except_msg(e))

    # o parâmetro 'required' foi definido e nenhum registro foi obtido?
    if required and not exc and result is None:
        # sim, reporte o erro
        errors.append(__db_required_msg(sel_stmt, where_vals))

    return result


def db_select_all(errors: list[str], sel_stmt: str,
                  where_vals: tuple, required: bool = False) -> list[tuple]:
    """
    Searches the database and returns all tuples that satisfy the *sel_stmt* search command.
    The command can optionally contain search criteria, with respective values given
    in *where_vals*. The list of values for an attribute with the *IN* clause must be contained
    in a specific tuple. If the search is empty, an empty list is returned.

    :param errors: incidental error messages
    :param sel_stmt: SELECT command for thew search
    :param where_vals: the values to be associated with the search criteria
    :param required: defines whether empty search should be considered an error
    :return: list of tuples containing the search result, or [] if the search is empty
    """
    # inicialize the return variable
    result: list[tuple] = []

    exc: bool = False
    try:
        with connect(__CONNECTION_KWARGS) as conn:
            # obtain the cursor and execute the operation
            with conn.cursor() as cursor:
                cursor.execute(sel_stmt, where_vals)
                # obtain the returned tuples
                rows: list[Row] = cursor.fetchall()
                for row in rows:
                    values: list = [item for item in row]
                    result.append(tuple(values))
    except Exception as e:
        exc = True
        errors.append(__db_except_msg(e))

    # no errors, the 'required' parameter has been defined, and the search is empty ?
    if required and not exc and len(result) == 0:
        # yes, report the error
        errors.append(__db_required_msg(sel_stmt, where_vals))

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
    :return: the number of updated tuples
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
    :return: the number of deleted tuples
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
    # inicialize the return variable
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
                result = len(insert_vals)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
    except Exception as e:
        errors.append(__db_except_msg(e))

    return result


def db_exec_stored_procedure(errors: list[str], proc_name: str, proc_vals: tuple) -> list[tuple]:
    """
    Execute the stored procedure *proc_name* in the database, with the parameters given in *proc_vals*.

    :param errors: incidental error messages
    :param proc_name: name of the stored procedure
    :param proc_vals: parameters for the stored procedure
    :return: list of tuples containing the search result, or [] if the search is empty
    """
    # inicialize the return variable
    result: list[tuple] = []

    try:
        with connect(__CONNECTION_KWARGS) as conn:
            # obtain the cursor and execute the operation
            with conn.cursor() as cursor:
                stmt = f"SET NOCOUNT ON; EXEC {proc_name} {','.join(('?',) * len(proc_vals))}"
                cursor.execute(stmt, proc_vals)
                # obtain the tuples returned
                rows: list[Row] = cursor.fetchall()
                for row in rows:
                    values: list = [item for item in row]
                    result.append(tuple(values))
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
    # inicialize the return variable
    result: int | None = None

    try:
        with connect(__CONNECTION_KWARGS) as conn:
            # obtain the cursor and execute the operation
            with conn.cursor() as cursor:
                cursor.execute(modify_stmt, bind_vals)
                result = cursor.rowcount
                conn.commit()
    except Exception as e:
        errors.append(__db_except_msg(e))

    return result


def __db_except_msg(exception: Exception) -> str:
    """
    Formats and returns the error message corresponding to the exception raised
    on accessing to the database.

    :param exception: the exception raised
    :return:the formatted error message
    """
    exc_msg: str = f"{exception}"
    exc_msg = exc_msg.replace('"', "'") \
                     .replace('\n', " ") \
                     .replace('\t', " ") \
                     .replace("\\", "")
    result = f"Error accessing {DB_NAME} at {DB_HOST}: {exc_msg}"

    return result


def __db_required_msg(sel_stmt: str, where_vals: tuple) -> str:
    """
    Formats and returns the message indicative of empty search.

    :param sel_stmt: the search command
    :param where_vals: values associatred with the search criteria
    :return: message indicative of empty search
    """
    stmt: str = sel_stmt.replace('"', "'") \
                        .replace('\n', " ") \
                        .replace('\t', " ") \
                        .replace("\\", "")
    result: str = f"No record found in {DB_NAME} at {DB_HOST}, for {stmt}"

    for val in where_vals:
        if isinstance(val, str):
            val = f"'{val}'"
        else:
            val = str(val)
        result = result.replace("%s", val, 1)

    return result
