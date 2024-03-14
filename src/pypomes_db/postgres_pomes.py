from logging import Logger
from psycopg2 import connect
# noinspection PyProtectedMember
from psycopg2._psycopg import connection

from._db_common import (
    DB_NAME, DB_HOST, DB_PORT, DB_PWD, DB_USER,
    _db_log, _db_except_msg, _db_build_query_msg
)


def db_connect(errors: list[str], logger: Logger = None) -> connection:
    """
    Obtain and return a connection to the database, or *None* if the connection cannot be obtained.

    :param errors: incidental error messages
    :param logger: optional logger
    :return: the connection to the database
    """
    # initialize the return valiable
    result: connection | None = None

    # Obtain a connection to the database
    err_msg: str | None = None
    try:
        result = connect(host=DB_HOST,
                         port=DB_PORT,
                         database=DB_NAME,
                         user=DB_USER,
                         password=DB_PWD)
    except Exception as e:
        err_msg = _db_except_msg(e)

    _db_log(errors, err_msg, logger, f"Connected to '{DB_NAME}'")

    return result


def db_exists(errors: list[str], table: str,
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
        sel_stmt += " WHERE " + "".join(f"{attr} = %s AND " for attr in where_attrs)[0:-5]
    rec: tuple = db_select_one(errors, sel_stmt, where_vals, False, logger)
    result: bool = None if len(errors) > 0 else rec is not None

    return result


def db_select_one(errors: list[str], sel_stmt: str,
                  where_vals: tuple, require_nonempty: bool = False, logger: Logger = None) -> tuple:
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


def db_select_all(errors: list[str], sel_stmt: str, where_vals: tuple,
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
    # initialize the return valiable
    result: list[tuple] = []

    err_msg: str | None = None
    try:
        # obtem o cursor e executa a operação
        with connect(host=DB_HOST,
                     port=DB_PORT,
                     database=DB_NAME,
                     user=DB_USER,
                     password=DB_PWD) as conn, \
             conn.cursor() as cursor:
            cursor.execute(query=f"{sel_stmt};",
                           vars=where_vals)

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
                # for record in cursor:
                #     result.append(record)
                result = cursor
            conn.commit()
    except Exception as e:
        err_msg = _db_except_msg(e)

    _db_log(errors, err_msg, logger, sel_stmt, where_vals)

    return result


def db_insert(errors: list[str], insert_stmt: str, insert_vals: tuple, logger: Logger = None) -> int:
    """
    Insert a tuple, with values defined in *insert_vals*, into the database.

    :param errors: incidental error messages
    :param insert_stmt: the INSERT command
    :param insert_vals: the values to be inserted
    :param logger: optional logger
    :return: the number of inserted tuples (0 ou 1), or None if an error occurred
    """
    return __db_modify(errors, insert_stmt, insert_vals, logger)


def db_update(errors: list[str], update_stmt: str,
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


def db_delete(errors: list[str], delete_stmt: str, where_vals: tuple, logger: Logger = None) -> int:
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

    return result


def db_exec_stored_procedure(errors: list[str] | None, proc_name: str, proc_vals: tuple,
                             require_nonempty: bool = False, require_count: int = None,
                             logger: Logger = None) -> list[tuple]:

    result: list[tuple] = [()]
    return result


# modifica tabela no banco de dados e retorna o número de tuplas afetadas
def __db_modify(errors: list[str], modify_stmt: str, bind_vals: tuple, logger: Logger) -> int:
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
        with connect(host=DB_HOST,
                     port=DB_PORT,
                     database=DB_NAME,
                     user=DB_USER,
                     password=DB_PWD) as conn:
            # make sure the connection is not in autocommit mode
            conn.autocommit = False
            # obtain the cursor and execute the operation
            with conn.cursor() as cursor:
                cursor.execute(query=f"{modify_stmt};",
                               vars=bind_vals)
                result = cursor.rowcount
                conn.commit()
    except Exception as e:
        err_msg = _db_except_msg(e)

    _db_log(errors, err_msg, logger, modify_stmt, bind_vals)

    return result
