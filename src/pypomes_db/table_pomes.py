from logging import Logger
from typing import Any

from .db_pomes import db_execute, db_select
from .db_common import _assert_engine


def db_get_tables(errors: list[str] | None,
                  schema: str = None,
                  engine: str = None,
                  connection: Any = None,
                  committable: bool = True,
                  logger: Logger = None) -> list[str]:
    """
    Retrieve and return the list of tables in the database.

    The returned table names will be qualified with the schema they belong to.

    :param errors: incidental error messages
    :param schema: optional name of the schema to restrict the search to
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'connection' to be provided)
    :param logger: optional logger
    :return: the schema-qualified table names found, or 'None' if an error ocurred
    """
    #initialize the return variable
    result: list[str] | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(errors=op_errors,
                                      engine=engine)
    # proceed, if no errors
    if not op_errors:
        # build the query
        if curr_engine == "oracle":
            sel_stmt: str = "SELECT schema_name || '.' || table_name FROM all_tables"
            if schema:
                sel_stmt += f" WHERE owner = '{schema.upper()}'"
        else:
            sel_stmt: str = ("SELECT table_schema || '.' || table_name "
                             "FROM information_schema.tables "
                             "WHERE table_type = 'BASE TABLE'")
            if schema:
                sel_stmt += f" AND LOWER(table_schema) = '{schema.lower()}'"

        # execute the query
        recs: list[tuple[str]] = db_select(errors=op_errors,
                                           sel_stmt=sel_stmt,
                                           engine=curr_engine,
                                           connection=connection,
                                           committable=committable,
                                           logger=logger)
        # process the query result
        if not op_errors:
            result = [rec[0] for rec in recs]

    # acknowledge eventual local errors, if appropriate
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_table_exists(errors: list[str] | None,
                    table_name: str,
                    engine: str = None,
                    connection: Any = None,
                    committable: bool = True,
                    logger: Logger = None) -> bool:
    """
    Determine whether the table *table_name* exists in the database.

    If *table_name* is schema-qualified, then the search will be restricted to that schema.

    :param errors: incidental error messages
    :param table_name: the, possibly schema-qualified, name of the table to look for
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'connection' to be provided)
    :param logger: optional logger
    :return: 'True' if the table was found, 'False' otherwise, or 'None' if an error ocurred
    """
    #initialize the return variable
    result: bool | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(errors=op_errors,
                                      engine=engine)
    # proceed, if no errors
    if not op_errors:
        # extract the schema, if possible
        schema_name: str | None = None
        splits: list[str] = table_name.split(".")
        if len(splits) > 1:
            schema_name = splits[0]
            table_name = splits[1]

        # build the query
        if curr_engine == "oracle":
            sel_stmt: str = ("SELECT COUNT(*) FROM all_tables "
                             f"WHERE table_name = '{table_name.upper()}'")
            if schema_name:
                sel_stmt += f" AND owner = '{schema_name.upper()}'"
        else:
            sel_stmt: str = ("SELECT COUNT(*) "
                             "FROM information_schema.tables "
                             f"WHERE table_type = 'BASE TABLE' AND "
                             f"LOWER(table_name) = '{table_name.lower()}'")
            if schema_name:
                sel_stmt += f" AND LOWER(table_schema) = '{schema_name.lower()}'"

        # execute the query
        recs: list[tuple[int]] = db_select(errors=op_errors,
                                           sel_stmt=sel_stmt,
                                           engine=curr_engine,
                                           connection=connection,
                                           committable=committable,
                                           logger=logger)
        # process the query result
        if not op_errors:
            result = recs[0][0] > 0

    # acknowledge eventual local errors, if appropriate
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_drop_table(errors: list[str] | None,
                  table_name: str,
                  engine: str = None,
                  connection: Any = None,
                  committable: bool = True,
                  logger: Logger = None) -> None:
    """
    Drop the table given by the, possibly schema-qualified, *table_name*.

    Tis is a silent *DDL* operation. Whether commits or rollbacks are applicable,
    and what their use would entail, depends on the response of the *engine* to the
    mixing of *DDL* and *DML* statements in a transaction.

    :param errors: incidental error messages
    :param table_name: the, possibly schema-qualified, name of the table to drop
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'connection' to be provided)
    :param logger: optional logger
    """
    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: str = _assert_engine(errors=op_errors,
                                      engine=engine)
    # proceed, if no errors
    if not op_errors:
        # build the DROP statement
        if curr_engine == "oracle":
            # oracle has no 'IF EXISTS' clause
            drop_stmt: str = \
                (f"BEGIN"
                 f" EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS'; "
                 "EXCEPTION"
                 " WHEN OTHERS THEN NULL; "
                 "END;")
        elif curr_engine == "postgres":
            drop_stmt: str = \
                ("DO $$"
                 "BEGIN" 
                 f" EXECUTE 'DROP TABLE IF EXISTS {table_name} CASCADE'; "
                 "EXCEPTION"
                 " WHEN OTHERS THEN NULL; "
                 "END $$;")
        elif curr_engine == "sqlserver":
            drop_stmt: str = \
                ("BEGIN TRY" 
                 f" EXEC('DROP TABLE IF EXISTS {table_name} CASCADE;'); "
                 "END TRY "
                 "BEGIN CATCH "
                 "END CATCH;")
        else:
            drop_stmt: str = f"DROP TABLE IF EXISTS {table_name}"

        # drop the table
        db_execute(errors=op_errors,
                   exc_stmt=drop_stmt,
                   engine=curr_engine,
                   connection=connection,
                   committable=committable,
                   logger=logger)

    # acknowledge eventual local errors, if appropriate
    if isinstance(errors, list):
        errors.extend(op_errors)
