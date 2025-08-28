from logging import Logger
from typing import Any

from . import DbParam
from .db_common import (
    _DB_CONN_DATA, DbEngine, _assert_engine, _get_param
)
from .db_pomes import db_execute, db_select


def db_create_session_table(engine: DbEngine,
                            connection: Any,
                            table_name: str,
                            column_data: list[str],
                            errors: list[str] = None,
                            logger: Logger = None) -> None:
    """
    Create the session-scoped table *table_name*, with the list of columns specifications in *column_data*.

    The table created has the scope of the given *connection*, its data is kept throughout the session,
    and is automatically dropped when the session ends, that is, when *connection* is closed.

    Note that there is no support for session-scoped temporary tables for versions of *Oracle* before 18c,
    and the newer versions which do support them require their names to start with *ORA$PTT_*.
    In case these two conditions are not met, a global temporary table is created, instead.
    Although its data are session-specific, the table definition for such table remains in the database
    permanently, and thus the non-existence of a table named *table_name* is verified beforehand.

    For *SQLServer*, session-scoped temporary table names must start with the hash symbol *#*, an error
    being raised if otherwise.

    :param engine: the reference database engine
    :param connection: the connection defining the session
    :param table_name: the, possibly schema-qualified, name of the table to be created
    :param column_data: this list of column information for the table cration
    :param errors: incidental error messages
    :param logger: optional logger
    """
    # assert the database engine
    if not isinstance(errors, list):
        errors = []
    engine = _assert_engine(engine=engine,
                            errors=errors)
    if engine:
        stmt: str | None = None
        match engine:
            case DbEngine.POSTGRES:
                # - 'TEMP' or 'TEMPORARY' creates a session-scoped table
                # - 'ON COMMIT PRESERVE ROWS' ensures rows are kept until session ends
                stmt = (f"CREATE TEMP TABLE {table_name} "
                        f"({', '.join(column_data)}) ON COMMIT PRESERVE ROWS")
            case DbEngine.MYSQL:
                # 'TEMPORARY' ensures the table is dropped when the session ends
                stmt = f"CREATE TEMPORARY TABLE {table_name} ({', '.join(column_data)})"
            case DbEngine.ORACLE:
                # private, session-scoped, temporary tables require:
                #   - database version 18c or later
                #   - name starting with 'ORA$PTT_'
                version: str = _get_param(engine=engine,
                                          param=DbParam.VERSION)
                if version and version > "18" and table_name.upper().startswith("ORA$PTT_"):
                    # 'ON COMMIT PRESERVE DEFINITION' ensures the table lasts for the session
                    stmt = (f"CREATE PRIVATE TEMPORARY TABLE {table_name.upper()} "
                            f"({', '.join(column_data)}) ON COMMIT PRESERVE DEFINITION")
                elif not db_table_exists(table_name=table_name,
                                         engine=DbEngine.ORACLE,
                                         errors=errors,
                                         logger=logger) and not errors:
                    # - the table definition remains in the database permanently, but the data is session-specific
                    # - 'ON COMMIT PRESERVE ROWS' keeps data for the entire session
                    stmt = (f"CREATE GLOBAL TEMPORARY TABLE {table_name} "
                            f"({'. '.join(column_data)}) ON COMMIT PRESERVE ROWS")
            case DbEngine.SQLSERVER:
                # 'table_name' must be prepended with '#', which creates a local, session-scoped, temporary table
                stmt = f"CREATE TABLE {table_name} ({', '.join(column_data)})"

        if stmt:
            db_execute(exc_stmt=stmt,
                       engine=engine,
                       connection=connection,
                       errors=errors,
                       logger=logger)


def db_get_session_table_prefix(engine: DbEngine) -> str:
    """
    Return the prefix required in the table's name for it to be created as a session-scoped table.

    The prefix is returned in uppercase. For contexts which do not require such a prefix, such as
    *MySQL* or *PostgreSQL* engines, or *Oracle* engines prior to version 18c, an empty string is returned.

    :param engine: the reference database engine
    :return: th prefix required for session tables, or an empty string if otherwise
    """
    # initialize the return variable
    result: str = ""

    # assert the database engine
    engine = next(iter(_DB_CONN_DATA)) if not engine and _DB_CONN_DATA else engine

    if engine == DbEngine.ORACLE:
        version: str = _get_param(engine=engine,
                                  param=DbParam.VERSION)
        if version and version > "18":
            result = "ORA$PTT_"
    elif engine == DbEngine.SQLSERVER:
        result = "#"

    return result


def db_get_tables(schema: str = None,
                  engine: DbEngine = None,
                  connection: Any = None,
                  committable: bool = None,
                  errors: list[str] = None,
                  logger: Logger = None) -> list[str] | None:
    """
    Retrieve the list of schema-qualified tables in the database.

    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param schema: optional name of the schema to restrict the search to
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation on errorless completion
    :param errors: incidental error messages
    :param logger: optional logger
    :return: the schema-qualified table names found, or *None* if error
    """
    # initialize the return variable
    result: list[str] | None = None

    # assert the database engine
    if not isinstance(errors, list):
        errors = []
    engine = _assert_engine(engine=engine,
                            errors=errors)
    if not errors:
        # build the query
        if engine == DbEngine.ORACLE:
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
        recs: list[tuple[str]] = db_select(sel_stmt=sel_stmt,
                                           engine=engine,
                                           connection=connection,
                                           committable=committable,
                                           errors=errors,
                                           logger=logger)
        # process the query result
        if not errors:
            result = [rec[0] for rec in recs]

    return result


def db_table_exists(table_name: str,
                    engine: DbEngine = None,
                    connection: Any = None,
                    committable: bool = None,
                    errors: list[str] = None,
                    logger: Logger = None) -> bool | None:
    """
    Determine whether the table *table_name* exists in the database.

    If *table_name* is schema-qualified, then the search will be restricted to that schema.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param table_name: the, possibly schema-qualified, name of the table to look for
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation on errorless completion
    :param errors: incidental error messages
    :param logger: optional logger
    :return: *True* if the table was found, *False* otherwise, or *None* if error
    """
    # initialize the return variable
    result: bool | None = None

    # assert the database engine
    if not isinstance(errors, list):
        errors = []
    if not isinstance(errors, list):
        errors = []
    engine = _assert_engine(engine=engine,
                            errors=errors)
    if not errors:
        # determine the table schema
        table_schema: str
        table_schema, table_name = table_name.split(sep=".") if "." in table_name else (None, table_name)

        # build the query
        if engine == DbEngine.ORACLE:
            sel_stmt: str = ("SELECT COUNT(*) FROM all_tables "
                             f"WHERE table_name = '{table_name.upper()}'")
            if table_schema:
                sel_stmt += f" AND owner = '{table_schema.upper()}'"
        else:
            sel_stmt: str = ("SELECT COUNT(*) "
                             "FROM information_schema.tables "
                             f"WHERE table_type = 'BASE TABLE' AND "
                             f"LOWER(table_name) = '{table_name.lower()}'")
            if table_schema:
                sel_stmt += f" AND LOWER(table_schema) = '{table_schema.lower()}'"

        # execute the query
        recs: list[tuple[int]] = db_select(sel_stmt=sel_stmt,
                                           engine=engine,
                                           connection=connection,
                                           committable=committable,
                                           errors=errors,
                                           logger=logger)
        # process the query result
        if not errors:
            result = recs[0][0] > 0

    return result


def db_drop_table(table_name: str,
                  engine: DbEngine = None,
                  connection: Any = None,
                  committable: bool = None,
                  errors: list[str] = None,
                  logger: Logger = None) -> None:
    """
    Drop the table given by the, possibly schema-qualified, *table_name*.

    This is a silent *DDL* operation. Whether commits or rollbacks are applicable,
    and what their use would entail, depends on the response of the *engine* to the
    mixing of *DDL* and *DML* statements in a transaction.

    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param table_name: the, possibly schema-qualified, name of the table to drop
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation on errorless completion
    :param errors: incidental error messages
    :param logger: optional logger
    """
    # assert the database engine
    if not isinstance(errors, list):
        errors = []
    engine = _assert_engine(engine=engine,
                            errors=errors)
    if not errors:
        # build the DROP statement
        if engine == DbEngine.ORACLE:
            # oracle has no 'IF EXISTS' clause
            drop_stmt: str = \
                (f"BEGIN"
                 f" EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS'; "
                 "EXCEPTION"
                 " WHEN OTHERS THEN NULL; "
                 "END;")
        elif engine == DbEngine.POSTGRES:
            drop_stmt: str = \
                ("DO $$"
                 "BEGIN"
                 f" EXECUTE 'DROP TABLE IF EXISTS {table_name} CASCADE'; "
                 "EXCEPTION"
                 " WHEN OTHERS THEN NULL; "
                 "END $$;")
        elif engine == DbEngine.SQLSERVER:
            drop_stmt: str = \
                ("BEGIN TRY"
                 f" EXEC('DROP TABLE IF EXISTS {table_name} CASCADE;'); "
                 "END TRY "
                 "BEGIN CATCH "
                 "END CATCH;")
        else:
            drop_stmt: str = f"DROP TABLE IF EXISTS {table_name}"

        # drop the table
        db_execute(exc_stmt=drop_stmt,
                   engine=engine,
                   connection=connection,
                   committable=committable,
                   errors=errors,
                   logger=logger)


def db_get_table_columns(table_name: str,
                         engine: DbEngine = None,
                         connection: Any = None,
                         committable: bool = None,
                         errors: list[str] = None,
                         logger: Logger = None) -> list[tuple[str, str, bool]] | None:
    """
    Retrieve all columns in *table_name*, along with their respective data types and nullabilities.

    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param table_name: the possibly schema-qualified name of the table
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation on errorless completion
    :param errors: incidental error messages
    :param logger: optional logger
    :return: a list of tuples with names, types and the nullability state of all columns in *table_name*
    """
    # initialize the return variable
    result: list[tuple[str, str, bool]] | None = None

    # assert the database engine
    if not isinstance(errors, list):
        errors = []
    engine = _assert_engine(engine=engine,
                            errors=errors)
    if not errors:
        # determine the table schema
        table_schema: str
        table_schema, table_name = table_name.split(sep=".") if "." in table_name else (None, table_name)

        # build the query
        sel_stmt: str | None = None
        where_data: dict[str, str] = {}
        if engine == DbEngine.MYSQL:
            pass
        if engine == DbEngine.ORACLE:
            sel_stmt = "SELECT COLUMN_NAME, DATA_TYPE, NULLABLE FROM ALL_TAB_COLUMNS"
            where_data["TABLE_NAME"] = table_name
            if table_schema:
                where_data["OWNER"] = table_schema
        elif engine == DbEngine.POSTGRES:
            sel_stmt = "SELECT column_name, udt_name, is_nullable FROM information_schema.columns"
            where_data["table_name"] = table_name
            if table_schema:
                where_data["table_schema"] = table_schema
        elif engine == DbEngine.SQLSERVER:
            sel_stmt = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS"
            where_data["TABLE_NAME"] = table_name
            if table_schema:
                where_data["TABLE_SCHEMA"] = table_schema

        # execute the query
        # noinspection PyTypeChecker
        recs: list[tuple[str, str, str]] = db_select(sel_stmt=sel_stmt,
                                                     where_data=where_data,
                                                     engine=engine,
                                                     connection=connection,
                                                     committable=committable,
                                                     errors=errors,
                                                     logger=logger)
        # process the query result
        if not errors and recs:
            result = [(rec[0], rec[1], rec[2].startswith("Y")) for rec in recs]

    return result


def db_get_table_ddl(table_name: str,
                     engine: DbEngine = None,
                     connection: Any = None,
                     committable: bool = None,
                     errors: list[str] = None,
                     logger: Logger = None) -> str | None:
    """
    Retrieve the DDL script used to create the table *table_name*.

    Note that *table_name* must be schema-qualified, or else the invocation will fail.
    For *postgres* databases, make sure that the function *pg_get_tabledef* is installed and accessible.
    This function is freely available at https://github.com/MichaelDBA/pg_get_tabledef.

    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param table_name: the schema-qualified name of the table
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit operation on errorless completion
    :param errors: incidental error messages
    :param logger: optional logger
    :return: the DDL script used to create the index, or *None* if error or the index does not exist
    """
    # initialize the return variable
    result: str | None = None

    # assert the database engine
    if not isinstance(errors, list):
        errors = []
    engine = _assert_engine(engine=engine,
                            errors=errors)
    if not errors:
        # determine the table schema
        table_schema: str
        table_schema, table_name = table_name.split(sep=".") if "." in table_name else (None, table_name)

        if table_schema:
            # build the query
            sel_stmt: str | None = None
            if engine == DbEngine.MYSQL:
                pass
            if engine == DbEngine.ORACLE:
                sel_stmt = ("SELECT DBMS_METADATA.GET_DDL('TABLE', "
                            f"'{table_name.upper()}', '{table_schema.upper()}') "
                            "FROM DUAL")
            elif engine == DbEngine.POSTGRES:
                sel_stmt = ("SELECT * FROM public.pg_get_table_def("
                            f"'{table_schema.lower()}', '{table_name.lower()}', false)")
            elif engine == DbEngine.SQLSERVER:
                # sel_stmt = f"EXEC sp_help '{schema_name}.{table_name}'"
                sel_stmt = ("SELECT OBJECT_DEFINITION (OBJECT_ID("
                            f"'{table_schema.lower()}.{table_name.upper()}'))")

            # execute the query
            recs: list[tuple[str]] = db_select(sel_stmt=sel_stmt,
                                               engine=engine,
                                               connection=connection,
                                               committable=committable,
                                               errors=errors,
                                               logger=logger)
            # process the query result
            if not errors and recs:
                result = recs[0][0].strip()
        else:
            # 'table_name' not schema-qualified, report the problem
            errors.append(f"Table '{table_name}' not properly schema-qualified")

    return result
