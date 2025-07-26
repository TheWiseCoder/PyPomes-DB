from logging import Logger
from pypomes_core import str_positional
from typing import Any

from .db_common import DbEngine, _assert_engine
from .db_pomes import db_select


def db_get_indexes(errors: list[str] | None,
                   schema: str = None,
                   omit_pks: bool = True,
                   tables: list[str] = None,
                   engine: DbEngine = None,
                   connection: Any = None,
                   committable: bool = None,
                   logger: Logger = None) -> list[str]:
    """
    Retrieve the list of schema-qualified indexes in the database.

    If the list of possibly schema-qualified table names *tables* is provided,
    only the indexes created on any of these tables' columns are returned.
    If *omit_pks* is set to 'True' (its default value),
    indexes created on primary key columns will not be included.

    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param schema: optional name of the schema to restrict the search to
    :param omit_pks: omit indexes on primary key columns (defaults to 'True')
    :param tables: optional list of possibly schema-qualified tables whose columns the indexes were created on
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: the list of schema-qualified indexes in the database
    """
    # initialize the return variable
    result: list[str] | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: DbEngine = _assert_engine(errors=op_errors,
                                           engine=engine)
    # proceed, if no errors
    if not op_errors:
        # process table names
        tbl_name = str_positional(source=curr_engine,
                                  list_from=["oracle", "postgres", "sqlserver"],
                                  list_to=["table_name", "LOWER(t.relname)", "LOWER(t.name)"])
        sch_name = str_positional(source=curr_engine,
                                  list_from=["oracle", "postgres", "sqlserver"],
                                  list_to=["aic.table_name",
                                           "LOWER(ns.nspname)", "SCHEMA_NAME(t.schema_id)"])
        in_tables: str = ""
        where_tables: str = ""
        for table in tables:
            # process the existing schema
            splits: list[str] = table.split(".")
            # is 'table' schema-qualified ?
            if len(splits) == 1:
                # no
                tbl_value: str = table.upper() if curr_engine == DbEngine.ORACLE else table.lower()
                in_tables += f"'{tbl_value}',"
            else:
                # yes
                tbl_value: str = splits[1].upper() if curr_engine == DbEngine.ORACLE else splits[1].lower()
                sch_value: str = splits[0].upper() if curr_engine == DbEngine.ORACLE else splits[0].lower()
                where_tables += (f"({tbl_name} = '{tbl_value}' "
                                 f"AND {sch_name} = '{sch_value}') OR ")
        if in_tables:
            where_tables += f"{tbl_name} IN ({in_tables[:-1]})"
        else:
            where_tables = where_tables[:-4]

        # build the query
        sel_stmt: str | None = None
        match curr_engine:
            case DbEngine.MYSQL:
                pass
            case DbEngine.ORACLE:
                sel_stmt: str = "SELECT ai.index_name FROM all_indexes ai "
                if omit_pks:
                    sel_stmt += ("INNER JOIN all_ind_columns aic ON ai.index_name = aic.index_name "
                                 "INNER JOIN all_cons_columns acc "
                                 "ON aic.table_name = acc.table_name AND aic.column_name = acc.column_name "
                                 "INNER JOIN all_constraints ac "
                                 "ON acc.constraint_name = ac.constraint_name AND ac.constraint_type != 'P' ")
                sel_stmt += "WHERE ai.dropped = 'NO' AND "
                if schema:
                    sel_stmt += f"ai.owner = '{schema.upper()}' AND "
                if where_tables:
                    sel_stmt += f"({where_tables}) AND "
                sel_stmt = sel_stmt[:-5]
            case DbEngine.POSTGRES:
                sel_stmt: str = ("SELECT i.relname FROM pg_class t "
                                 "INNER JOIN pg_namespace ns ON ns.oid = t.relnamespace "
                                 "INNER JOIN pg_index ix ON ix.indrelid = t.oid "
                                 "INNER JOIN pg_class i ON i.oid = ix.indexrelid ")
                if omit_pks or schema or tables:
                    sel_stmt += " WHERE "
                    if omit_pks:
                        sel_stmt += "ix.indisprimary = false AND "
                    if schema:
                        sel_stmt += f"LOWER(ns.nspname) = '{schema.lower()}' AND "
                if where_tables:
                    sel_stmt += f"({where_tables}) AND "
                sel_stmt = sel_stmt[:-5]
            case DbEngine.SQLSERVER:
                sel_stmt = ("SELECT i.name FROM sys.tables t "
                            "INNER JOIN sys.indexes i ON i.object_id = t.object_id")
                if omit_pks or schema or where_tables:
                    sel_stmt += " WHERE "
                    if omit_pks:
                        sel_stmt += "i.is_primary_key = 0 AND "
                    if schema:
                        sel_stmt += f"SCHEMA_NAME(t.schema_id) = '{schema.lower()}' AND "
                    if where_tables:
                        sel_stmt += f"({where_tables}) AND "
                        sel_stmt = sel_stmt[:-5]

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

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_get_index_ddl(errors: list[str] | None,
                     index_name: str,
                     engine: DbEngine = None,
                     connection: Any = None,
                     committable: bool = None,
                     logger: Logger = None) -> str | None:
    """
    Retrieve the DDL script used to create the index *index_name*.

    Note that *index_name* must be schema-qualified, or else the invocation will fail.

    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param index_name: the schema-qualified name of the index
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: the DDL script used to create the index, or *None* if error or if the index does not exist
    """
    # initialize the return variable
    result: str | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: DbEngine = _assert_engine(errors=op_errors,
                                           engine=engine)

    # is 'index_name' schema-qualified ?
    splits: list[str] = index_name.split(".")
    if len(splits) != 2:
        # no, report the problem
        op_errors.append(f"Index name '{index_name}' not properly schema-qualified")

    # proceed, if no errors
    if not op_errors:
        # extract the schema and index names
        schema_name: str = splits[0]
        index_name: str = splits[1]

        # build the query
        sel_stmt: str | None = None
        if curr_engine == DbEngine.MYSQL:
            pass
        if curr_engine == DbEngine.ORACLE:
            sel_stmt = ("SELECT DBMS_METADATA.GET_DDL('INDEX', "
                        f"'{index_name.upper()}', '{schema_name.upper()}') "
                        "FROM DUAL")
        elif curr_engine == DbEngine.POSTGRES:
            sel_stmt = ("SELECT pg_get_indexdef("
                        f"(quote_ident('{schema_name.lower()}') || '.' || "
                        f"quote_ident('{index_name.lower()}'))::regclass))")
        elif curr_engine == DbEngine.SQLSERVER:
            sel_stmt = ("SELECT OBJECT_DEFINITION (OBJECT_ID("
                        f"'{schema_name.lower()}.{index_name.lower()}'))")

        # execute the query
        recs: list[tuple[str]] = db_select(errors=op_errors,
                                           sel_stmt=sel_stmt,
                                           engine=curr_engine,
                                           connection=connection,
                                           committable=committable,
                                           logger=logger)
        # process the query result
        if not op_errors and recs:
            result = recs[0][0].strip()

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result
