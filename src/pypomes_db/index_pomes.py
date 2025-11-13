from logging import Logger
from pypomes_core import str_positional
from typing import Any

from .db_common import DbEngine, _assert_engine
from .db_pomes import db_select


def db_get_indexes(schema: str = None,
                   omit_pks: bool = True,
                   tables: list[str] = None,
                   engine: DbEngine = None,
                   connection: Any = None,
                   committable: bool = None,
                   errors: list[str] = None,
                   logger: Logger = None) -> list[str]:
    """
    Retrieve the list of schema-qualified indexes in the database.

    If the list of possibly schema-qualified table names *tables* is provided,
    only the indexes created on any of these tables' columns are returned.
    If *omit_pks* is set to 'True' (its default value),
    indexes created on primary key columns will not be included.

    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param schema: optional name of the schema to restrict the search to
    :param omit_pks: omit indexes on primary key columns (defaults to 'True')
    :param tables: optional list of possibly schema-qualified tables whose columns the indexes were created on
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param errors: incidental error messages (might be a non-empty list)
    :param logger: optional logger
    :return: the list of schema-qualified indexes in the database
    """
    # initialize the return variable
    result: list[str] | None = None

    # assert the database engine
    engine = _assert_engine(engine=engine,
                            errors=errors)
    if engine:
        # process table names
        tbl_name = str_positional(engine,
                                  keys=tuple(DbEngine),
                                  values=("", "table_name", "LOWER(t.relname)", "LOWER(t.name)"))
        sch_name = str_positional(engine,
                                  keys=tuple(DbEngine),
                                  values=("", "aic.table_name",
                                          "LOWER(ns.nspname)", "SCHEMA_NAME(t.schema_id)"))
        in_tables: str = ""
        where_tables: str = ""
        for table in tables:
            # process the existing schema
            splits: list[str] = table.split(".")
            # is 'table' schema-qualified ?
            if len(splits) == 1:
                # no
                tbl_value: str = table.upper() if engine == DbEngine.ORACLE else table.lower()
                in_tables += f"'{tbl_value}',"
            else:
                # yes
                tbl_value: str = splits[1].upper() if engine == DbEngine.ORACLE else splits[1].lower()
                sch_value: str = splits[0].upper() if engine == DbEngine.ORACLE else splits[0].lower()
                where_tables += (f"({tbl_name} = '{tbl_value}' "
                                 f"AND {sch_name} = '{sch_value}') OR ")
        if in_tables:
            where_tables += f"{tbl_name} IN ({in_tables[:-1]})"
        else:
            where_tables = where_tables[:-4]

        # build the query
        sel_stmt: str | None = None
        match engine:
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
        recs: list[tuple[str]] = db_select(sel_stmt=sel_stmt,
                                           engine=engine,
                                           connection=connection,
                                           committable=committable,
                                           errors=errors,
                                           logger=logger)
        # process the query result
        if isinstance(recs, list):
            result = [rec[0] for rec in recs]

    return result


def db_get_index_ddl(index_name: str,
                     engine: DbEngine = None,
                     connection: Any = None,
                     committable: bool = None,
                     errors: list[str] = None,
                     logger: Logger = None) -> str | None:
    """
    Retrieve the DDL script used to create the index *index_name*.

    Note that *index_name* must be schema-qualified, or else the invocation will fail.

    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param index_name: the schema-qualified name of the index
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param errors: incidental error messages (might be a non-empty list)
    :param logger: optional logger
    :return: the DDL script used to create the index, or *None* if error or if the index does not exist
    """
    # initialize the return variable
    result: str | None = None

    # necessary, lest the state of 'errors' be tested
    curr_errors: list[str] = []

    # assert the database engine
    engine = _assert_engine(engine=engine,
                            errors=curr_errors)

    # is 'index_name' schema-qualified ?
    splits: list[str] = index_name.split(".")
    if len(splits) != 2:
        # no, report the problem
        curr_errors.append(f"Index '{index_name}' not properly schema-qualified")

    # proceed, if no errors
    if not curr_errors:
        # extract the schema and index names
        schema_name: str = splits[0]
        index_name: str = splits[1]

        # build the query
        sel_stmt: str | None = None
        if engine == DbEngine.MYSQL:
            pass
        if engine == DbEngine.ORACLE:
            sel_stmt = ("SELECT DBMS_METADATA.GET_DDL('INDEX', "
                        f"'{index_name.upper()}', '{schema_name.upper()}') "
                        "FROM DUAL")
        elif engine == DbEngine.POSTGRES:
            sel_stmt = ("SELECT pg_get_indexdef("
                        f"(quote_ident('{schema_name.lower()}') || '.' || "
                        f"quote_ident('{index_name.lower()}'))::regclass))")
        elif engine == DbEngine.SQLSERVER:
            sel_stmt = ("SELECT OBJECT_DEFINITION (OBJECT_ID("
                        f"'{schema_name.lower()}.{index_name.lower()}'))")

        # execute the query
        recs: list[tuple[str]] = db_select(sel_stmt=sel_stmt,
                                           engine=engine,
                                           connection=connection,
                                           committable=committable,
                                           errors=curr_errors,
                                           logger=logger)
        # process the query result
        if not curr_errors and recs:
            result = recs[0][0].strip()

    if curr_errors and isinstance(errors, list):
        errors.extend(curr_errors)

    return result
