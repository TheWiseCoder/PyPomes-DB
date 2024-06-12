from logging import Logger
from typing import Any

from .db_pomes import db_select
from .db_common import _assert_engine


def db_get_indexes(errors: list[str],
                   schema: str = None,
                   omit_pks: bool = True,
                   tables: list[str] = None,
                   engine: str = None,
                   connection: Any = None,
                   committable: bool = True,
                   logger: Logger = None) -> list[str]:
    """
    Retrieve and return the list of indexes in the database.

    If the list of table names *tables* is provided,
    only the indexes created on any of these tables' columns are returned.
    If *omit_pks* is set to 'True' (its default value),
    indexes created on primary key columns will not be included.

    :param errors: incidental error messages
    :param schema: optional name of the schema to restrict the search to
    :param omit_pks: omit indexes on primary key columns (defaults to 'True')
    :param tables: optional list of tables whose columns the indexes were created on
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion ('False' requires 'connection' to be provided)
    :param logger: optional logger
    :return: 'True' if the table was found, 'False' otherwise, 'None' if an error ocurred
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
        sel_stmt: str | None = None
        match curr_engine:
            case "mysql":
                pass
            case "oracle":
                sel_stmt: str = "SELECT ai.index_name FROM all_indexes ai "
                if omit_pks:
                    sel_stmt += ("INNER JOIN all_ind_columns aic ON ai.index_name = aic.index_name "
                                 "INNER JOIN all_cons_columns acc "
                                 "ON aic.table_name = acc.table_name AND aic.column_name = acc.column_name "
                                 "INNER JOIN all_constraints ac "
                                 "ON acc.constraint_name = ac.constraint_name AND ac.constraint_type != 'P' ")
                sel_stmt += "WHERE ai.dropped = 'NO' AND "
                if schema:
                    sel_stmt += f"owner = '{schema.upper()}' AND "
                if tables:
                    in_tables: str = "','".join(tables)
                    sel_stmt += f"table_name IN ('{in_tables.upper()}') AND "
                sel_stmt = sel_stmt[:-5]
            case "postgres":
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
                    if tables:
                        in_tables: str = "','".join(tables)
                        sel_stmt += f"LOWER(t.relname) IN ('{in_tables.lower()}') AND "
                    sel_stmt = sel_stmt[:-5]
            case "sqlserver":
                sel_stmt = ("SELECT i.name FROM sys.tables t "
                            "INNER JOIN sys.indexes i ON i.object_id = t.object_id")
                if omit_pks or schema or tables:
                    sel_stmt += " WHERE "
                    if omit_pks:
                        sel_stmt += "i.is_primary_key = 0 AND "
                    if schema:
                        sel_stmt += f"SCHEMA_NAME(t.schema_id) = '{schema.lower()}' AND "
                    if tables:
                        in_tables: str = "','".join(tables)
                        sel_stmt += f"LOWER(t.name) IN ('{in_tables.lower()}') AND "
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

    # acknowledge eventual local errors, if appropriate
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result