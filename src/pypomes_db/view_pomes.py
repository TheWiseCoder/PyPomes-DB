from logging import Logger
from typing import Any, Literal

from .db_common import DbEngine, _assert_engine
from .db_pomes import db_execute, db_select


def db_get_views(errors: list[str] | None,
                 view_type: Literal["M", "P"] = "P",
                 schema: str = None,
                 tables: list[str] = None,
                 engine: DbEngine = None,
                 connection: Any = None,
                 committable: bool = None,
                 logger: Logger = None) -> list[str]:
    """
    Retrieve and return the list of views in the database.

    The returned view names will be qualified with the schema they belong to.
    If a list of, possibly schema-qualified, table names is provided in *tables*, then only
    the views whose table dependencies are all included therein are returned.

    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param view_type: the type of the view ("M": materialized, "P": plain, defaults to "P")
    :param schema: optional name of the schema to restrict the search to
    :param tables: optional list of, possibly schema-qualified, table names containing all views' dependencies
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: the schema-qualified views found, or 'None' if an error ocurred
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
        # build the query
        if curr_engine == DbEngine.ORACLE:
            vw_table: str = "all_mviews" if view_type == "M" else "all_views"
            vw_name: str = "mview_name" if view_type == "M" else "view_name"
            sel_stmt: str = f"SELECT owner || '.' || {vw_name} FROM {vw_table}"
            if schema:
                sel_stmt += f" WHERE owner = '{schema.upper()}'"
        elif view_type == "M":  # materialized views (postgres, sqlserver)
            if curr_engine == DbEngine.POSTGRES:
                sel_stmt = "SELECT schemaname || '.' || matviewname FROM pg_matviews "
                if schema:
                    sel_stmt += f" WHERE LOWER(schemaname) = '{schema.lower()}'"
            else:  # DbEngine.SQLSERVER
                sel_stmt = ("SELECT SCHEMA_NAME(v.schema_id) || '.' || table_name FROM sys.views v "
                            "INNER JOIN sys.indexes i ON i.object_id = v.object_id "
                            "WHERE i.index_id < 2")
                if schema:
                    sel_stmt += f" AND LOWER(SCHEMA_NAME(v.schema_id)) = '{schema.lower()}'"
        else:  # standard views (DbEngine.POSTGRES, DbEngine.SQLSERVER)
            sel_stmt: str = ("SELECT table_schema || '.' || table_name "
                             "FROM information_schema.views")
            if schema:
                sel_stmt += f" WHERE LOWER(table_schema) = '{schema.lower()}'"

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

    # omit views with dependencies not in 'tables', if applicable
    if result and tables:
        omitted_views: list[str] = []
        for view_name in result:
            op_errors = []
            dependencies: list[str] = \
                db_get_view_dependencies(errors=op_errors,
                                         view_name=view_name,
                                         engine=curr_engine,
                                         connection=connection,
                                         committable=committable,
                                         logger=logger) or []
            for dependency in dependencies:
                if dependency not in tables:
                    omitted_views.append(view_name)
                    break
            # acknowledge eventual local errors, if appropriate
            if isinstance(errors, list):
                errors.extend(op_errors)
        for omitted_view in omitted_views:
            result.remove(omitted_view)

    return result


def db_view_exists(errors: list[str] | None,
                   view_name: str,
                   view_type: Literal["M", "P"] = "P",
                   engine: DbEngine = None,
                   connection: Any = None,
                   committable: bool = None,
                   logger: Logger = None) -> bool:
    """
    Determine whether the view *view_name* exists in the database.

    If *view_name* is schema-qualified, then the search will be restricted to that schema.

    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param view_name: the, possibly schema-qualified, name of the view to look for
    :param view_type: the type of the view ("M": materialized, "P": plain, defaults to "P")
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: 'True' if the view was found, 'False' otherwise, 'None' if an error ocurred
    """
    # initialize the return variable
    result: bool | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: DbEngine = _assert_engine(errors=op_errors,
                                           engine=engine)
    # proceed, if no errors
    if not op_errors:
        # extract the schema, if possible
        schema_name: str | None = None
        splits: list[str] = view_name.split(".")
        if len(splits) > 1:
            schema_name = splits[0]
            view_name = splits[1]

        # build the query
        sel_stmt: str
        if curr_engine == DbEngine.ORACLE:
            vw_table: str = "all_mviews" if view_type == "M" else "all_views"
            sel_stmt = (f"SELECT COUNT(*) FROM {vw_table} "
                        f"WHERE view_name = '{view_name.upper()}'")
            if schema_name:
                sel_stmt += f" AND owner = '{schema_name.upper()}'"

        elif view_type == "M":  # materialized views (postgres, sqlserver)
            if curr_engine == DbEngine.POSTGRES:
                sel_stmt = ("SELECT COUNT(*) FROM pg_matview "
                            f"WHERE LOWER(matviewname) = '{view_name.lower()}'")
                if schema_name:
                    sel_stmt += f" AND LOWER(schemaname) = {schema_name.lower()}"
            else:  # DbEngine.SQLSERVER
                sel_stmt = ("SELECT COUNT(*) FROM sys.views v "
                            "INNER JOIN sys.indexes i ON i.object_id - v.object_id "
                            f"WHERE i.index_id < 2 AND LOWER(table_name) = {view_name.lower()}")
                if schema_name:
                    sel_stmt += f" AND LOWER(SCHEMA_NAME(v.schema_id)) = {schema_name.lower()}"

        else:  # standard views (DbEngine.POSTGRES, DbEngine.SQLSERVER)
            sel_stmt = ("SELECT COUNT(*) "
                        "FROM information_schema.views "
                        f"WHERE LOWER(table_name) = '{view_name.lower()}'")
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


def db_drop_view(errors: list[str] | None,
                 view_name: str,
                 view_type: Literal["M", "P"] = "P",
                 engine: DbEngine = None,
                 connection: Any = None,
                 committable: bool = None,
                 logger: Logger = None) -> None:
    """
    Drop the view given by the, possibly schema-qualified, *view_name*.

    This is a silent *DDL* operation. Whether commits or rollbacks are applicable,
    and what their use would entail, depends on the response of the *engine* to the
    mixing of *DDL* and *DML* statements in a transaction.

    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param view_name: the, possibly schema-qualified, name of the view to drop
    :param view_type: the type of the view ("M": materialized, "P": plain, defaults to "P")
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    """
    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: DbEngine = _assert_engine(errors=op_errors,
                                           engine=engine)
    # proceed, if no errors
    if not op_errors:
        # build the DROP statement
        tag: str = "MATERIALIZED VIEW" if view_type == "M" else "VIEW"
        if curr_engine == DbEngine.ORACLE:
            # oracle has no 'IF EXISTS' clause
            drop_stmt: str = \
                (f"BEGIN"
                 f" EXECUTE IMMEDIATE 'DROP {tag} {view_name}'; "
                 "EXCEPTION"
                 " WHEN OTHERS THEN NULL; "
                 "END;")
        elif curr_engine == DbEngine.POSTGRES:
            drop_stmt: str = \
                ("DO $$"
                 "BEGIN"
                 f" EXECUTE 'DROP {tag} IF EXISTS {view_name}'; "
                 "EXCEPTION"
                 " WHEN OTHERS THEN NULL; "
                 "END $$;")
        elif curr_engine == DbEngine.SQLSERVER:
            # in SQLServer, materialized views are regular views with indexes
            drop_stmt: str = \
                ("BEGIN TRY"
                 f" EXEC('DROP VIEW IF EXISTS {view_name};'); "
                 "END TRY "
                 "BEGIN CATCH "
                 "END CATCH;")
        else:  # DbEngine.MYSQL
            drop_stmt: str = f"DROP {tag} IF EXISTS {view_name}"

        # drop the view
        db_execute(errors=op_errors,
                   exc_stmt=drop_stmt,
                   engine=curr_engine,
                   connection=connection,
                   committable=committable,
                   logger=logger)

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)


def db_get_view_ddl(errors: list[str] | None,
                    view_name: str,
                    view_type: Literal["M", "P"] = "P",
                    engine: DbEngine = None,
                    connection: Any = None,
                    committable: bool = None,
                    logger: Logger = None) -> str:
    """
    Retrieve and return the DDL script used to create the view *view_name*.

    If *view_name* is schema-qualified, then the search will be pointed to the view in that schema.
    For Oracle databases, if the schema qualification is not provided, the search will fail.

    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param view_name: the name of the view
    :param view_type: the type of the view ("M": materialized, "P": plain, defaults to "P")
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: the DDL script used to create the view, or 'None' if the view does not exist, or an error ocurred
    """
    # initialize the return variable
    result: str | None = None

    # initialize the local errors list
    op_errors: list[str] = []

    # determine the database engine
    curr_engine: DbEngine = _assert_engine(errors=op_errors,
                                           engine=engine)
    # proceed, if no errors
    if not op_errors:
        # extract the schema, if possible
        schema_name: str = ""
        splits: list[str] = view_name.split(".")
        if len(splits) > 1:
            schema_name = splits[0]
            view_name = splits[1]

        # build the query
        sel_stmt: str | None = None
        if curr_engine == DbEngine.ORACLE:
            vw_type: str = "MATERIALIZED_VIEW" if view_type == "M" else "VIEW"
            vw_table: str = "all_mviews" if view_type == "M" else "all_views"
            vw_column: str = "mview_name" if view_type == "M" else "view_name"
            sel_stmt = (f"SELECT DBMS_METADATA.GET_DDL("
                        f"'{vw_type}', '{view_name.upper()}', '{schema_name.upper()}') "
                        "FROM dual WHERE EXISTS "
                        f"(SELECT NULL FROM {vw_table} "
                        f"WHERE {vw_column} = '{view_name.upper()}' "
                        f"AND owner = '{schema_name.upper()}')")

        elif view_type == "M":  # materialized view (postgres, sqlserver)
            if curr_engine == DbEngine.POSTGRES:
                sel_stmt = ("SELECT definition FROM pg_matviews "
                            f"WHERE matviewname = '{view_name.lower()}'")
                if schema_name:
                    sel_stmt += f" AND schemaname = {schema_name.lower()}"
            elif curr_engine == DbEngine.SQLSERVER:
                sel_stmt = ("SELECT view_definition "
                            "FROM information_schema.views AS v "
                            "INNER JOIN sys.indexes AS i ON OBJECT_NAME(i.object_id) = v.table_name "
                            f"WHERE i.index_id < 2 AND LOWER(v.table_name) = '{view_name.lower()}'")
                if schema_name:
                    sel_stmt += f" AND v.table_schema = '{schema_name.lower()}'"

        elif curr_engine in [DbEngine.POSTGRES, DbEngine.SQLSERVER]:
            sel_stmt = ("SELECT view_definition "
                        "FROM information_schema.views "
                        f"WHERE LOWER(table_name) = '{view_name.lower()}'")
            if schema_name:
                sel_stmt += f" AND LOWER(table_schema) = '{schema_name.lower()}'"

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


def db_get_view_dependencies(errors: list[str] | None,
                             view_name: str,
                             view_type: Literal["M", "P"] = "P",
                             engine: DbEngine = None,
                             connection: Any = None,
                             committable: bool = None,
                             logger: Logger = None) -> list[str]:
    """
    Retrieve and return the schema-qualified names of the tables *view_name* depends on.

    If *view_name* is schema-qualified, then the search will be pointed to the view in that schema.

    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param view_name: the name of the view
    :param view_type: the type of the view ("M": materialized, 'P': plain, defaults to 'P')
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param logger: optional logger
    :return: the schema-qualified tables the view depends on, or 'None' if view not found or an error ocurred
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
        # extract the schema, if possible
        schema_name: str | None = None
        splits: list[str] = view_name.split(".")
        if len(splits) > 1:
            schema_name = splits[0]
            view_name = splits[1]

        # build the query
        sel_stmt: str | None = None
        match engine:
            case DbEngine.MYSQL:
                pass
            case DbEngine.ORACLE:
                vw_type: str = "MATERIALIZED VIEW" if view_type == "M" else "VIEW"
                sel_stmt = ("SELECT DISTINCT referenced_owner || '.' || referenced_name "
                            "FROM all_dependencies "
                            f"WHERE name = '{view_name.upper()}'"
                            f"AND type = '{vw_type}' AND referenced_type = 'TABLE'")
                if schema_name:
                    sel_stmt += f" AND owner = '{schema_name.upper()}'"
            case DbEngine.POSTGRES:
                sel_stmt = ("SELECT DISTINCT nsp.nspname || '.' || cl1.relname "
                            "FROM pg_class AS cl1 "
                            "INNER JOIN pg_namespace AS nsp ON nsp.oid = cl1.relnamespace "
                            "INNER JOIN pg_rewrite AS rw ON rw.ev_class = cl1.oid "
                            "INNER JOIN pg_depend AS d ON d.objid = rw.oid "
                            "INNER JOIN pg_class AS cl2 ON cl2.oid = d.refobjid "
                            f"WHERE LOWER(cl2.relname) = '{view_name.lower()}' AND cl2.relkind = ")
                sel_stmt += "'m'" if view_type == "M" else "'v'"
                if schema_name:
                    sel_stmt += (" AND cl2.relnamespace = "
                                 f"(SELECT oid FROM pg_namespace "
                                 f"WHERE LOWER(nspname) = '{schema_name.lower()}')")
            case DbEngine.SQLSERVER:
                entity: str = view_name.lower()
                if schema_name:
                    entity = schema_name.lower() + "." + entity
                sel_stmt = ("SELECT DISTINCT s.name || '.' || re.referencing_entity_name "
                            f"FROM sys.dm_sql_referencing_entities ('{entity}', 'OBJECT') AS re "
                            "INNER JOIN sys.objects AS o ON o.object_id = re.referencing_id "
                            "INNER JOIN sys.schemas AS s ON s.schema_id = o.schema_id")
                if schema_name:
                    sel_stmt += f" WHERE LOWER(referencing_schema_name) = '{schema_name.lower()}'"

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
