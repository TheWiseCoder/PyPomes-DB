from pypomes_core import str_between
from typing import Any

from ..db_common import (
    DbEngine, DbParam, _assert_engine, _get_param
)
from ..db_pomes import db_execute, db_select


def db_create_table(table_name: str,
                    column_data: list[str] | list[tuple[str, str, int, int, int, bool, str]],
                    constraints: list[str] = None,
                    engine: DbEngine | str = None,
                    connection: Any = None,
                    errors: list[str] = None) -> None:
    """
    Create the table *table_name*, with the list of columns specifications in *column_data*.

    The data in *column_data* may be:
        - a list of SQL-compliant column creation clauses, or
        - a list of column metadata, as returned by *get_columns_metadata()*.
    The optional *constraints* must comply with the appropriate SQL syntax rules.

    :param table_name: the, possibly schema-qualified, name of the table to be created
    :param column_data: the list of column information for the table creation
    :param constraints: optional list of constraints
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param errors: incidental error messages (may be a non-empty list)
    """
    # build the table creation statement
    stmt: str = f"CREATE TABLE {table_name} ("
    for col_data in column_data:
        if isinstance(col_data, str):
            stmt += col_data
        else:
            # noinspection PyTypeChecker
            stmt += db_build_column_clause(col_name=col_data[0],
                                           col_metadata=col_data[1:])
        stmt += ", "
    if constraints:
        stmt += f"CONSTRAINT {', CONSTRAINT '.join(constraints)}"
    else:
        stmt = stmt[:-2]
    stmt += ")"

    # execute the table creation statement
    db_execute(exc_stmt=stmt,
               engine=engine,
               connection=connection,
               errors=errors)


def db_create_session_table(table_name: str,
                            column_data: list[str],
                            engine: DbEngine | str,
                            connection: Any,
                            errors: list[str] = None) -> None:
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

    :param table_name: the, possibly schema-qualified, name of the table to be created
    :param column_data: this list of column information for the table creation
    :param engine: the reference database engine (uses the default engine, if not provided)
    :param connection: the connection defining the session
    :param errors: incidental error messages (may be a non-empty list)
    """
    # necessary, lest the state of 'errors' be tested
    curr_errors: list[str] = []

    # assert the database engine
    engine_id, engine_type = _assert_engine(engine,
                                            errors=curr_errors)
    if engine_id:
        stmt: str | None = None
        match engine_type:
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
                version: str = _get_param(engine_id=engine_id,
                                          param=DbParam.VERSION)
                if version and version > "18" and table_name.upper().startswith("ORA$PTT_"):
                    # 'ON COMMIT PRESERVE DEFINITION' ensures the table lasts for the session
                    stmt = (f"CREATE PRIVATE TEMPORARY TABLE {table_name.upper()} "
                            f"({', '.join(column_data)}) ON COMMIT PRESERVE DEFINITION")
                elif not db_table_exists(table_name=table_name,
                                         engine=engine,
                                         errors=curr_errors) and not curr_errors:
                    # - the table definition remains in the database permanently, but the data is session-specific
                    # - 'ON COMMIT PRESERVE ROWS' keeps data for the entire session
                    stmt = (f"CREATE GLOBAL TEMPORARY TABLE {table_name} "
                            f"({'. '.join(column_data)}) ON COMMIT PRESERVE ROWS")
            case DbEngine.SQLSERVER:
                # 'table_name' must be prepended with '#', which creates a local, session-scoped, temporary table
                stmt = f"CREATE TABLE {table_name} ({', '.join(column_data)})"

        if stmt:
            db_execute(exc_stmt=stmt,
                       engine=engine_id,
                       connection=connection,
                       errors=curr_errors)

    if curr_errors and isinstance(errors, list):
        errors.extend(curr_errors)


def db_get_session_table_prefix(engine: DbEngine | str) -> str:
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
    engine_id, engine_type = _assert_engine(engine)

    if engine_type == DbEngine.ORACLE:
        version: str = _get_param(engine_id=engine_id,
                                  param=DbParam.VERSION)
        if version and version > "18":
            result = "ORA$PTT_"
    elif engine_type == DbEngine.SQLSERVER:
        result = "#"

    return result


def db_get_tables(schema: str = None,
                  engine: DbEngine | str = None,
                  connection: Any = None,
                  errors: list[str] = None) -> list[str] | None:
    """
    Retrieve the list of schema-qualified tables in the database.

    :param schema: optional name of the schema to restrict the search to
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param errors: incidental error messages (may be a non-empty list)
    :return: the schema-qualified table names found, or *None* if error
    """
    # initialize the return variable
    result: list[str] | None = None

    # assert the database engine
    engine_id, engine_type = _assert_engine(engine,
                                            errors=errors)
    if engine_id:
        # build the query
        if engine_type == DbEngine.ORACLE:
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
                                           engine=engine_id,
                                           connection=connection,
                                           errors=errors)
        # process the query result
        if isinstance(recs, list):
            result = [rec[0] for rec in recs]

    return result


def db_table_exists(table_name: str,
                    engine: DbEngine | str = None,
                    connection: Any = None,
                    errors: list[str] = None) -> bool | None:
    """
    Determine whether the table *table_name* exists in the database.

    If *table_name* is schema-qualified, then the search will be restricted to that schema.

    :param table_name: the, possibly schema-qualified, name of the table to look for
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param errors: incidental error messages (may be a non-empty list)
    :return: *True* if the table was found, *False* otherwise, or *None* if error
    """
    # initialize the return variable
    result: bool | None = None

    # assert the database engine
    engine_id, engine_type = _assert_engine(engine,
                                            errors=errors)
    if engine_id:
        # determine the table schema
        table_schema: str
        table_schema, table_name = table_name.split(sep=".") if "." in table_name else (None, table_name)

        # build the query
        if engine_type == DbEngine.ORACLE:
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
                                           engine=engine_id,
                                           connection=connection,
                                           errors=errors)
        # process the query result
        if isinstance(recs, list):
            result = recs[0][0] > 0

    return result


def db_drop_table(table_name: str,
                  engine: DbEngine | str = None,
                  connection: Any = None,
                  errors: list[str] = None) -> None:
    """
    Drop the table given by the, possibly schema-qualified, *table_name*.

    This is a silent *DDL* operation. Whether commits or rollbacks are applicable,
    and what their use would entail, depends on the response of the *engine* to the
    mixing of *DDL* and *DML* statements in a transaction.

    :param table_name: the, possibly schema-qualified, name of the table to drop
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param errors: incidental error messages (may be a non-empty list)
    """
    # assert the database engine
    engine_id, engine_type = _assert_engine(engine,
                                            errors=errors)
    if engine_id:
        # build the DROP statement
        if engine_type == DbEngine.ORACLE:
            # oracle has no 'IF EXISTS' clause
            drop_stmt: str = \
                (f"BEGIN"
                 f" EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS'; "
                 "EXCEPTION"
                 " WHEN OTHERS THEN NULL; "
                 "END;")
        elif engine_type == DbEngine.POSTGRES:
            drop_stmt: str = \
                ("DO $$"
                 "BEGIN"
                 f" EXECUTE 'DROP TABLE IF EXISTS {table_name} CASCADE'; "
                 "EXCEPTION"
                 " WHEN OTHERS THEN NULL; "
                 "END $$;")
        elif engine_type == DbEngine.SQLSERVER:
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
                   engine=engine_id,
                   connection=connection,
                   errors=errors)


def db_get_table_ddl(table_name: str,
                     engine: DbEngine | str = None,
                     connection: Any = None,
                     errors: list[str] = None) -> str | None:
    """
    Retrieve the DDL script used to create the table *table_name*.

    Note that *table_name* must be schema-qualified, or else the invocation will fail.
    For *postgres* databases, make sure that the function *pg_get_tabledef* is installed and accessible.
    This function is freely available at https://github.com/MichaelDBA/pg_get_tabledef.

    :param table_name: the schema-qualified name of the table
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param errors: incidental error messages (may be a non-empty list)
    :return: the DDL script used to create the index, or *None* if error or the table does not exist
    """
    # initialize the return variable
    result: str | None = None

    # assert the database engine
    engine_id, engine_type = _assert_engine(engine,
                                            errors=errors)
    if engine_id:
        # determine the table schema
        table_schema: str
        table_schema, table_name = table_name.split(sep=".") if "." in table_name else (None, table_name)

        if table_schema:
            # build the query
            sel_stmt: str | None = None
            match engine_type:
                case DbEngine.MYSQL:
                    pass
                case DbEngine.ORACLE:
                    sel_stmt = ("SELECT DBMS_METADATA.GET_DDL('TABLE', "
                                f"'{table_name.upper()}', '{table_schema.upper()}') "
                                "FROM DUAL")
                case DbEngine.POSTGRES:
                    sel_stmt = ("SELECT * FROM public.pg_get_table_def("
                                f"'{table_schema.lower()}', '{table_name.lower()}', false)")
                case DbEngine.SQLSERVER:
                    # sel_stmt = f"EXEC sp_help '{schema_name}.{table_name}'"
                    sel_stmt = ("SELECT OBJECT_DEFINITION (OBJECT_ID("
                                f"'{table_schema.lower()}.{table_name.upper()}'))")

            # execute the query
            recs: list[tuple[str]] = db_select(sel_stmt=sel_stmt,
                                               engine=engine_id,
                                               connection=connection,
                                               errors=errors)
            # process the query result
            if isinstance(recs, list) and recs:
                result = recs[0][0].strip()
        else:
            # 'table_name' not schema-qualified, report the problem
            errors.append(f"Table '{table_name}' not properly schema-qualified")

    return result


def db_get_table_constraints(table_name: str,
                             engine: DbEngine | str = None,
                             connection: Any = None,
                             errors: list[str] = None) -> list[tuple[str, str, str]] | None:
    """
    Retrieve and return all constraints registered in *engine* for table *table_name*.

    The constraint data is returned as a list of tuples, each tuple holding the following information:
        - constraint name - the name of the constraint
        - constraint type - the type of the constraint:
            - C: check
            - F: foreign key
            - N: not null
            - P: primary key
            - U: unique
            - X: exclusion
        - constraint definition:
            - C: CHECK (<check-expression>)
            - F: FOREIGN KEY (<col>) REFERENCES <table>(<col>)
            - N: NOT NULL <col>
            - P: PRIMARY KEY (<col1>, ..., <coln>)
            - U: UNIQUE (<col1>, ..., <coln>)
            - X: EXCLUDE <exclUsion-expression>

    :param table_name: the, possibly qualified, table name
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param errors: incidental error messages (may be a non-empty list)
    :return: the constraint data, *[]* if no constraints were fouund, or *None* if error
    """
    # initialize the return variable
    result: list[tuple[str, str, str]] | None = None

    # assert the database engine
    engine_id, engine_type = _assert_engine(engine,
                                            errors=errors)
    if engine_id:
        # determine the table schema
        table_schema: str
        table_schema, table_name = table_name.split(sep=".") if "." in table_name else (None, table_name)

        # build the query
        stmt: str | None = None
        match engine_type:
            case DbEngine.MYSQL:
                pass
            case DbEngine.ORACLE:
                stmt = ("SELECT c.constraint_name, c.constraint_type,"
                        "LISTAGG(cc.column_name, ', ') WITHIN GROUP (ORDER BY cc.position),"
                        "rc.table_name, " 
                        "LISTAGG(rcc.column_name, ', ') WITHIN GROUP (ORDER BY rcc.position) "
                        "FROM all_constraints c "
                        "LEFT JOIN all_cons_columns cc "
                        "ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name "
                        "LEFT JOIN all_constraints rc "
                        "ON rc.owner = c.r_owner AND rc.constraint_name = c.r_constraint_name "
                        "LEFT JOIN all_cons_columns rcc "
                        "ON rcc.owner = rc.owner AND rcc.constraint_name = rc.constraint_name "
                        "AND rcc.position = cc.position "
                        f"WHERE c.owner = '{table_schema.upper()}' AND c.table_name = '{table_name.upper()}' "
                        "AND c.constraint_type IN ('P','U','R') "
                        "GROUP BY c.constraint_name, c.constraint_type, rc.table_name "
                        "ORDER BY c.constraint_name")
            case DbEngine.POSTGRES:
                stmt = ("SELECT con.conname, upper(con.contype), pg_get_constraintdef(con.oid) "
                        "FROM pg_constraint con "
                        "JOIN pg_class rel ON rel.oid = con.conrelid "
                        "JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace "
                        f"WHERE rel.relname = '{table_name.lower()}' AND nsp.nspname = '{table_schema.lower()}' "
                        " ORDER BY con.conname")
            case DbEngine.SQLSERVER:
                pass

        # execute the query
        recs: list[tuple] = db_select(sel_stmt=stmt,
                                      engine=engine_id,
                                      connection=connection,
                                      errors=errors)
        if isinstance(recs, list):
            result = []
            # organize the Oracle output
            if engine_type == DbEngine.ORACLE:
                for rec in recs:
                    c_type: str = "F" if rec[1] == "R" else rec[1]
                    if c_type == "P":
                        c_def: str = f"PRIMARY KEY ({rec[2].lower()})"
                    elif c_type == "U":
                        c_def: str = f"UNIQUE ({rec[2].lower()})"
                    else:  # c_type == "F"
                        c_def: str = (f"FOREIGN KEY ({rec[2].lower()}) "
                                      f"REFERENCES {rec[3].lower()}({rec[4].lower()})")
                    result.append((rec[0], c_type, c_def))

                # obtain the CHECK constraints
                # (driver converts 'ac.search_condition' from Oracle 'LONG' to Python 'str')
                stmt = ("SELECT ac.constraint_name, ac.constraint_type, ac.search_condition "
                        "FROM all_constraints ac "
                        f"WHERE ac.constraint_type = 'C' AND "
                        f"ac.owner = '{table_schema.upper()}' AND ac.table_name = '{table_name.upper()}'")
                recs = db_select(sel_stmt=stmt,
                                 engine=engine_id,
                                 connection=connection,
                                 errors=errors)
                for rec in recs or []:
                    if " NOT NULL " in rec[2]:
                        c_type: str = "N"
                        c_def: str = str_between(rec[2],
                                                 from_str='"',
                                                 to_str='"').lower()
                    else:
                        c_type: str = "C"
                        c_def: str = rec[2].lower()
                    result.append((rec[0], c_type, c_def))
            else:
                # noinspection PyTypeChecker
                result = recs
    return result


def db_get_table_pk(table_name: str,
                    engine: DbEngine | str = None,
                    connection: Any = None,
                    errors: list[str] = None) -> tuple[str, str] | None:
    """
    Retrieve and return the ordered list of columns constituting *table_name*'s primary key.

    The return list will contain the name of the PK constraint, and one or more comma-separated,
    ordered column names.

    :param table_name: the, possibly qualified, table name
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param errors: incidental error messages (may be a non-empty list)
    :return: the PK constraint name and the ordered columns, *()* if PK was not found, or *None* if error
    """
    # initialize the return variable
    result: tuple[str, str] | None = None

    # assert the database engine
    engine_id, engine_type = _assert_engine(engine,
                                            errors=errors)
    if engine_id:
        # determine the table schema
        table_schema: str
        table_schema, table_name = table_name.split(sep=".") if "." in table_name else (None, table_name)

        # build the query
        stmt: str | None = None
        match engine_type:
            case DbEngine.MYSQL:
                pass
            case DbEngine.ORACLE:
                stmt = ("SELECT ac.constraint_name, "
                        "LISTAGG(acc.column_name, ',') WITHIN GROUP (ORDER BY acc.position) "
                        "FROM all_constraints ac "
                        "JOIN all_cons_columns acc "
                        "ON ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name "
                        "WHERE ac.constraint_type = 'P' "
                        f"AND ac.owner = '{table_schema.upper()}' AND ac.table_name = '{table_name.upper()}' "
                        "GROUP BY ac.constraint_name")
            case DbEngine.POSTGRES:
                stmt = ("SELECT con.conname, string_agg(att.attname, ',' ORDER BY u.ordinality) "
                        "FROM pg_constraint con "
                        "JOIN pg_class tbl ON tbl.oid = con.conrelid "
                        "JOIN pg_namespace nsp ON nsp.oid = tbl.relnamespace "
                        "JOIN unnest(con.conkey) WITH ORDINALITY AS u(attnum, ordinality) ON TRUE "
                        "JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = u.attnum "
                        "WHERE con.contype = 'p' "
                        f"AND nsp.nspname = '{table_schema.lower()}' AND tbl.relname = '{table_name.lower()}' "
                        "GROUP BY con.conname")
            case DbEngine.SQLSERVER:
                stmt = ("SELECT kc.name AS constraint_name, "
                        "STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) "
                        "FROM sys.key_constraints kc "
                        "JOIN sys.tables t ON kc.parent_object_id = t.object_id "
                        "JOIN sys.schemas s ON t.schema_id = s.schema_id "
                        "JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id "
                        "AND kc.unique_index_id = ic.index_id "
                        "JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id "
                        "WHERE kc.type = 'PK' "
                        f"AND s.name = '{table_schema.upper()}' AND t.name = '{table_name.upper()}' "
                        "GROUP BY kc.name")

        # execute the query
        # noinspection PyTypeChecker
        recs: list[tuple[str, str]] = db_select(sel_stmt=stmt,
                                                engine=engine_id,
                                                connection=connection,
                                                errors=errors)
        if isinstance(recs, list):
            result = recs[0] if len(recs) > 0 else ()

    return result


def db_build_column_clause(col_name: str,
                           col_metadata: tuple[str, int, int, int, bool, str]) -> str:
    """
    Given *col_metadata*, build the column creation clause for a SQL 'CREATE TABLE' DDL statement.

    The contents of *col_metadata* are described as the data returned by *db_get_column_metadata()*.
    The expected column types are listed as *udt_types* in *db_get_column_metadata()*.

    :param col_name: name of the reference column
    :param col_metadata: tuple containing the column metadata
    :return: the column creation clause
    """
    # initialize the return variable
    result: str = col_name + " " + col_metadata[0]

    col_type: str = col_metadata[0].lower()
    match col_type:
        case "char" | "varchar" | "varchar2" | "bpchar":
            result += f"({col_metadata[1]})"
        case "number" | "numeric":
            result += f"({col_metadata[2]}, {col_metadata[3]})"

    if col_metadata[5]:
        result += f" DEFAULT {col_metadata[5]}"

    if col_metadata[4]:
        result += " NULL"
    else:
        result += " NOT NULL"

    return result


def db_get_column_metadata(table_name: str,
                           column_name: str,
                           engine: DbEngine | str = None,
                           connection: Any = None,
                           errors: list[str] = None) -> tuple[str, int, int, int, bool, str] | None:
    """
    Retrieve metadata on column *column_name* in *table_name*.

    The metadata is returned as a tuple containing:
        - data type: the *udt_name* (name of the data type used for the column)
        - data length:
            - char: number of characters
            - varchar: maximum number of characters
            - all other types: number of bytes or NULL
        - precision:
            - numeric types: total number of digits
            - integer types: size in bytes
            - float types: number of bits used to store the mantissa (significant digits) - 24 (float4) or 53 (float8)
            - timestamp types: NULL
        - scale:
            - numeric types: number of decimal digits
            - integer types: 0
            - all other types: NULL
        - nullability: whether the column can hold NULL values
        - default value: the column default value, or NULL if not applicable

    These are the standard *udt_names* for all database engines but Oracle:
        - numbers (integers and decimals):
            - int2: 2-byte integer (SMALLINT)
            - int4: 4-byte integer (INTEGER)
            - int8: 8-byte integer (BIGINT)
            - float4: floating point (REAL)
            - float8: double precision floating point (DOUBLE PRECISION)
            - number: decimal number with precision and scale (NUMBER / NUMERIC)
        - characters and text:
            - varchar: variable-length string, with limit (VARCHAR)
            - bpchar: blank-padded, fixed-length string (CHAR)
            - text: unlimited-length string (TEXT)
        - date and time:
            - date: date, only (DATE)
            - time: time, no time zone (TIME)
            - timetz: time, with time zone (TIME WITH TIME ZONE)
            - timestamp: date and time, no time zone (TIMESTAMP)
            - timestamptz: date and time, with time zone (TIMESTAMP WITH TIME ZONE)
            - interval: time interval (INTERVAL)
        - logical and binary:
            - bool: True/False value (BOOLEAN)
            - bytea: binary data/byte array (BYTEA / BLOB)
        - identifiers and structured data:
            - json: string in JSON format (JSON)
            - jsonb: binary JSON format (JSONB)
            - uuid: Universal Unique Identifier (UUID)
            - xml: data in XML format (XML)
        - vectors (arrays):
            - _int4: array of integers (INTEGER[])
            - _varchar: array of strings (VARCHAR[])
            - _text: array of text (TEXT[])

    :param table_name: the, possibly schema-qualified, name of the table
    :param column_name: the name of the column to retrieve
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param errors: incidental error messages (may be a non-empty list)
    :return: the metadata on column *column_name* in *table_name*, or *None* if error or metadata not found
    """
    # initialize the return variable
    result: tuple[str, int, int, int, bool, str] | None = None

    # assert the database engine
    engine_id, engine_type = _assert_engine(engine,
                                            errors=errors)
    if engine_id:
        # determine the table schema
        table_schema: str
        table_schema, table_name = table_name.split(sep=".") if "." in table_name else (None, table_name)

        # build the query
        sel_stmt: str | None = None
        where_data: dict[str, str] | None = None
        match engine_type:
            case DbEngine.MYSQL:
                pass
            case DbEngine.ORACLE:
                sel_stmt = ("SELECT DATA_TYPE, DATA_LENGTH, "
                            "DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT  FROM ALL_TAB_COLUMNS")
                where_data = {
                    "TABLE_NAME": table_name.upper(),
                    "COLUMN_NAME": column_name.upper()
                }
                if table_schema:
                    where_data["OWNER"] = table_schema.upper()
            case DbEngine.POSTGRES:
                sel_stmt = ("SELECT udt_name, character_maximum_length, numeric_precision, "
                            "numeric_scale, is_nullable, column_default FROM information_schema.columns")
                where_data = {
                    "table_name": table_name.lower(),
                    "column_name": column_name.lower()
                }
                if table_schema:
                    where_data["table_schema"] = table_schema.lower()
            case DbEngine.SQLSERVER:
                sel_stmt = ("SELECT UDT_NAME, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, "
                            "NUMERIC_SCALE, IS_NULLABLE, COLUMN_DEFAULT FROM information_schema.columns")
                where_data = {
                    "TABLE_NAME": table_name.upper(),
                    "COLUMN_NAME": column_name.upper()
                }
                if table_schema:
                    where_data["TABLE_SCHEMA"] = table_schema.upper()

        # execute the query
        recs: list[tuple] = db_select(sel_stmt=sel_stmt,
                                      where_data=where_data,
                                      engine=engine_id,
                                      connection=connection,
                                      errors=errors)
        # process the query result
        if isinstance(recs, list) and recs:
            rec: tuple = recs[0]
            # BUG HANDLING: Oracle may report 'NUMBER(None, None)', NUMBER(None, <s>)' or 'NUMBER(<p>, None)'
            force_38: bool = engine_type == DbEngine.ORACLE and rec[0] == "NUMBER" and rec[2] is None
            force_0: bool = engine_type == DbEngine.ORACLE and rec[0] == "NUMBER" and rec[3] is None
            result = (rec[0], rec[1],
                      38 if force_38 else rec[2],
                      0 if force_0 else rec[3],
                      rec[4].startswith("Y"), rec[5])

    return result


def db_get_columns_metadata(table_name: str,
                            engine: DbEngine | str = None,
                            connection: Any = None,
                            errors: list[str] = None) -> list[tuple[str, str, int, int, int, bool, str]] | None:
    """
    Retrieve metadata on all columns in *table_name*.

    The metadata is returned as a list of tuples, each one containing the column name plus the data
    returned in *db_get_column_metadata()*. The standard *udt_names* are listed in *db_get_column_metadata()*.

    :param table_name: the, possibly schema-qualified, name of the table
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param errors: incidental error messages (may be a non-empty list)
    :return: a list of tuples with the metadata on all columns in *table_name*, or *None* if error
    """
    # initialize the return variable
    result: list[tuple[str, str, int, int, int, bool, str]] | None = None

    # assert the database engine
    engine_id, engine_type = _assert_engine(engine,
                                            errors=errors)
    if engine_id:
        # determine the table schema
        table_schema: str
        table_schema, table_name = table_name.split(sep=".") if "." in table_name else (None, table_name)

        # build the query
        sel_stmt: str | None = None
        where_data: dict[str, str] | None = None
        match engine_type:
            case DbEngine.MYSQL:
                pass
            case DbEngine.ORACLE:
                sel_stmt = ("SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, "
                            "DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT FROM ALL_TAB_COLUMNS "
                            "ORDER BY COLUMN_NAME")
                where_data = {
                    "TABLE_NAME": table_name.upper()
                }
                if table_schema:
                    where_data["OWNER"] = table_schema.upper()
            case DbEngine.POSTGRES:
                sel_stmt = ("SELECT column_name, udt_name, character_maximum_length, numeric_precision, "
                            "numeric_scale, is_nullable, column_default FROM information_schema.columns "
                            "ORDER BY column_name")
                where_data = {
                    "table_name": table_name.lower()
                }
                if table_schema:
                    where_data["table_schema"] = table_schema.lower()
            case DbEngine.SQLSERVER:
                sel_stmt = ("SELECT COLUMN_NAME, UDT_NAME, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, "
                            "NUMERIC_SCALE, IS_NULLABLE, COLUMN_DEFAULT FROM information_schema.columns "
                            "ORDER BH COLUMN_NAME")
                where_data = {
                    "TABLE_NAME": table_name.upper()
                }
                if table_schema:
                    where_data["TABLE_SCHEMA"] = table_schema.upper()

        # execute the query
        recs: list[tuple] = db_select(sel_stmt=sel_stmt,
                                      where_data=where_data,
                                      engine=engine_id,
                                      connection=connection,
                                      errors=errors)
        # process the query result
        if isinstance(recs, list) and recs:
            result = []
            for rec in recs:
                # BUG HANDLING: Oracle may report 'NUMBER(None, None)', NUMBER(None, <s>)' or 'NUMBER(<p>, None)'
                force_38: bool = engine_type == DbEngine.ORACLE and rec[1] == "NUMBER" and rec[3] is None
                force_0: bool = engine_type == DbEngine.ORACLE and rec[1] == "NUMBER" and rec[4] is None
                result.append((rec[0], rec[1], rec[2],
                               38 if force_38 else rec[3],
                               0 if force_0 else rec[4],
                               rec[5].startswith("Y"), rec[6]))
    return result
