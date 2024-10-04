from logging import Logger
from typing import Any

from .db_common import (
    _bind_columns, _bind_marks, _except_msg, _remove_nulls
)
from .db_pomes import db_connect, db_execute, db_select

def db_migrate_data(errors: list[str] | None,
                    source_engine: str,
                    source_table: str,
                    source_columns: list[str],
                    target_engine: str,
                    target_table: str,
                    target_columns: list[str] = None,
                    source_conn: Any = None,
                    target_conn: Any = None,
                    source_committable: bool = None,
                    target_committable: bool = None,
                    where_clause: str = None,
                    orderby_clause: str = None,
                    batch_size: int = None,
                    identity_column: str = None,
                    has_nulls: bool = None,
                    logger: Logger = None) -> int:
    """
    Migrate data from one database to another database.

    The origin and destination databases must be in the list of databases configured and
    supported by this package. The specified database columns must not be type LOB
    (large binary object), and must have the same cardinality, and be of respectively
    equivalent types, in both the origin and the destination databases.
    Specific handling is required for identity columns (i.e., columns with values generated
    directly by the database), and thus they must be identified by *identity_column*.
    For Oracle databases, *GENERATED ALWAYS* identity columns should be ommited from *source_columns*,
    as attempting to insert values therein raises an error, whereas *GENERATED BY DEFAULT AS IDENTITY*
    columns are acceptable.
    The parameter *has_nulls* indicates that one or more columns holding string content in the source
    table have embedded NULL characters. Some databases, particularly Postgres, do not accept NULLs
    embedded in text, so they must be removed.
    The parameters *source_committable* and *target_committable* are relevant only
    if *source_conn* and *target_conn* are provided, respectively, and are otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param source_engine: the source database engine type
    :param source_table: the possibly schema-qualified origin table
    :param source_columns: the columns to be migrated
    :param target_engine: the destination database engine type
    :param target_table: the possibly schema-qualified table to write the data to
    :param target_columns: the columns to write the data to (defaults to the source columns)
    :param source_conn: the connection to the source database (obtains a new one, if not provided)
    :param target_conn: the connection to the destination database (obtains a new one, if not provided)
    :param source_committable: whether to commit on *source_conn* upon errorless completion
    :param target_committable: whether to commit on *target_conn* upon errorless completion
    :param where_clause: optional criteria for tuple selection
    :param orderby_clause: optional retrieval order (required, if 'batch_size' is specified)
    :param batch_size: the maximum number of tuples to migrate in each batch, or 0 or None for no limit
    :param identity_column: column whose values are generated by the database
    :param has_nulls: indicates that one or more string-type source columns have nulls
    :param logger: optional logger
    :return: the number of tuples effectively migrated
    """
    # initialize the return variable
    result: int = 0

    # initialize the local errors list
    op_errors: list[str] = []

    # make sure to have connections to the source and destination databases
    curr_source_conn: Any = source_conn or db_connect(errors=op_errors,
                                                      engine=source_engine,
                                                      logger=logger)
    curr_target_conn: Any = target_conn or db_connect(errors=op_errors,
                                                      engine=target_engine,
                                                      logger=logger)
    if curr_source_conn and curr_target_conn:
        # make sure to have target columns
        if not target_columns:
            target_columns = source_columns

        # buid the SELECT query
        cols: str = ", ".join(source_columns)
        sel_stmt: str = f"SELECT {cols} FROM {source_table}"
        if where_clause:
            sel_stmt += f" WHERE {where_clause}"
        if orderby_clause:
            sel_stmt += f" ORDER BY {orderby_clause}"
            if batch_size:
                if source_engine == "postgres":
                    sel_stmt += f" OFFSET @offset ROWS LIMIT {batch_size}"
                elif source_engine in ["oracle", "slqserver"]:
                    sel_stmt += f" OFFSET @offset ROWS FETCH NEXT {batch_size} ROWS ONLY"
        else:
            # 'batch_size' requires an ORDER BY clause
            batch_size = 0

        # build the INSERT query
        if target_engine == "postgres":
            values: str = "VALUES %s"
            # pre-insert handling of identity columns on Postgres
            if identity_column:
                values = " OVERRIDING SYSTEM VALUE " + values
        else:
            bind_marks: str = _bind_marks(engine=target_engine,
                                          start=1,
                                          finish=len(target_columns)+1)
            values: str = f"VALUES({bind_marks})"
        cols = ", ".join(target_columns)
        insert_stmt: str = (f"INSERT INTO {target_table} "
                            f"({cols}) {values}")

        # log the migration start
        if logger:
            logger.debug(msg=(f"Started migrating data, "
                              f"from {source_engine}.{source_table} "
                              f"to {target_engine}.{target_table}"))

        # pre-insert handling of identity columns on SQLServer
        if identity_column and target_engine == "sqlserver":
            db_execute(errors=op_errors,
                       exc_stmt=f"SET IDENTITY_INSERT {target_table} ON",
                       engine=target_engine,
                       connection=curr_target_conn,
                       committable=False,
                       logger=logger)

        err_msg: str | None = None
        # errors ?
        if not op_errors:
            # no, migrate the data
            log_step: int = 10000
            try:
                source_cursor: Any = curr_source_conn.cursor()
                target_cursor: Any = curr_target_conn.cursor()
                if target_engine == "sqlserver":
                    target_cursor.fast_executemany = True
                # execute the query
                source_cursor.execute(statement=sel_stmt.replace("OFFSET @offset ROWS ", ""))
                rows: list[tuple] = source_cursor.fetchall()

                # traverse the result set
                while rows:
                    match target_engine:
                        case "mysql":
                            pass
                        case "oracle":
                            target_cursor.executemany(statement=insert_stmt,
                                                      parameters=rows)
                        case "postgres":
                            from psycopg2.extras import execute_values
                            if has_nulls:
                                rows = _remove_nulls(rows=rows)
                            execute_values(cur=target_cursor,
                                           sql=insert_stmt,
                                           argslist=rows)
                        case "sqlserver":
                            target_cursor.executemany(insert_stmt,
                                                      rows)

                    # increment the tuple migration counter
                    result += len(rows)

                    # log partial result
                    if logger and result % log_step == 0:
                        logger.debug(msg=(f"Migrating {result} tuples, "
                                          f"from {source_engine}.{source_table} "
                                          f"to {target_engine}.{target_table}"))
                    # read the next batch
                    if batch_size:
                        source_cursor.execute(statement=sel_stmt.replace("@offset", str(result)))
                        rows = source_cursor.fetchall()
                    else:
                        rows = []

                # close the cursors
                source_cursor.close()
                target_cursor.close()

                # post-insert handling of identity columns
                if identity_column and result > 0 and \
                   target_engine in ["postgres", "sqlserver"]:
                    # obtain the maximum value inserted
                    recs: list[tuple[int]] = db_select(errors=op_errors,
                                                       sel_stmt=(f"SELECT MAX({identity_column}) "
                                                                 f"FROM {target_table}"),
                                                       engine=target_engine,
                                                       connection=target_conn,
                                                       committable=False,
                                                       logger=logger)
                    # establish the commands to execute
                    if not op_errors:
                        if target_engine == "postgres":
                            db_execute(errors=op_errors,
                                       exc_stmt=(f"SELECT setval("
                                                 f"pg_get_serial_sequence('{target_table}', '{identity_column}'), "
                                                 f"{recs[0][0]})"),
                                       engine=target_engine,
                                       connection=target_conn,
                                       committable=False,
                                       logger=logger)
                        elif target_engine == "sqlserver":
                            db_execute(errors=op_errors,
                                       exc_stmt=f"SET IDENTITY_INSERT {target_table} OFF",
                                       engine=target_engine,
                                       connection=target_conn,
                                       committable=False,
                                       logger=logger)
                            if not op_errors:
                                db_execute(errors=op_errors,
                                           exc_stmt=f"DBCC CHECKIDENT ('{target_table}', RESEED, {recs[0][0]})",
                                           engine=target_engine,
                                           connection=target_conn,
                                           committable=False,
                                           logger=logger)
                if op_errors:
                    if curr_source_conn:
                        curr_source_conn.rollback()
                else:
                    # commit the transactions
                    if source_committable or not source_conn:
                        curr_source_conn.commit()
                    if target_committable or not target_conn:
                        curr_target_conn.commit()
            except Exception as e:
                # rollback the transactions
                if curr_source_conn:
                    curr_source_conn.rollback()
                if curr_target_conn:
                    curr_target_conn.rollback()
                result = 0
                err_msg = _except_msg(exception=e,
                                      engine=source_engine)
            finally:
                # close the connections, if locally acquired
                if curr_source_conn and not source_conn:
                    curr_source_conn.close()
                if curr_target_conn and not target_conn:
                    curr_target_conn.close()

        # log the migration finish
        if err_msg:
            op_errors.append(err_msg)
        if logger:
            logger.debug(msg=(f"Migrated {result} tuples, "
                              f"from {source_engine}.{source_table} "
                              f"to {target_engine}.{target_table}"))

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result

def db_migrate_lobs(errors: list[str] | None,
                    source_engine: str,
                    source_table: str,
                    source_lob_column: str,
                    source_pk_columns: list[str],
                    target_engine: str,
                    target_table: str,
                    target_lob_column: str = None,
                    target_pk_columns: list[str] = None,
                    source_conn: Any = None,
                    target_conn: Any = None,
                    source_committable: bool = None,
                    target_committable: bool = None,
                    where_clause: str = None,
                    accept_empty: bool = False,
                    chunk_size: int = None,
                    logger: Logger = None) -> int:
    """
    Migrate large binary objects (LOBs) from one database to another database.

    The origin and destination databases must be in the list of databases configured and
    supported by this package. One or more columns making up a primary key, or a unique row identifier,
    must exist on *source_table* and *target_table*, and be provided in *source_pk_columns*.
    It is assumed that the primary key columns have the same cardinality, and are of respectively
    equivalent types, in both the origin and the destination databases.
    The parameters *source_committable* and *target_committable* are relevant only
    if *source_conn* and *target_conn* are provided, respectively, and are otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param source_engine: the source database engine type
    :param source_table: the possibly schema-qualified table holding the LOBs
    :param source_lob_column: the column holding the LOB
    :param source_pk_columns: columns making up a primary key, or a unique identifier for a tuple, in source table
    :param target_engine: the destination database engine type
    :param target_table: the possibly schema-qualified table to write the LOB to
    :param target_lob_column: the column to write the LOB to (defaults to the source column)
    :param target_pk_columns: columns making up a primary key, or a unique identifier for a tuple, in target table
    :param source_conn: the connection to the source database (obtains a new one, if not provided)
    :param target_conn: the connection to the destination database (obtains a new one, if not provided)
    :param source_committable: whether to commit on *source_conn* upon errorless completion
    :param target_committable: whether to commit on *target_conn* upon errorless completion
    :param where_clause: the criteria for tuple selection
    :param accept_empty: account for all LOBs, even empty ones
    :param chunk_size: size in bytes of the data chunk to read/write, or 0 or None for no limit
    :param logger: optional logger
    :return: the number of LOBs effectively migrated
    """
    # initialize the return variable
    result: int = 0

    # initialize the local errors list
    op_errors: list[str] = []

    # make sure to have connections to the source and destination databases
    curr_source_conn: Any = source_conn or db_connect(errors=op_errors,
                                                      engine=source_engine,
                                                      logger=logger)
    curr_target_conn: Any = target_conn or db_connect(errors=op_errors,
                                                      engine=target_engine,
                                                      logger=logger)
    if curr_source_conn and curr_target_conn:
        # make sure to have a target column
        if not target_lob_column:
            target_lob_column = source_lob_column

        # make sure to have target PK columns
        if not target_pk_columns:
            target_pk_columns = source_pk_columns

        # normalize the chunk size
        if not chunk_size:
            chunk_size = -1

        # buid the SELECT query
        source_pks: str = ", ".join(source_pk_columns)
        sel_stmt: str = f"SELECT {source_pks}, {source_lob_column} FROM {source_table}"
        if where_clause:
            sel_stmt += f" WHERE {where_clause}"
        lob_index: int = len(source_pk_columns)

        # build the UPDATE query
        target_where_columns: str = _bind_columns(engine=target_engine,
                                                  columns=target_pk_columns,
                                                  concat=" AND ",
                                                  start_index=2)
        if target_engine == "oracle":
            update_stmt: str = (f"UPDATE {target_table} "
                                f"SET {target_lob_column} = empty_blob() "
                                f"WHERE {target_where_columns} "
                                f"RETURNING {target_lob_column} INTO :{len(target_where_columns) + 2}")
        else:
            lob_bind: str = _bind_columns(engine=target_engine,
                                          columns=[target_lob_column],
                                          concat=", ",
                                          start_index=1)
            update_stmt: str = (f"UPDATE {target_table} "
                                f"SET {target_lob_column} = "
                                f"{target_lob_column} || {lob_bind[len(target_lob_column) + 3:]} "
                                f"WHERE {target_where_columns}")

        # log the migration start
        if logger:
            logger.debug(msg=(f"Started migrating LOBs, "
                              f"from {source_engine}.{source_table}.{source_lob_column} "
                              f"to {target_engine}.{target_table}.{target_lob_column}"))
        # migrate the LOBs
        log_step: int = 10000
        err_msg: str | None = None
        try:
            source_cursor: Any = curr_source_conn.cursor()
            target_cursor: Any = curr_target_conn.cursor()

            # execute the query
            # (parameter is 'statement' for oracle, 'query' for postgres, 'sql' for sqlserver)
            source_cursor.execute(sel_stmt)

            # fetch rows
            for row in source_cursor:

                # retrieve the values of the primary key columns (leave LOB column out)
                pk_vals: tuple = tuple([row[inx] for inx in range(lob_index)])

                ora_lob: Any = None
                if target_engine == "oracle":
                    # initialize the LOB column with an empty LOB
                    import oracledb
                    lob_var = target_cursor.var(oracledb.DB_TYPE_BLOB)
                    target_cursor.execute(statement=update_stmt,
                                          parameters=(*pk_vals, lob_var))
                    ora_lob = lob_var.getValue()

                # access the LOB in chunks and write it to database
                offset: int = 1
                has_data: bool = accept_empty
                lob: Any = row[lob_index]
                lob_data: bytes | str = lob.read(offset=offset,
                                                 amount=chunk_size) if lob else None
                while lob_data:
                    size: int = len(lob_data)
                    has_data = True
                    match target_engine:
                        case "mysql":
                            pass
                        case "oracle":
                            ora_lob.write(data=lob_data,
                                          offset=offset)
                        case "postgres":
                            from psycopg2 import Binary
                            # remove append indication on first update
                            update_pg: str = update_stmt if offset > 1 else \
                                             update_stmt.replace(f"{target_lob_column} || ", "")

                            # string data may come from LOB (Oracle's NCLOB is a good example)
                            col_data: str | Binary = Binary(lob_data) if isinstance(lob_data, bytes) else lob_data
                            target_cursor.execute(query=update_pg,
                                                  vars=(col_data, *pk_vals))
                        case "sqlserver":
                            from pyodbc import Binary
                            # remove append indication on first update
                            update_sqls: str = update_stmt if offset > 1 else \
                                               update_stmt.replace(f"{target_lob_column} || ", "")

                            # string data may come from LOB (Oracle's NCLOB is a good example)
                            col_data: str | Binary = Binary(lob_data) if isinstance(lob_data, bytes) else lob_data
                            target_cursor.execute(sql=update_sqls,
                                                  params=(col_data, *pk_vals))
                    # read the next chunk
                    offset += size
                    lob_data = lob.read(offset=offset,
                                        amount=chunk_size)

                # increment the LOB migration counter, if applicable
                if has_data:
                    result += 1

                # log partial result at each 'log_step' LOBs migrated
                if logger and result % log_step == 0:
                    logger.debug(msg=(f"Migrating {result} LOBs, "
                                      f"from {source_engine}.{source_table}.{source_lob_column} "
                                      f"to {target_engine}.{target_table}.{target_lob_column}"))

            # close the cursors and commit the transactions
            source_cursor.close()
            if source_committable or not source_conn:
                curr_source_conn.commit()
            target_cursor.close()
            if target_committable or not target_conn:
                curr_target_conn.commit()
        except Exception as e:
            # rollback the transactions
            if curr_source_conn:
                curr_source_conn.rollback()
            if curr_target_conn:
                curr_target_conn.rollback()
            result = 0
            err_msg = _except_msg(exception=e,
                                  engine=source_engine)
        finally:
            # close the connections, if locally acquired
            if curr_source_conn and not source_conn:
                curr_source_conn.close()
            if curr_target_conn and not target_conn:
                curr_target_conn.close()

        # log the migration finish
        if err_msg:
            op_errors.append(err_msg)
        if logger:
            logger.debug(msg=(f"Migrated {result} LOBs, "
                              f"from {source_engine}.{source_table}.{source_lob_column} "
                              f"to {target_engine}.{target_table}.{target_lob_column}"))

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result

def db_stream_lobs(errors: list[str] | None,
                   table: str,
                   lob_column: str,
                   pk_columns: list[str],
                   ref_column: str = None,
                   engine: str = None,
                   connection: Any = None,
                   committable: bool = None,
                   where_clause: str = None,
                   accept_empty: bool = False,
                   chunk_size: int = None,
                   logger: Logger = None) -> None:
    """
    Stream data in large binary objects (LOBs) from a database.

    This is accomplished with the implementation of the *generator* pattern, whereby an *iterator*
    is returned, allowing the invoker to iterate over the values being streamed.
    The origin database must be in the list of databases configured and
    supported by this package. One or more columns making up a primary key, or a unique
    row identifier, must exist on *source_table*, and be provided in *source_pk_columns*.
    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param table: the table holding the LOBs
    :param lob_column: the column holding the LOB
    :param pk_columns: columns making up a primary key, or a unique identifier for a tuple, in database
    :param ref_column: optional column whose content to return when yielding
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param where_clause: the criteria for tuple selection
    :param accept_empty: account for all LOBs, even empty ones
    :param chunk_size: size in bytes of the data chunk to read/write, or 0 or None for no limit
    :param logger: optional logger
    """
    # initialize the local errors list
    op_errors: list[str] = []

    # make sure to have a connection to the source database
    curr_conn: Any = connection or db_connect(errors=op_errors,
                                              engine=engine,
                                              logger=logger)
    if curr_conn:
        # normalize the chunk size
        if not chunk_size:
            chunk_size = -1

        # buid the SELECT query
        lob_index: int = len(pk_columns)
        sel_stmt: str = f"SELECT {', '.join(pk_columns)}"
        if ref_column and ref_column not in pk_columns:
            sel_stmt += f", {ref_column}"
            lob_index += 1
        sel_stmt += f", {lob_column} FROM {table}"
        if where_clause:
            sel_stmt += f" WHERE {where_clause}"

        # log the migration start
        if logger:
            logger.debug(msg=(f"Started migrating LOBs, "
                              f"from {engine}.{table}.{lob_column}"))

        # stream the LOBs
        log_step: int = 10000
        lob_count: int = 0
        err_msg: str | None = None
        try:
            source_cursor: Any = curr_conn.cursor()

            # execute the query
            # (parameter is 'statement' for oracle, 'query' for postgres, 'sql' for sqlserver)
            source_cursor.execute(sel_stmt)

            # fetch rows
            for row in source_cursor:

                # retrieve the values of the primary key and reference columns (leave LOB column out)
                pk_vals: tuple = tuple([row[inx] for inx in range(lob_index)])
                identifier: dict[str, Any] = {}
                for inx, pk_val in enumerate(pk_vals):
                    identifier[pk_columns[inx]] = pk_val
                # send the LOB's metadata
                yield identifier

                # access the LOB's bytes in chunks and stream them
                offset: int = 1
                has_data: bool = accept_empty
                lob: Any = row[lob_index]
                lob_data: bytes | str = lob.read(offset=offset,
                                                 amount=chunk_size) if lob else None
                while lob_data:
                    # send a data chunk
                    yield lob_data
                    size: int = len(lob_data)
                    has_data = True
                    # read the next chunk
                    offset += size
                    lob_data = lob.read(offset=offset,
                                        amount=chunk_size)
                if has_data:
                    # increment the LOB migration counter, if applicable
                    lob_count += 1

                # signal that sending data chunks for the current LOB is finished
                yield None

                # log partial result at each 'log_step' LOBs migrated
                if logger and lob_count % log_step == 0:
                    logger.debug(msg=(f"Streaming {lob_count} LOBs, "
                                      f"from {engine}.{table}.{lob_column}"))

            # close the cursors and commit the transactions
            source_cursor.close()
            if committable or not connection:
                curr_conn.commit()
        except Exception as e:
            # rollback the transactions
            if curr_conn:
                curr_conn.rollback()
            lob_count = 0
            err_msg = _except_msg(exception=e,
                                  engine=engine)
        finally:
            # close the connections, if locally acquired
            if curr_conn and not connection:
                curr_conn.close()

        # log the stream finish
        if err_msg:
            op_errors.append(err_msg)
        if logger:
            logger.debug(msg=(f"Streamed {lob_count} LOBs, "
                         f"from {engine}.{table}.{lob_column}"))

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)
