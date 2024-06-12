from logging import WARNING, Logger
from typing import Any

from .db_common import (
    _bind_columns, _bind_marks, _log, _except_msg, _remove_nulls
)
from .db_pomes import db_connect


def db_migrate_data(errors: list[str],
                    source_engine: str,
                    source_table: str,
                    source_columns: list[str],
                    target_engine: str,
                    target_table: str = None,
                    target_columns: list[str] = None,
                    source_conn: Any = None,
                    target_conn: Any = None,
                    source_committable: bool = True,
                    target_committable: bool = True,
                    where_clause: tuple = None,
                    batch_size: int = None,
                    logger: Logger = None) -> int:
    """
    Migrate data from one database to another database.

    The origin and destination databases must be in the list of databases configured and
    supported by this package. The specified database columns must not be type LOB
    (large binary object), and must have the same cardinality, and be of respectively
    equivalent types, in both the origin and the destination databases.

    :param errors: incidental error messages
    :param source_engine: the source database engine type
    :param source_table: the origin table
    :param source_columns: the colums to be migrated
    :param target_engine: the destination database engine type
    :param target_table: the table to write the data to (defaults to the source table)
    :param target_columns: the columns to write the data to (defaults to the source columns)
    :param source_conn: the connection to the source database (obtains a new one, if not provided)
    :param target_conn: the connection to the destination database (obtains a new one, if not provided)
    :param source_committable: whether to commit upon errorless completion ('False' needs 'source_conn' to be provided)
    :param target_committable: whether to commit upon errorless completion ('False' needs 'target_conn' to be provided)
    :param where_clause: the criteria for tuple selection
    :param batch_size: the maximum number of tuples to migrate in each batch, or 0 or None for no limit
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
    # make sure to have a target table
    if not target_table:
        target_table = source_table

    # make sure to have target columns
    if not target_columns:
        target_columns = source_columns

    # buid the SELECT query
    cols: str = ", ".join(source_columns)
    sel_stmt: str = f"SELECT {cols} FROM {source_table}"
    if where_clause:
        sel_stmt += f" WHERE {where_clause}"

    # build the INSERT query
    if target_engine == "postgres":
        values: str = "VALUES %s"
    else:
        raw_row: str = _bind_marks(engine=target_engine,
                                   start=1,
                                   finish=len(target_columns)+1)
        values: str = f"VALUES({raw_row})"
    cols: str = ", ".join(target_columns)
    insert_stmt = (f"INSERT INTO {target_table} "
                   f"({cols}) {values}")

    # log the migration start
    _log(logger=logger,
         engine=source_engine,
         stmt=(f"Started migrating data, "
                  f"from {source_engine}.{source_table} "
                  f"to {target_engine}.{target_table}"))

    # migrate the data
    err_msg: str | None = None
    try:
        source_cursor: Any = curr_source_conn.cursor()
        target_cursor: Any = curr_target_conn.cursor()
        if target_engine == "sqlserver":
            target_cursor.fast_executemany = True

        # execute the query
        source_cursor.execute(statement=sel_stmt)

        # establish the conditions for logging
        count_step: int = 100000
        count_mark = count_step

        # fetch rows in batches/all rows
        rows: list[tuple]
        if batch_size:
            rows = source_cursor.fetchmany(size=batch_size)
        else:
            rows = source_cursor.fetchall()
        while rows:
            match target_engine:
                case "mysql":
                    pass
                case "oracle":
                    target_cursor.executemany(statement=insert_stmt,
                                              parameters=rows)
                    result += len(rows)
                case "postgres":
                    from psycopg2.extras import execute_values
                    try:
                        execute_values(cur=target_cursor,
                                       sql=insert_stmt,
                                       argslist=rows)
                    except ValueError as e:
                        # is it a 'ValueError' exception on NULLs in strings ?
                        # ("A string literal cannot contain NUL (0x00) characters.")
                        if "contain NUL" in e.args[0]:
                            # yes, log the occurrence, remove the NULLs, and try again
                            _log(logger=logger,
                                 engine="postgres",
                                 level=WARNING,
                                 stmt=f"Found NULLs in values for {insert_stmt}")
                            # search for NULLs in input data
                            cleaned_rows: [tuple] = []
                            for row in rows:
                                cleaned_row: list[Any] = _remove_nulls(row)
                                # has the row been cleaned ?
                                if cleaned_row:
                                    # yes, use it
                                    cleaned_rows.append(tuple(cleaned_row))
                                else:
                                    # no, use original row
                                    cleaned_rows.append(row)
                            # insert the cleaned data
                            execute_values(cur=target_cursor,
                                           sql=insert_stmt,
                                           argslist=cleaned_rows)
                case "sqlserver":
                    target_cursor.executemany(insert_stmt, rows)

            # increment the tuple migration counter
            result += len(rows)

            # log partial result
            if result > count_mark:
                count_mark = result + count_step
                _log(logger=logger,
                     engine=source_engine,
                     stmt=(f"Migrated {result} tuples, "
                              f"from {source_engine}.{source_table} "
                              f"to {target_engine}.{target_table}"))

            # read the next batch
            if batch_size:
                rows = source_cursor.fetchmany(size=batch_size)
            else:
                rows = source_cursor.fetchall()

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
        err_msg = _except_msg(exception=e,
                              engine=source_engine)
    finally:
        # close the connections, if locally acquired
        if curr_source_conn and not source_conn:
            curr_source_conn.close()
        if curr_target_conn and not target_conn:
            curr_target_conn.close()

    # log the migration finish
    _log(logger=logger,
         engine=source_engine,
         err_msg=err_msg,
         errors=op_errors,
         stmt=(f"Migrated {result} tuples, "
                  f"from {source_engine}.{source_table} "
                  f"to {target_engine}.{target_table}"))

    # acknowledge eventual local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result

def db_migrate_lobs(errors: list[str],
                    source_engine: str,
                    source_table: str,
                    source_lob_column: str,
                    source_pk_columns: list[str],
                    target_engine: str,
                    target_table: str = None,
                    target_lob_column: str = None,
                    target_pk_columns: list[str] = None,
                    source_conn: Any = None,
                    target_conn: Any = None,
                    source_committable: bool = True,
                    target_committable: bool = True,
                    where_clause: tuple = None,
                    chunk_size: int = None,
                    logger: Logger = None) -> int:
    """
    Migrate large binary objects (LOBs) from one database to another database.

    The origin and destination databases must be in the list of databases configured and
    supported by this package. One or more columns making up a primary key, or a unique row identifier,
    must exist on *source_table* and *target_table*, and be provided in *pk_columns*.
    It is assumed that the primary key columns have the same cardinality, and are of respectively
    equivalent types, in both the origin and the destination databases.

    :param errors: incidental error messages
    :param source_engine: the source database engine type
    :param source_table: the table holding the LOBs
    :param source_lob_column: the column holding the LOB
    :param source_pk_columns: columns making up a primary key, or a unique identifier for a tuple, in source database
    :param target_engine: the destination database engine type
    :param target_table: the table to write the lob to (defaults to the source table)
    :param target_lob_column: the column to write the lob to (defaults to the source column)
    :param target_pk_columns: columns making up a primary key, or a unique identifier for a tuple, in target database
    :param source_conn: the connection to the source database (obtains a new one, if not provided)
    :param target_conn: the connection to the destination database (obtains a new one, if not provided)
    :param source_committable: whether to commit upon errorless completion ('False' needs 'source_conn' to be provided)
    :param target_committable: whether to commit upon errorless completion ('False' needs 'target_conn' to be provided)
    :param where_clause: the criteria for tuple selection
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
    # make sure to have a target table
    if not target_table:
        target_table = source_table

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
    blob_index: int = len(source_pk_columns)

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
                            f"SET {target_lob_column} = {target_lob_column} || {lob_bind[len(target_lob_column) + 3:]} "
                            f"WHERE {target_where_columns}")

    # log the migration start
    _log(logger=logger,
         engine=source_engine,
         stmt=(f"Started migrating LOBs, "
                  f"from {source_engine}.{source_table}.{source_lob_column} "
                  f"to {target_engine}.{target_table}.{target_lob_column}"))

    # migrate the LOBs
    count_step: int = 10000
    err_msg: str | None = None
    try:
        source_cursor: Any = curr_source_conn.cursor()
        target_cursor: Any = curr_target_conn.cursor()

        # execute the query
        # (parameter is 'statement' for oracle, 'query' for postgres, 'sql' for sqlserver)
        source_cursor.execute(sel_stmt)

        # fetch rows
        for row in source_cursor:

            # retrieve the values of the primary key columns (leave lob column out)
            pk_vals: tuple = tuple([row[inx] for inx in range(blob_index)])

            ora_lob: Any = None
            if target_engine == "oracle":
                # initialize the LOB column with an empty LOB
                import oracledb
                lob_var = target_cursor.var(oracledb.DB_TYPE_BLOB)
                target_cursor.execute(statement=update_stmt,
                                      parameters=(*pk_vals, lob_var))
                ora_lob = lob_var.getValue()


            # access the blob in chunks and write it to database
            offset: int = 1
            has_data: bool = False
            lob: Any = row[blob_index]
            lob_data: bytes | str = lob.read(offset=offset,
                                             amount=chunk_size) \
                                    if lob else None
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

            # log partial result at each 'count_step' LOBs migrated
            if result % count_step == 0:
                _log(logger=logger,
                     engine=source_engine,
                     stmt=(f"Migrated {result} LOBs, "
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
        err_msg = _except_msg(exception=e,
                              engine=source_engine)
    finally:
        # close the connections, if locally acquired
        if curr_source_conn and not source_conn:
            curr_source_conn.close()
        if curr_target_conn and not target_conn:
            curr_target_conn.close()

    # log the migration finish
    _log(logger=logger,
         engine=source_engine,
         err_msg=err_msg,
         errors=op_errors,
         stmt=(f"Migrated {result} LOBs, "
                  f"from {source_engine}.{source_table}.{source_lob_column} "
                  f"to {target_engine}.{target_table}.{target_lob_column}"))

    # acknowledge eventual local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result
