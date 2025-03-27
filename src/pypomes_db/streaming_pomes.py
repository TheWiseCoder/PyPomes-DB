from contextlib import suppress
from logging import Logger
from typing import Any

from .db_pomes import db_connect
from .db_common import DbEngine, _except_msg


def db_stream_data(errors: list[str] | None,
                   table: str,
                   columns: list[str],
                   engine: DbEngine = None,
                   connection: Any = None,
                   committable: bool = None,
                   where_clause: str = None,
                   orderby_clause: str = None,
                   offset_count: int = None,
                   limit_count: int = None,
                   batch_size_in: int = None,
                   batch_size_out: int = None,
                   logger: Logger = None) -> int | None:
    """
    Stream data from a database table.

    This is accomplished with the implementation of the *generator* pattern, whereby an *iterator*
    is returned, allowing the invoker to iterate over the values being streamed.
    The database in *engine* must be in the list of databases configured and supported by this package.

    Care should be exercised when specifying the parameter *offset_count*, so as not to skip wanted tuples.
    It is used to offset the retrieval of tuples, and is ignored for all values different from a positive integer.

    The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param table: the possibly schema-qualified table to read the data from
    :param columns: the table columns to stream
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param where_clause: the criteria for tuple selection
    :param orderby_clause: optional retrieval order
    :param offset_count: number of tuples to skip in source table (defaults to none)
    :param limit_count: maximum number of tuples to migrate (defaults to no limit)
    :param batch_size_in: maximum number of tuples to read in each batch (defaults to no limit)
    :param batch_size_out: maximum number of tuples to stream in each batch (defaults to no limit)
    :param logger: optional logger
    :return: the number of tuples effectively streamed, or *None* if error
    """
    # initialize the return variable
    result: int | None = 0

    # initialize the local errors list
    op_errors: list[str] = []

    # make sure to have a connection to the source database
    curr_conn: Any = connection or db_connect(errors=op_errors,
                                              engine=engine,
                                              logger=logger)
    if curr_conn:
        # normalize these parameters
        offset_count = __normalize_value(value=offset_count)
        limit_count = __normalize_value(value=limit_count)
        batch_size_in = __normalize_value(value=batch_size_in)
        batch_size_out = __normalize_value(value=batch_size_out)

        # buid the SELECT query
        sel_stmt: str = f"SELECT {', '.join(columns)} FROM {table}"
        if where_clause:
            sel_stmt += f" WHERE {where_clause}"
        if orderby_clause:
            sel_stmt += f" ORDER BY {orderby_clause}"
        sel_stmt += " OFFSET @offset ROWS"

        if limit_count or batch_size_in:
            if engine == DbEngine.POSTGRES:
                sel_stmt += " LIMIT @limit"
            elif engine == DbEngine.ORACLE:
                sel_stmt += " FETCH @limit ROWS ONLY"
            elif engine == DbEngine.SQLSERVER:
                sel_stmt = sel_stmt.replace("SELECT ", "SELECT TOP @limit ", 1)

        # log the streaming start
        if logger:
            logger.debug(msg=f"Started streaming data from {engine}.{table}, "
                             f"limit {limit_count}, offset {offset_count},"
                             f" batch size in {batch_size_in},  batch size out {batch_size_out}")
        # stream the tuples
        row_count: int = 0
        err_msg: str | None = None
        try:
            source_cursor: Any = curr_conn.cursor()

            # adjust the offset and limit
            curr_stmt: str = sel_stmt
            curr_limit: int = min(batch_size_in, limit_count)
            if curr_limit == 0:
                curr_limit = max(batch_size_in, limit_count)
            if offset_count:
                curr_stmt = curr_stmt.replace("@offset", str(offset_count), 1)\
                                     .replace("@limit", str(curr_limit), 1)\
                                     .replace(" FETCH ", " FETCH NEXT ", 1)
            else:
                curr_stmt = curr_stmt.replace(" OFFSET @offset ROWS", "", 1)\
                                     .replace("@limit", str(curr_limit), 1)\
                                     .replace(" FETCH ", " FETCH FIRST ", 1)
            # execute the query
            # (parameter name is 'statement' for oracle, 'query' for postgres, 'sql' for sqlserver)
            source_cursor.execute(curr_stmt)
            rows_in: list[tuple] = source_cursor.fetchall()

            # traverse the result set
            while rows_in:
                # log the retrieval operation
                if logger:
                    logger.debug(msg=f"Read {len(rows_in)} tuples from {engine}.{table}")
                pos_from: int = 0

                # migrate the tuples
                while pos_from < len(rows_in):
                    pos_to: int = min(pos_from + batch_size_out, len(rows_in)) \
                                  if batch_size_out else len(rows_in)
                    rows_out: list[tuple] = rows_in[pos_from:pos_to]
                    # noinspection PyTypeChecker
                    yield rows_out
                    pos_from = pos_to

                    # increment the tuple streaming counter and log the partial streaming
                    result += len(rows_out)
                    if logger:
                        logger.debug(msg=f"Streamed {result} tuples from {engine}.{table}")
                # read the next batch
                if limit_count > result or (batch_size_in and not limit_count):
                    curr_limit = min(batch_size_in, limit_count - result)
                    if curr_limit <= 0:
                        curr_limit = max(batch_size_in, limit_count - result)
                    curr_stmt = sel_stmt.replace("@offset", str(offset_count + result), 1)\
                                        .replace("@limit", str(curr_limit), 1)\
                                        .replace(" FETCH ", " FETCH NEXT ", 1)
                    source_cursor.execute(statement=curr_stmt)
                    rows_in = source_cursor.fetchall()
                else:
                    # signal end of migration
                    rows_in = []

            # close the cursors and commit the transactions
            source_cursor.close()
            if committable or not connection:
                curr_conn.commit()
        except Exception as e:
            # rollback the transactions
            if curr_conn:
                with suppress(Exception):
                    curr_conn.rollback()
            row_count = 0
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
                logger.error(msg=err_msg)
        elif logger:
            logger.debug(msg=f"Finished streaming {row_count} tuples from {engine}.{table}")

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result


def db_stream_lobs(errors: list[str] | None,
                   table: str,
                   lob_column: str,
                   pk_columns: list[str],
                   ref_column: str = None,
                   engine: DbEngine = None,
                   connection: Any = None,
                   committable: bool = None,
                   where_clause: str = None,
                   orderby_clause: str = None,
                   offset_count: int = None,
                   limit_count: int = None,
                   batch_size: int = None,
                   chunk_size: int = None,
                   logger: Logger = None,
                   log_trigger: int = 10000) -> None:
    """
    Stream data in large binary objects (LOBs) from a database table.

    This is accomplished with the implementation of the *generator* pattern, whereby an *iterator*
    is returned, allowing the invoker to iterate over the values being streamed.
    The database in *engine* must be in the list of databases configured and supported by this package.
    One or more columns making up a primary key, or a unique row identifier, must exist on *source_table*,
    and be provided in *source_pk_columns*.

    Care should be exercised when specifying the parameter *offset_count*, so as not to skip wanted tuples.
    It is used to offset the retrieval of tuples, and is ignored for all values different from a positive integer.
    Further, if *batch_size* or *offset_count* has been specified, but *orderby_clause* has not,
    then an ORDER BY clause is constructed from the data in *pk_columns*.

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
    :param orderby_clause: optional retrieval order
    :param offset_count: number of tuples to skip in source table (defaults to none)
    :param limit_count: maximum number of tuples to migrate (defaults to no limit)
    :param batch_size: maximum number of tuples to read in each batch (defaults to no limit)
    :param chunk_size: size in bytes of the data chunk to read/write (defaults to no limit)
    :param logger: optional logger
    :param log_trigger: number of tuples to trigger logging info on migration (defaults to 10000 tuples)
    """
    # initialize the local errors list
    op_errors: list[str] = []

    # make sure to have a connection to the source database
    curr_conn: Any = connection or db_connect(errors=op_errors,
                                              engine=engine,
                                              logger=logger)
    if curr_conn:
        # normalize these parameters
        offset_count = __normalize_value(value=offset_count)
        limit_count = __normalize_value(value=limit_count)
        batch_size = __normalize_value(value=batch_size)
        chunk_size = __normalize_value(value=chunk_size) or -1
        log_trigger = __normalize_value(value=log_trigger)

        # buid the SELECT query
        ref_columns: list[str] = pk_columns.copy()
        lob_index: int = len(pk_columns)
        sel_stmt: str = f"SELECT {', '.join(pk_columns)}"
        if ref_column and ref_column not in pk_columns:
            sel_stmt += f", {ref_column}"
            lob_index += 1
            ref_columns.append(ref_column)
        sel_stmt += f", {lob_column} FROM {table}"
        if where_clause:
            sel_stmt += f" WHERE {where_clause}"
        if not orderby_clause and (offset_count or batch_size):
            orderby_clause = ", ".join(pk_columns)
        if orderby_clause:
            sel_stmt += f" ORDER BY {orderby_clause}"
        sel_stmt += " OFFSET @offset ROWS"

        if limit_count or batch_size:
            if engine == DbEngine.POSTGRES:
                sel_stmt += " LIMIT @limit"
            elif engine == DbEngine.ORACLE:
                sel_stmt += " FETCH @limit ROWS ONLY"
            elif engine == DbEngine.SQLSERVER:
                sel_stmt = sel_stmt.replace("SELECT ", "SELECT TOP @limit ", 1)

        # log the migration start
        if logger:
            logger.debug(msg="Started streaming LOBs "
                             f"from {engine}.{table}.{lob_column}, "
                             f"limit {limit_count}, offset {offset_count}, "
                             f"batch size {batch_size}, chunk size {chunk_size}")
        # stream the LOBs
        log_step: int = 0
        lob_count: int = 0
        err_msg: str | None = None
        try:
            source_cursor: Any = curr_conn.cursor()

            # adjust the offset and limit
            curr_stmt: str = sel_stmt
            curr_limit: int = min(batch_size, limit_count)
            if curr_limit == 0:
                curr_limit = max(batch_size, limit_count)
            if offset_count:
                curr_stmt = curr_stmt.replace("@offset", str(offset_count), 1)\
                                     .replace("@limit", str(curr_limit), 1)\
                                     .replace(" FETCH ", " FETCH NEXT ", 1)
            else:
                curr_stmt = curr_stmt.replace(" OFFSET @offset ROWS", "", 1)\
                                     .replace("@limit", str(curr_limit), 1)\
                                     .replace(" FETCH ", " FETCH FIRST ", 1)
            # go for the data
            next_rs: bool = True
            while next_rs:
                next_rs = False

                # execute the query
                # (parameter name is 'statement' for oracle, 'query' for postgres, 'sql' for sqlserver)
                source_cursor.execute(curr_stmt)

                # traverse the result set
                row: tuple = source_cursor.fetchone()
                while row:
                    next_rs = True

                    # retrieve the values of the primary key and reference columns (leave LOB column out)
                    ref_vals: tuple = tuple([row[inx] for inx in range(lob_index)])
                    identifier: dict[str, Any] = {}
                    for inx, pk_val in enumerate(ref_vals):
                        identifier[ref_columns[inx]] = pk_val
                    # send the LOB's metadata
                    # noinspection PyTypeChecker
                    yield identifier

                    # access the LOB's bytes in chunks and stream them
                    first: bool = True
                    offset: int = 1
                    is_migrated: bool = False
                    lob: Any = row[lob_index]
                    lob_data: bytes | str = lob.read(offset=offset,
                                                     amount=chunk_size) if lob is not None else None
                    # make sure to skip null LOBs, and to migrate empty ones
                    while lob_data is not None and (first or len(lob_data) > 0):
                        first = False
                        is_migrated = True
                        # send a data chunk
                        # noinspection PyTypeChecker
                        yield lob_data

                        # read the next chunk
                        if len(lob_data) > 0:
                            offset += len(lob_data)
                            lob_data = lob.read(offset=offset,
                                                amount=chunk_size)
                    if is_migrated:
                        # increment the LOB migration counter, if applicable
                        lob_count += 1
                        log_step += 1

                    # signal that sending data chunks for the current LOB is finished
                    # noinspection PyTypeChecker
                    yield None

                    # log partial result at each 'log_trigger' LOBs migrated
                    if logger and log_step >= log_trigger:
                        logger.debug(msg=f"Streamed {lob_count} LOBs "
                                         f"from {engine}.{table}.{lob_column}")
                        log_step = 0

                    # retrieve the next row
                    row = source_cursor.fetchone()

                # adjust the new offset and limit
                if next_rs and (limit_count > lob_count or (batch_size and not limit_count)):
                    curr_limit = min(batch_size, limit_count - lob_count)
                    if curr_limit <= 0:
                        curr_limit = max(batch_size, limit_count - lob_count)
                    curr_stmt = sel_stmt.replace("@offset", str(offset_count + lob_count), 1)\
                                        .replace("@limit", str(curr_limit), 1)\
                                        .replace(" FETCH ", " FETCH NEXT ", 1)
                else:
                    # signal end of migration
                    next_rs = False

            # close the cursors and commit the transactions
            source_cursor.close()
            if committable or not connection:
                curr_conn.commit()
        except Exception as e:
            # rollback the transactions
            if curr_conn:
                with suppress(Exception):
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
                logger.error(msg=err_msg)
        elif logger:
            logger.debug(msg=f"Finished streaming {lob_count} LOBs "
                             f"from {engine}.{table}.{lob_column}")

    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)


def __normalize_value(value: int) -> int:
    """
    Normalize *value* to a consistent value.

    :param value: the value to normalized
    :return: the normalized value
    """
    if isinstance(value, int) and \
            not isinstance(value, bool) and value > 0:
        result: int = value
    else:
        result: int = 0

    return result
