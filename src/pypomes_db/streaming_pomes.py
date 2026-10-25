import time
from collections.abc import Generator
from contextlib import suppress
from logging import Logger
from pypomes_core import timestamp_duration
from typing import Any

from .db_common import (
    DB_CONN_DATA, DbEngine, DbParam, _assert_engine, _except_msg
)
from .db_pomes import db_connect, db_close


def db_stream_data(table: str,
                   columns: list[str],
                   where_clause: str = None,
                   orderby_clause: str = None,
                   offset_count: int = None,
                   limit_count: int = None,
                   batch_size_in: int = None,
                   batch_size_out: int = None,
                   engine: DbEngine | str = None,
                   connection: Any = None,
                   committable: bool = None,
                   errors: list[str] = None) -> Generator[list[tuple], None, None]:
    """
    Stream data from a database table.

    This is accomplished with the implementation of the *generator* pattern, whereby an *iterator*
    is returned, allowing the invoker to iterate over the values being streamed.
    The database in *engine* must be in the list of databases configured and supported by this package.

    It is recommend that *orderby_clause* be provided, specially if partial streaming is being requested.
    If not positive integers, *offset_count*, *limit_count*, *batch_size_in*, and *chunk_size_out* are ignored.
    Care should be exercised when specifying *offset_count*, so as not to skip wanted tuples,
    as it is used to offset the retrieval of tuples.

    The parameter *committable* defines whether a commit or rollback is performed on the provided *connection*.

    :param table: the, possibly schema-qualified, table to read the data from
    :param columns: the table columns to stream
    :param where_clause: the criteria for tuple selection
    :param orderby_clause: optional retrieval order
    :param offset_count: number of tuples to skip in source table (defaults to none)
    :param limit_count: maximum number of tuples to migrate (defaults to no limit)
    :param batch_size_in: maximum number of tuples to read in each batch (defaults to no limit)
    :param batch_size_out: maximum number of tuples to stream in each batch (defaults to no limit)
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param errors: incidental error messages (may be a non-empty list)
    """
    # initialize a local errors list
    curr_errors: list[str] = []

    # assert the database engine
    engine_id, engine_type = _assert_engine(engine,
                                            errors=errors)

    # make sure to have a connection to the source database
    curr_conn: Any = connection or db_connect(engine=engine_id,
                                              errors=curr_errors)
    if not curr_errors:
        # normalize these parameters
        offset_count = __normalize_value(offset_count)
        limit_count = __normalize_value(limit_count)
        batch_size_in = __normalize_value(batch_size_in)
        batch_size_out = __normalize_value(batch_size_out)
        if 0 < limit_count < batch_size_in:
            batch_size_in = limit_count
            logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
            if logger:
                logger.debug(msg=f"Value of input batch size set to {batch_size_in}")
        if 0 < batch_size_in < batch_size_out:
            batch_size_out = batch_size_in
            logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
            if logger:
                logger.debug(msg=f"Value of output batch size set to {batch_size_out}")

        # buid the SELECT query
        sel_stmt: str = f"SELECT {', '.join(columns)} FROM {table}"
        if where_clause:
            sel_stmt += f" WHERE {where_clause}"
        if orderby_clause:
            sel_stmt += f" ORDER BY {orderby_clause}"
        elif offset_count or batch_size_in:
            logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
            if logger:
                logger.warning(msg="Attempting partial data migration without "
                                   "providing an ORDER BY clause is not recommended")
        sel_stmt += " OFFSET @offset ROWS"

        if limit_count or batch_size_in:
            if engine_type == DbEngine.POSTGRES:
                sel_stmt += " LIMIT @limit"
            elif engine_type in [DbEngine.ORACLE, DbEngine.SQLSERVER]:
                sel_stmt += " FETCH @limit ROWS ONLY"

        # log the streaming start
        logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
        if logger:
            logger.debug(msg=f"Started streaming data from {engine}.{table}, limit "
                             f"{limit_count}, offset {offset_count}, batch size in {batch_size_in}, "
                             f"batch size out {batch_size_out}, connection {id(curr_conn)}")
        # stream the tuples
        row_count: int = 0
        stream_count: int = 0  # number of tuples effectively streamed
        err_msg: str | None = None
        try:
            source_cursor: Any = curr_conn.cursor()

            # adjust the offset and limit
            curr_stmt: str = sel_stmt
            curr_limit: int = min(batch_size_in, limit_count)
            if curr_limit == 0:
                curr_limit = max(batch_size_in, limit_count)
            if curr_limit:
                curr_stmt = curr_stmt.replace("@limit", str(curr_limit), 1)
            else:
                curr_stmt = curr_stmt.replace(" LIMIT @limit", "", 1)
                curr_stmt = curr_stmt.replace(" FETCH @limit ROWS ONLY", "", 1)
            if offset_count:
                curr_stmt = curr_stmt.replace("@offset", str(offset_count), 1)\
                                     .replace(" FETCH ", " FETCH NEXT ", 1)
            else:
                curr_stmt = curr_stmt.replace(" OFFSET @offset ROWS", "", 1)\
                                     .replace(" FETCH ", " FETCH FIRST ", 1)
            source_cursor.execute(curr_stmt)
            rows_in: list[tuple] = source_cursor.fetchall()

            # traverse the result set
            while rows_in:
                # log the retrieval operation
                logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
                if logger:
                    logger.debug(msg=f"Read {len(rows_in)} tuples from {engine}.{table}, "
                                     f"offset {offset_count + stream_count}, "
                                     f"connection {id(curr_conn)}")
                pos_from: int = 0

                # migrate the tuples
                while pos_from < len(rows_in):
                    pos_to: int = min(pos_from + batch_size_out, len(rows_in)) \
                                  if batch_size_out else len(rows_in)
                    rows_out: list[tuple] = rows_in[pos_from:pos_to]
                    yield rows_out
                    pos_from = pos_to

                    # increment the tuple streaming counter and log the partial streaming
                    stream_count += len(rows_out)
                    logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
                    if logger:
                        logger.debug(msg=f"Streamed {stream_count} tuples "
                                         f"from {engine}.{table}, connection {id(curr_conn)}")
                # read the next batch
                if limit_count > stream_count or (batch_size_in and not limit_count):
                    curr_limit = min(batch_size_in, limit_count - stream_count)
                    if curr_limit <= 0:
                        curr_limit = max(batch_size_in, limit_count - stream_count)
                    curr_stmt = sel_stmt.replace("@offset", str(offset_count + stream_count), 1)\
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
            if committable or not connection:
                with suppress(Exception):
                    curr_conn.rollback()
            row_count = 0
            err_msg = _except_msg(exception=e,
                                  engine_id=engine_id,
                                  connection=curr_conn)
        finally:
            # close the connections, if locally acquired
            if curr_conn and not connection:
                db_close(connection=curr_conn,
                         engine=engine_id)

        # log the streaming finish
        if err_msg:
            curr_errors.append(err_msg)
            logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
            if logger:
                logger.error(msg=err_msg)
        elif not curr_errors:
            logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
            if logger:
                logger.debug(msg=f"Finished streaming {row_count} tuples "
                                 f"from {engine}.{table}, connection {id(curr_conn)}")

    if curr_errors and isinstance(errors, list):
        errors.extend(curr_errors)


def db_stream_lobs(table: str,
                   lob_column: str,
                   pk_columns: list[str] = None,
                   ret_column: str = None,
                   where_clause: str = None,
                   orderby_clause: str = None,
                   offset_count: int = None,
                   limit_count: int = None,
                   batch_size: int = None,
                   chunk_size: int = None,
                   engine: DbEngine | str = None,
                   connection: Any = None,
                   committable: bool = None,
                   errors: list[str] = None,
                   log_trigger: int = 10000) -> Generator[dict[str, Any] | str | bytes | None, None, None]:
    """
    Stream data in large binary objects (LOBs) from a database table.

    This is accomplished with the implementation of the *generator* pattern, whereby an *iterator*
    is returned, allowing the invoker to iterate over the values being streamed.
    The database in *engine* must be in the list of databases configured and supported by this package.
    Either *orderby_clause* or one or more columns making up a primary key, or a unique row identifier,
    must be provided in *pk_columns*. The content of these columns, along with the contents of the
    optional *ret_column*, are returned as metadata on the first data chunk of each tuple.

    If not positive integers, *offset_count*, *limit_count*, *batch_size*, and *chunk_size* are ignored.
    Further, if *batch_size* and *limit_count* are both defined, and *batch_size* is set to a larger value,
    it is reduced accordingly.

    Care should be exercised when specifying *offset_count*, so as not to skip wanted tuples, as it is used
    to offset the retrieval of tuples. Finally, if *batch_size* or *offset_count* has been specified,
    but *orderby_clause* has not, then an ORDER BY clause is constructed from the data in *pk_columns*.

    The parameter *committable* defines whether a commit or rollback is performed on the provided *connection*.

    :param table: the table holding the LOBs
    :param lob_column: the column holding the LOB
    :param pk_columns: columns making up a primary key, or a unique identifier for a tuple, in database
    :param ret_column: optional column whose content to return as metadata when yielding
    :param where_clause: the criteria for tuple selection
    :param orderby_clause: optional retrieval order
    :param offset_count: number of tuples to skip in source table (defaults to none)
    :param limit_count: maximum number of tuples to migrate (defaults to no limit)
    :param batch_size: maximum number of tuples to read in each batch (defaults to no limit)
    :param chunk_size: size in bytes of the data chunk to read/write (defaults to no limit)
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param connection: optional connection to use (obtains a new one, if not provided)
    :param committable: whether to commit upon errorless completion
    :param errors: incidental error messages
    :param log_trigger: number of tuples to trigger logging info on migration (defaults to 10000 tuples)
    """
    # initialize a local errors list
    curr_errors: list[str] = []

    # assert the database engine
    engine_id, engine_type = _assert_engine(engine,
                                            errors=errors)

    # make sure to have a connection to the source database
    curr_conn: Any = connection or db_connect(engine=engine_id,
                                              errors=curr_errors)
    if not curr_errors:
        # normalize these parameters
        offset_count = __normalize_value(offset_count)
        limit_count = __normalize_value(limit_count)
        batch_size = __normalize_value(batch_size)
        chunk_size = __normalize_value(chunk_size)
        log_trigger = __normalize_value(log_trigger)
        if 0 < limit_count < batch_size:
            batch_size = limit_count
            logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
            if logger:
                logger.debug(msg=f"Value of batch size set to {batch_size}")

        # buid the SELECT query
        ref_columns: list[str] = pk_columns.copy() if pk_columns else []
        lob_index: int = len(pk_columns)
        sel_stmt: str = f"SELECT {', '.join(pk_columns)}"
        if ret_column and ret_column not in ref_columns:
            sel_stmt += f", {ret_column}"
            lob_index += 1
            ref_columns.append(ret_column)
        sel_stmt += f", {lob_column} FROM {table}"
        if where_clause:
            sel_stmt += f" WHERE {where_clause}"
        if not orderby_clause and pk_columns and (offset_count or batch_size):
            orderby_clause = ", ".join(pk_columns)
        if orderby_clause:
            sel_stmt += f" ORDER BY {orderby_clause}"
        elif offset_count or batch_size:
            logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
            if logger:
                logger.warning(msg="Attempting partial LOB migration without providing "
                                   "PK columns or an ORDER BY clause is not recommended")
        sel_stmt += " OFFSET @offset ROWS"

        if limit_count or batch_size:
            if engine_type == DbEngine.POSTGRES:
                sel_stmt += " LIMIT @limit"
            elif engine_type in [DbEngine.ORACLE, DbEngine.SQLSERVER]:
                sel_stmt += " FETCH @limit ROWS ONLY"

        # log the migration start
        logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
        if logger:
            logger.debug(msg="Started streaming LOBs "
                             f"from {engine}.{table}.{lob_column}, limit {limit_count}, "
                             f"offset {offset_count}, batch size {batch_size}, "
                             f"chunk size {chunk_size}, connection {id(curr_conn)}")
        # stream the LOBs
        byte_count: int = 0
        byte_step: int = 0
        row_count: int = 0
        row_step: int = 0
        start_count: float = time.time()
        start_step: float = time.time()
        err_msg: str | None = None
        try:
            source_cursor: Any = curr_conn.cursor()

            # adjust the offset and limit
            curr_stmt: str = sel_stmt
            curr_limit: int = min(batch_size, limit_count)
            if curr_limit == 0:
                curr_limit = max(batch_size, limit_count)
            if curr_limit:
                curr_stmt = curr_stmt.replace("@limit", str(curr_limit), 1)
            else:
                curr_stmt = curr_stmt.replace(" LIMIT @limit", "", 1)
                curr_stmt = curr_stmt.replace(" FETCH @limit ROWS ONLY", "", 1)
            if offset_count:
                curr_stmt = (curr_stmt.replace("@offset", str(offset_count), 1)
                                      .replace(" FETCH ", " FETCH NEXT ", 1))
            else:
                curr_stmt = (curr_stmt.replace(" OFFSET @offset ROWS", "", 1)
                                      .replace(" FETCH ", " FETCH FIRST ", 1))
            # go for the data
            next_rs: bool = True
            while next_rs:
                next_rs = False

                # execute the query
                # (parameter name is 'statement' for Oracle, 'query' for PostgreSQL, 'sql' for SQLServer)
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
                    yield identifier

                    # access the LOB's bytes in chunks and stream them
                    first: bool = True
                    offset: int = 1
                    is_migrated: bool = False
                    lob: Any = row[lob_index]
                    lob_data: bytes | str = __lob_get_chunk(lob,
                                                            offset=offset,
                                                            size=chunk_size,
                                                            engine_type=engine_type)
                    # skip null LOBs, migrate empty ones
                    while lob_data is not None and (first or len(lob_data) > 0):
                        first = False
                        is_migrated = True
                        # send a data chunk
                        yield lob_data

                        # read the next chunk
                        if len(lob_data) > 0:
                            offset += len(lob_data)
                            lob_data = __lob_get_chunk(lob,
                                                       offset=offset,
                                                       size=chunk_size,
                                                       engine_type=engine_type)
                    if is_migrated:
                        # increment the LOB migration counter, if applicable
                        byte_count += offset - 1
                        byte_step += offset - 1
                        row_count += 1
                        row_step += 1

                    # signal that sending data chunks for the current LOB is finished
                    yield None

                    # log partial result at each 'log_trigger' LOBs migrated
                    if row_step >= log_trigger:
                        finish_step: float = time.time()
                        logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
                        if logger:
                            mins: float = (finish_step - start_step) / 60
                            duration: str = timestamp_duration(start=start_step,
                                                               finish=finish_step)
                            logger.debug(msg=f"Streamed {row_step} LOBs, {byte_step} bytes, in "
                                             f"{duration} ({row_step/mins:.2f} LOBs/min, "
                                             f"{byte_step/(mins * 1024 ** 2):.2f} MBytes/min),  "
                                             f"from {engine}.{table}.{lob_column}, "
                                             f"connection {id(curr_conn)} "
                                             f"offset {offset_count + row_count - row_step}")
                        byte_step = 0
                        row_step = 0
                        start_step = finish_step

                    # retrieve the next row
                    row = source_cursor.fetchone()

                # adjust the new offset and limit
                if next_rs:
                    if limit_count > row_count or (batch_size and not limit_count):
                        curr_limit = min(batch_size, limit_count - row_count)
                        if curr_limit <= 0:
                            curr_limit = max(batch_size, limit_count - row_count)
                        curr_stmt = sel_stmt.replace("@offset", str(offset_count + row_count), 1)\
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
            if committable or not connection:
                with suppress(Exception):
                    curr_conn.rollback()
            err_msg = _except_msg(exception=e,
                                  engine_id=engine_id,
                                  connection=curr_conn)
        finally:
            # close the connections, if locally acquired
            if curr_conn and not connection:
                db_close(connection=curr_conn,
                         engine=engine)
        # log the finish
        if err_msg:
            curr_errors.append(err_msg)
            logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
            if logger:
                logger.error(msg=err_msg)
        elif not curr_errors:
            logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
            if logger:
                finish_count: float = time.time()
                mins: float = (finish_count - start_count) / 60
                duration: str = timestamp_duration(start=start_count,
                                                   finish=finish_count)
                logger.debug(msg=f"Finished streaming {row_count} LOBs, {byte_count} bytes, "
                                 f"in {duration} ({row_count/mins:.2f} LOBs/min, "
                                 f"{byte_count/(mins * 1024 ** 2):.2f} MBytes/min), "
                                 f"from {engine}.{table}.{lob_column}, "
                                 f"offset {offset_count}, connection {id(curr_conn)}")

    if curr_errors and isinstance(errors, list):
        errors.extend(curr_errors)


def __normalize_value(_v: int,
                      /) -> int:
    """
    Normalize the input value to a consistent value.

    :param _v: the value to normalized
    :return: the normalized value
    """
    if isinstance(_v, int) and \
            not isinstance(_v, bool) and _v > 0:
        result: int = _v
    else:
        result: int = 0

    return result


def __lob_get_chunk(_lob: Any,
                    /,
                    offset: int,
                    size: int,
                    engine_type: DbEngine) -> Any:

    result: bytes | str | None = None

    if offset and size:
        match engine_type:
            case DbEngine.ORACLE:
                # HAZARD: offset in Oracle is 1-based
                result = _lob.read(amount=size,
                                   offset=offset + 1)
            case DbEngine.POSTGRES:
                _lob.seek(offset)
                result = _lob.read(size=size)
            case DbEngine.SQLSERVER:
                result = _lob[offset:offset + size]
    elif offset:
        match engine_type:
            case DbEngine.ORACLE:
                # HAZARD: offset in Oracle is 1-based
                result = _lob.read(offset=offset + 1)
            case DbEngine.POSTGRES:
                _lob.seek(offset)
                result = _lob.read()
            case DbEngine.SQLSERVER:
                result = _lob[offset:]
    elif size:
        match engine_type:
            case DbEngine.ORACLE:
                result = _lob.read(amount=size)
            case DbEngine.POSTGRES:
                result = _lob.read(size=size)
            case DbEngine.SQLSERVER:
                result = _lob[:size]
    else:
        match engine_type:
            case DbEngine.ORACLE:
                result = _lob.read()
            case DbEngine.POSTGRES:
                result = _lob.read()
            case DbEngine.SQLSERVER:
                result = _lob

    return result

