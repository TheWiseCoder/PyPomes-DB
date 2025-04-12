from contextlib import suppress
from logging import Logger
from pypomes_logging import PYPOMES_LOGGER
from typing import Any

from .db_common import (
    DbEngine, _except_msg, _remove_nulls
)
from .db_pomes import (
    db_connect, db_bulk_delete, db_bulk_insert, db_bulk_update
)


# ruff: noqa: C901 PLR0912 PLR0915
#   C901:    complex-structure -   Checks for functions with a high McCabe complexity
#   PLR0912: too-many-branches -   Checks for functions or methods with too many branches
#   PLR0915: too-many-statements - Checks for functions or methods with too many statements
def db_sync_data(errors: list[str] | None,
                 source_engine: DbEngine,
                 source_table: str,
                 target_engine: DbEngine,
                 target_table: str,
                 pk_columns: list[str],
                 sync_columns: list[str],
                 source_conn: Any = None,
                 target_conn: Any = None,
                 source_committable: bool = None,
                 target_committable: bool = None,
                 identity_column: str = None,
                 batch_size: int = None,
                 has_nulls: bool = None,
                 logger: Logger = PYPOMES_LOGGER,
                 log_trigger: int = 10000) -> tuple[int, int, int] | None:
    """
    Synchronize data in *target_table*, as per the contents of *source_table*.

    The origin and destination databases must be in the list of databases configured and
    supported by this package. The specified database columns must not be type LOB
    (large binary object), and must have the same cardinality, and be of respectively
    equivalent types, in both the origin and the destination databases.

    Specific handling is required for identity columns (i.e., columns with values generated
    directly by the database - typically, they are also primary keys), and thus they must be
    identified by *identity_column*. For target Oracle databases, *GENERATED ALWAYS*
    identity columns should be ommited, as attempting to insert values therein raises an error,
    whereas *GENERATED BY DEFAULT AS IDENTITY* columns are acceptable.

    The parameter *has_nulls* indicates that one or more columns holding string content in the source
    table have embedded NULL characters. Some databases, particularly Postgres, do not accept NULLs
    embedded in text, so they must be removed.

    The parameters *source_committable* and *target_committable* are relevant only
    if *source_conn* and *target_conn* are provided, respectively, and are otherwise ignored.
    A rollback is always attempted, if an error occurs.

    :param errors: incidental error messages
    :param source_engine: the source database engine type
    :param source_table: the possibly schema-qualified table to read the data from
    :param target_engine: the destination database engine type
    :param target_table: the possibly schema-qualified table to write the data to
    :param pk_columns: columns making up a primary key, or a unique identifier for a tuple in both tables
    :param sync_columns: the columns to be synchronized (must not include PK columns)
    :param source_conn: the connection to the source database (obtains a new one, if not provided)
    :param target_conn: the connection to the destination database (obtains a new one, if not provided)
    :param source_committable: whether to commit operation on *source_conn* on errorless completion
    :param target_committable: whether to commit operation on *target_conn* on errorless completion
    :param identity_column: column whose values are generated by the database
    :param batch_size: the maximum number of tuples to synchronize in each batch, or 0 or None for no limit
    :param has_nulls: indicates that one or more string-type source columns have nulls
    :param logger: optional logger
    :param log_trigger: number of LOBs to trigger logging info on migration (defaults to 10000 LOBs)
    :return: the number of tuples effectively deleted, inserted, and updated in the target table, or *None* if error
    """
    # helper function to obtain a primary key tuple from a row
    def get_pk_data(row: tuple, pk_cols: list[str]) -> tuple:
        return tuple(row[pos] for pos in range(len(pk_cols)))

    # helper function to obtain a sync data tuple from a row
    def get_sync_data(row: tuple, pk_cols: list[str], sync_cols: list[str]) -> tuple:
        return tuple(row[pos] for pos in range(len(pk_cols), len(pk_cols) + len(sync_cols)))

    # initialize the return variable
    result: tuple[int, int, int] | None = (0, 0, 0)

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
        # buid the SELECT query
        all_cols: str = ", ".join(pk_columns + sync_columns)
        source_sel_stmt: str = (f"SELECT {all_cols} FROM {source_table} "
                                f"ORDER BY {', '.join(pk_columns)}")
        target_sel_stmt: str = (f"SELECT {all_cols} FROM {target_table} "
                                f"ORDER BY {', '.join(pk_columns)}")
        if batch_size:
            source_sel_stmt += " OFFSET @offset ROWS"
            target_sel_stmt += " OFFSET @offset ROWS"
            if source_engine == DbEngine.POSTGRES:
                source_sel_stmt += f" LIMIT {batch_size}"
            elif source_engine in [DbEngine.ORACLE, DbEngine.SQLSERVER]:
                source_sel_stmt += f" FETCH NEXT {batch_size} ROWS ONLY"
            if target_engine == DbEngine.POSTGRES:
                target_sel_stmt += f" LIMIT {batch_size}"
            elif target_engine in [DbEngine.ORACLE, DbEngine.SQLSERVER]:
                target_sel_stmt += f" FETCH NEXT {batch_size} ROWS ONLY"
        rows_for_delete: list[tuple] = []
        rows_for_insert: list[tuple] = []
        rows_for_update: list[tuple] = []

        # log the synchronization start
        if logger:
            logger.debug(msg=f"Started synchronizing data, "
                             f"from {source_engine}.{source_table} "
                             f"to {target_engine}.{target_table}")
        # migrate the data
        log_count: int
        log_step: int = 0
        err_msg: str | None = None
        try:
            # obtain the cursors
            source_cursor: Any = curr_source_conn.cursor()
            target_cursor: Any = curr_target_conn.cursor()

            # execute the queries
            # (parameter is 'statement' for oracle, 'query' for postgres, 'sql' for sqlserver)
            source_cursor.execute(statement=source_sel_stmt.replace("OFFSET @offset ROWS ", ""))
            source_rows: list[tuple] = source_cursor.fetchall()
            # log query result
            if logger:
                logger.debug(msg=f"Read {len(source_rows)} tuples "
                                 f"from {source_engine}.{source_table}")
            if has_nulls and target_engine == "postges":
                source_rows = _remove_nulls(rows=source_rows)
            source_offset: int = len(source_rows)
            target_cursor.execute(target_sel_stmt.replace("OFFSET @offset ROWS ", ""))
            target_rows: list[tuple] = target_cursor.fetchall()
            # log query result
            if logger:
                logger.debug(msg=f"Read {len(target_rows)} tuples "
                                 f"from {target_engine}.{target_table}")
            target_offset: int = len(target_rows)
            log_count = source_offset

            # traverse the result set
            source_row: tuple | None = None
            target_row: tuple | None = None
            source_pks: tuple | None = None
            target_pks: tuple | None = None
            while source_rows or target_rows:
                if source_rows and not source_row:
                    source_row = source_rows.pop(0)
                    source_pks = get_pk_data(row=source_row,
                                             pk_cols=pk_columns)
                if target_rows and not target_row:
                    target_row = target_rows.pop(0)
                    target_pks = get_pk_data(row=target_row,
                                             pk_cols=pk_columns)
                if source_row and \
                   (not target_row or source_pks < target_pks):
                    # flag row for insertion
                    rows_for_insert.append(source_row)
                    source_row = None
                elif target_row and \
                        (not source_row or target_pks < source_pks):
                    # flag row for deletion
                    rows_for_delete.append(target_pks)
                    target_row = None
                else:
                    source_data: tuple = get_sync_data(row=source_row,
                                                       pk_cols=pk_columns,
                                                       sync_cols=sync_columns)
                    target_data: tuple = get_sync_data(row=target_row,
                                                       pk_cols=pk_columns,
                                                       sync_cols=sync_columns)
                    if source_data != target_data:
                        # flag row for update
                        rows_for_update.append(source_data + source_pks)
                    source_row = None
                    target_row = None

                if source_offset and not source_rows:
                    source_cursor.execute(source_sel_stmt.replace("@offset", str(source_offset)))
                    source_rows = source_cursor.fetchall()
                    if source_rows:
                        # log query result
                        if logger:
                            logger.debug(msg=f"Read {len(source_rows)} tuples "
                                             f"from {source_engine}.{source_table}")
                        source_offset += len(source_rows)
                        log_count += source_offset
                        log_step += source_offset
                        if has_nulls and target_engine == DbEngine.POSTGRES:
                            source_rows = _remove_nulls(rows=source_rows)
                    else:
                        # no more tuples in source table
                        source_offset = 0

                if target_offset and not target_rows:
                    target_cursor.execute(target_sel_stmt.replace("@offset", str(target_offset)))
                    target_rows = target_cursor.fetchall()
                    if target_rows:
                        # log query result
                        if logger:
                            logger.debug(msg=f"Read {len(target_rows)} tuples "
                                             f"from {target_engine}.{target_table}")
                        target_offset += len(target_rows)
                    else:
                        # no more tuples in target table
                        target_offset = 0

                # log partial result
                if logger and log_step >= log_trigger:
                    logger.debug(msg=f"Synchronizing {log_count} tuples, "
                                     f"from {source_engine}.{source_table} "
                                     f"to {target_engine}.{target_table}")
                    log_step = 0
            # close the cursors
            source_cursor.close()
            target_cursor.close()

            # process the rows flagged for delete
            if rows_for_delete:
                # delete the rows
                db_bulk_delete(errors=op_errors,
                               target_table=target_table,
                               where_attrs=pk_columns,
                               where_vals=rows_for_delete,
                               engine=target_engine,
                               connection=curr_target_conn,
                               committable=False,
                               logger=logger)

            # process the rows flagged for update
            if not op_errors and rows_for_update:
                # update the rows
                db_bulk_update(errors=op_errors,
                               target_table=target_table,
                               set_attrs=sync_columns,
                               where_attrs=pk_columns,
                               update_vals=rows_for_update,
                               engine=target_engine,
                               connection=curr_target_conn,
                               committable=False,
                               logger=logger)

            # process the rows flagged for insert
            if not op_errors and rows_for_insert:
                # insert the rows
                db_bulk_insert(errors=op_errors,
                               target_table=target_table,
                               insert_attrs=pk_columns + sync_columns,
                               insert_vals=rows_for_insert,
                               engine=target_engine,
                               connection=curr_target_conn,
                               committable=False,
                               identity_column=identity_column,
                               logger=logger)
            if op_errors:
                log_count = 0
            else:
                if source_committable or not source_conn:
                    curr_source_conn.commit()
                if target_committable or not target_conn:
                    curr_target_conn.commit()
                result = (len(rows_for_delete), len(rows_for_insert), len(rows_for_update))

        except Exception as e:
            # rollback the transactions
            if curr_source_conn:
                with suppress(Exception):
                    curr_source_conn.rollback()
            if curr_target_conn:
                with suppress(Exception):
                    curr_target_conn.rollback()
            log_count = 0
            err_msg = _except_msg(exception=e,
                                  engine=source_engine)
            result = None
        finally:
            # close the connections, if locally acquired
            if curr_source_conn and not source_conn:
                curr_source_conn.close()
            if curr_target_conn and not target_conn:
                curr_target_conn.close()

        # log the synchronization finish
        if err_msg:
            op_errors.append(err_msg)
            if logger:
                logger.error(msg=err_msg)
        if logger:
            logger.debug(msg=f"Synchronized {log_count} tuples, "
                             f"from {source_engine}.{source_table} "
                             f"to {target_engine}.{target_table}")
    # acknowledge local errors
    if isinstance(errors, list):
        errors.extend(op_errors)

    return result
