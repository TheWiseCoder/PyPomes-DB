from __future__ import annotations  # allow forward references
from contextlib import suppress
from datetime import datetime, UTC
from enum import StrEnum, auto
from logging import Logger
from pypomes_core import (
    APP_PREFIX, env_get_int, env_get_bool, str_positional, str_sanitize
)
from sys import getrefcount
from time import sleep
from threading import Lock
from typing import Any, Final

from .db_common import DbEngine, _assert_engine


class DbPoolEvent(StrEnum):
    """
    Pool events for tuning and monitoring.
    """
    CREATE = auto()
    CHECKOUT = auto()
    CHECKIN = auto()
    CLOSE = auto()


class _PoolParam(StrEnum):
    """
    Parameters for configuring the connection pool.
    """
    SIZE = auto()
    TIMEOUT = auto()
    RECYCLE = auto()
    VERIFY = auto()


class _ConnStage(StrEnum):
    """
    Stage data of connections in pool.
    """
    CONNECTION = auto()                 # the connection object
    TIMESTAMP = auto()                  # creation datetime (POSIX timestamp)
    AVAILABLE = auto()                  # status (True: available; False: taken)


# available DbConnectionPool instances:
# {
#    <DbEngine>: <DbConnectionPool>,
#    ...
# }
_POOL_INSTANCES: Final[dict[DbEngine, DbConnectionPool]] = {}

# pool instances access lock
_instances_lock: Final[Lock] = Lock()


def __get_pool_params() -> dict[DbEngine, dict[_PoolParam, Any]]:
    """
    Establish the connection pool parameters for supported databases, from values in environment variables.

    To specify database connection parameters with environment variables, use the set:
      - *<APP_PREFIX>_DB_POOL_SIZE*: maximum number of connections in the pool (defaults to 40 connections)
      - *<APP_PREFIX>_DB_POOL_TIMEOUT*: number of seconds to wait for a connection to become available,
        before failing the request (defaults to 60 seconds)
      - *<APP_PREFIX>_DB_POOL_RECYCLE*: number of seconds after which connections in the pool are closed
        (defaults to 3600 seconds)
      - *<APP_PREFIX>_DB_POOL_VERIFY*: ensures the connection is still active and healthy before it is
        checked out from the pool, by executing a lightweight query - if the verification fails,
        the connection is discarded (defaults to *True*)

    These variables apply to any of the supported database engines. To specify values for a specific engine,
    replace *_DB_* with *_MSQL_*, *_ORCL_*, *_PG_*, or *_SQLS_*, respectively for *mysql*, *oracle*,
    *postgres*, and *sqlserver*.

    :return: the connection parameters for supported database engines
    """
    # initialize the return variable
    result: dict[DbEngine, dict[_PoolParam, Any]] = {}

    for engine in DbEngine:
        prefix: str = str_positional(engine,
                                     keys=tuple(DbEngine),
                                     values=("MSQL", "ORCL", "PG", "SQLS"))
        # establish pool size
        size: int = env_get_int(key=f"{APP_PREFIX}_{prefix}_POOL_SIZE")
        if not isinstance(size, int) or size < 0:
            size = env_get_int(key=f"{APP_PREFIX}_DB_POOL_SIZE",
                               def_value=40)
        # establish pool timeout
        timeout: int = env_get_int(key=f"{APP_PREFIX}_{prefix}_POOL_TIMEOUT")
        if not timeout:
            timeout = env_get_int(key=f"{APP_PREFIX}_DB_POOL_TIMEOUT",
                                  def_value=60)
        # establish pool recycle
        recycle: int = env_get_int(key=f"{APP_PREFIX}_{prefix}_POOL_RECYCLE")
        if not recycle:
            recycle = env_get_int(key=f"{APP_PREFIX}_DB_POOL_RECYCLE",
                                  def_value=3600)
        # establish connection verification
        verify: bool = env_get_bool(key=f"{APP_PREFIX}_{prefix}_POOL_VERIFY")
        if verify is None:
            verify = env_get_bool(key=f"{APP_PREFIX}_DB_POOL_VERIFY",
                                  def_value=True)
        result[engine] = {
            _PoolParam.SIZE: size,
            _PoolParam.TIMEOUT: timeout,
            _PoolParam.RECYCLE: recycle,
            _PoolParam.VERIFY: verify
        }

    return result


# DbConnectionPool instance params:
# {
#    <DbEngine>: {
#       <_PoolParam.RECYCLE>: <int>,
#       <_PoolParam.SIZE>: <int>,
#       <_PoolParam.TIMEOUT>: <int>,
#       <_PoolParam.VERIFY>: <bool>
#    },
#    ...
# }
_POOL_PARAMS: Final[dict[DbEngine, dict[_PoolParam, Any]]] = __get_pool_params()


class DbConnectionPool:
    """
    A robust, transparent, and efficient implementation of a database connection pool.

    This is a mechanism to manage database connections efficiently by reusing them, instead of their being
    repeatedly created and closed. As a result, overall performance is improved, latency is reduced, and
    resource usage is optimized, especially in applications with frequent database interactions.
    The connections are lazily created, as demand for connections arise and cannot be met by the stock
    already in the pool.

    This implementation follows the best practices in the industry, mirroring state-of-the-art products
    such as the *SQLAlchemy* pool package. These are the configuration parameters:
      - *pool_size*: maximum number of connections in the pool
      - *pool_timeout*: number of seconds to wait for a connection to become available, before failing the request
      - *pool_recycle*: number of seconds after which connections in the pool are closed
      - *pool_verify*: ensures the connection is still active and healthy before it is checked out from the pool

    These are the events that a pool client may hook up to, with a call to *on_event_actions()*, by
    specifying a callback function to be invoked, and/or SQL commands to be executed:
      - *create*: when a connection is created, allowing for connection session settings customization
      - *checkout*: when a connection is retrieved from the pool, allowing for fine-tuning the connection settings
      - *checkin*: when a connection is returned to the pool, allowing for cleanup of session state
      - *close*: when a connection is closed, allowing for resources cleanup and connection life cycle auditing

    The modules handling the native database operations (namely, *mysql_pomes.py*, *oracle_pomes.py*,
    *postgres_pomes.py*, and *sqlserver_pomes.py*) have no awareness of this pool. The higher level module
    *db_pomes.py* will attempt to obtain a connection from this pool on *db_connect()*, and to return it
    on *db_close()*. It is worth emphasizing that a *close()* operation should not be invoked directly
    on the native connection, as this would prevent it from being reclaimed, and cause it to be eventually
    discarded, thus defeating the very purpose of the pool.
    """
    def __init__(self,
                 engine: DbEngine = None,
                 pool_size: int = None,
                 pool_timeout: int = None,
                 pool_recycle: int = None,
                 pool_verify: bool = None,
                 errors: list[str] = None,
                 logger: Logger = None) -> None:
        """
        Construct a connection pool specific for the database provided in *engine*.

        The database engine specified must have been previously configured. If not provided, the values
        for pool configuration parameters specified in the environment variables suffixed with their
        uppercase names (namely, *POOL_SIZE*, *POOL_TIMEOUT*, *POOL_RECYCLE*, and *POOL_VERIFY*) are used.
        If still not specified, these are the default values used:
          - *pool_size*: 40 connections
          - *pool_timeout*: 60 seconds
          - *pool_recycle*: 3600 seconds (60 minutos)
          - *pool_verify*: True

        All instance attributes are final and should not be directly changed, lest the pool malfunction.

        :param engine: the database engine to use (uses the default engine, if not provided)
        :param pool_size: number of connections to keep in the pool
        :param pool_timeout: number of seconds to wait for an available connection before failing
        :param pool_recycle: number of seconds after which connections in the pool are closed and reopened
        :param pool_verify: ensures the connection is still active and healthy before it is checked out
        :param errors: incidental error messages
        :param logger: optional logger
        """
        # make sure a configured databasee engine has been specified
        engine = _assert_engine(engine=engine,
                                errors=errors)

        # obtain the default values for the pool parameters
        pool_params: dict[_PoolParam, Any] | None = None
        if engine:
            with _instances_lock:
                if engine in _POOL_INSTANCES:
                    msg: str = f"Attempt to create connection pool failed: {engine} pool already exists"
                    if logger:
                        logger.error(msg)
                    if isinstance(errors, list):
                        errors.append(msg)
                else:
                    pool_params = _POOL_PARAMS[engine]

        if pool_params:
            if not isinstance(pool_size, int):
                pool_size = pool_params[_PoolParam.SIZE]
            if pool_size > 0:
                self.db_engine: Final[DbEngine] = engine
                self.pool_size: Final[int] = pool_size
                self.pool_timeout: Final[int] = pool_timeout if isinstance(pool_timeout, int) \
                    else pool_params[_PoolParam.TIMEOUT]
                self.pool_recycle: Final[int] = pool_recycle if isinstance(pool_recycle, int) \
                    else pool_params[_PoolParam.RECYCLE]
                self.pool_verify: Final[bool] = pool_verify if isinstance(pool_verify, bool) \
                    else pool_params[_PoolParam.VERIFY]
                self.stage_lock: Final[Lock] = Lock()
                self.event_lock: Final[Lock] = Lock()

                self.event_callbacks: Final[dict[DbPoolEvent, callable]] = {
                    DbPoolEvent.CREATE: None,
                    DbPoolEvent.CHECKOUT: None,
                    DbPoolEvent.CHECKIN: None,
                    DbPoolEvent.CLOSE: None
                }
                self.event_stmts: Final[dict[DbPoolEvent, list[str]]] = {
                    DbPoolEvent.CREATE: [],
                    DbPoolEvent.CHECKOUT: [],
                    DbPoolEvent.CHECKIN: [],
                    DbPoolEvent.CLOSE: []
                }
                self.conn_data: Final[list[dict[_ConnStage, Any]]] = []

                # register this instance
                _POOL_INSTANCES[engine] = self
                if logger:
                    logger.info(msg=f"{self.db_engine} pool created: size {self.pool_size}, "
                                    f"timeout {self.pool_timeout}, recycle {self.pool_recycle}")
            else:
                msg: str = f"{engine} pool not created: specified size was {pool_size}"
                if logger:
                    logger.error(msg=msg)
                    if isinstance(errors, list):
                        errors.append(msg)

    def connect(self,
                start: float = None,
                errors: list[str] = None,
                logger: Logger = None) -> Any | None:
        """
        Obtain a pooled connection.

        :param start: start of operation
        :param errors: incidental error messages
        :param logger: optional logger
        :return: a connection from the pool, or *None* if error
        """
        # initialize the return variable
        result: Any = None

        if not isinstance(errors, list):
            errors = []
        if not isinstance(start, float):
            start = datetime.now(tz=UTC).timestamp()

        with self.stage_lock:
            while not errors and not result:
                reclaimed: bool = False
                # traverse the connection data in reverse order
                for i in range(len(self.conn_data) - 1, -1, -1):
                    conn: Any = self.conn_data[i][_ConnStage.CONNECTION]

                    # connection is available
                    if self.conn_data[i][_ConnStage.AVAILABLE]:
                        # connection exhausted its lifetime
                        if datetime.now(tz=UTC).timestamp() > \
                                self.conn_data[i][_ConnStage.TIMESTAMP] + self.pool_recycle:
                            # close the exhausted connection
                            self.__act_on_event(event=DbPoolEvent.CLOSE,
                                                conn=conn,
                                                logger=logger)
                            with suppress(Exception):
                                conn.close()
                            # dispose of exhausted connection
                            self.conn_data.pop(i)
                            if logger:
                                logger.debug(msg=f"The exhausted connection {id(conn)} was removed from the "
                                                 f"{self.db_engine} pool, count = {len(self.conn_data)}")
                        # assess the connection
                        else:
                            if self.pool_verify:
                                # assert connection health
                                stmt = "SELECT 1"
                                if self.db_engine == DbEngine.ORACLE:
                                    stmt += " FROM DUAL"
                                try:
                                    cursor: Any = conn.cursor()
                                    # parameter name:
                                    #   - mysql-connector-python (mysql): none
                                    #   - oracledb (oracle): 'statement'
                                    #   - psycopg2 (postgres): 'query'
                                    #   - pyodbc (sqlserver): 'sql'
                                    cursor.execute(stmt)
                                    cursor.fetchone()
                                    cursor.close()
                                except Exception as e:
                                    # dispose of bad connection
                                    self.conn_data.pop(i)
                                    conn = None
                                    if logger:
                                        logger.warning(f"Connection {id(conn)} "
                                                       f"assessment failed: {str_sanitize(f'{e}')}")
                                        logger.debug(msg=f"The bad connection {id(conn)} was removed from the "
                                                         f"{self.db_engine} pool, count = {len(self.conn_data)}")
                            if conn:
                                # retrieve the connection, and mark it as taken
                                result = conn
                                self.conn_data[i][_ConnStage.AVAILABLE] = False
                                break

                    # connection was closed elsewhere
                    elif hasattr(conn, "closed") and conn.closed:
                        # dispose of closed connection
                        self.conn_data.pop(i)
                        if logger:
                            logger.debug(msg=f"The closed connection {id(conn)} was removed from the "
                                             f"{self.db_engine} pool, count = {len(self.conn_data)}")

                    # connection is no longer in use - these are the 3 remaining references:
                    #   - pool repository 'self.conn_data[i][_ConnStage.CONNECTION]'
                    #   - local variable 'conn'
                    #   - argument to 'getrefcount()'
                    elif getrefcount(conn) < 4:
                        # reclaim the connection
                        self.__act_on_event(event=DbPoolEvent.CHECKIN,
                                            conn=conn,
                                            logger=logger)
                        # mark the connection as available
                        self.conn_data[i][_ConnStage.AVAILABLE] = True
                        reclaimed = True
                        if logger:
                            logger.debug(msg=f"The stray connection {id(conn)} "
                                             f"was reclaimed by the {self.db_engine} pool")
                # end of conn data traversal
                if not result and not reclaimed:
                    # no connection retrieved or reclaimed, obtain a new one
                    if len(self.conn_data) < self.pool_size:
                        match self.db_engine:
                            case DbEngine.MYSQL:
                                from . import mysql_pomes
                                conn = mysql_pomes.connect(autocommit=False,
                                                           errors=errors,
                                                           logger=logger)
                            case DbEngine.ORACLE:
                                from . import oracle_pomes
                                conn = oracle_pomes.connect(autocommit=False,
                                                            errors=errors,
                                                            logger=logger)
                            case DbEngine.POSTGRES:
                                from . import postgres_pomes
                                conn = postgres_pomes.connect(autocommit=False,
                                                              errors=errors,
                                                              logger=logger)
                            case DbEngine.SQLSERVER:
                                from . import sqlserver_pomes
                                conn = sqlserver_pomes.connect(autocommit=False,
                                                               errors=errors,
                                                               logger=logger)
                        if not errors:
                            self.__act_on_event(event=DbPoolEvent.CREATE,
                                                conn=conn,
                                                logger=logger)
                            # retrieve and store the connection
                            result = conn
                            self.conn_data.append({
                                _ConnStage.AVAILABLE: False,
                                _ConnStage.CONNECTION: result,
                                _ConnStage.TIMESTAMP: datetime.now(tz=UTC).timestamp()
                            })
                            if logger:
                                logger.debug(msg=f"Connection {id(result)} created by the "
                                                 f"{self.db_engine} pool, count = {len(self.conn_data)}")
                    else:
                        # connection pool is at capacity
                        break
        if not errors:
            if result:
                # connection retrieved
                self.__act_on_event(event=DbPoolEvent.CHECKOUT,
                                    conn=result,
                                    logger=logger)
                if logger:
                    logger.debug(msg=f"Connection {id(result)} "
                                     f"delivered by the {self.db_engine} pool")
            elif datetime.now(tz=UTC).timestamp() < start + self.pool_timeout:
                # no connection retrieved, there is still time to retry
                sleep(1.5)
                result = self.connect(start=start,
                                      errors=errors,
                                      logger=logger)
            else:
                errors.append("Timeout waiting for available connection")

        return result

    def on_event_actions(self,
                         event: DbPoolEvent,
                         callback: callable = None,
                         stmts: list[str] = None) -> None:
        """
        Specify a callback function to be invoked, and/or SQL commands to be executed, when *event* occurs.

        The possible events are:
          - *PoolEvent.CREATE*: a connection is created and stored in the pool
          - *PoolEvent.CHECKOUT*: a connection is retrieved from the pool
          - *PoolEvent.CHECKIN*: a connection is returned to the pool
          - *PoolEven.CLOSE*: a connection is closed and removed from the pool

        If *callback* is not specified, any current callback is removed for *event*, otherwise it will be
        invoked with the following parameters:
          - *connection*: the native connection obtained from the underlying database driver
          - *logger*: the logger in use by the operation that raised the event (may be *None*)

        If *stmts* is not specified, any current statements are removed for *event*, otherwise it holds
        one or more SQL commands to be executed, aiming to, among others, and depending on the event:
          - initialize, set, reset, or cleanup the session state
          - set encoding, timezone, and sort order
          - verify connection health
          - audit connection life cycle
          - cleanup resources
          - log usage to database

        :param event: the reference event
        :param callback: the function to be invoked
        :param stmts: optional list of SQL commands to be executed
        """
        with self.event_lock:
            # register the event hook
            self.event_callbacks[event] = callback
            # register the SQL commands
            self.event_stmts[event] = stmts

    def reclaim(self,
                conn: Any,
                logger: Logger = None) -> bool:
        """
        Reclaim the connection given in *conn*, allowing for its reuse.

        :param conn: the connection to be reclaimed
        :param logger: optional logger
        :return: *True* if *conn* was successfully reclaimed, *False* otherwise
        """
        # initialize the return variable
        result: bool = False

        with self.stage_lock:
            for data in self.conn_data:
                if data[_ConnStage.CONNECTION] == conn:
                    self.__act_on_event(event=DbPoolEvent.CHECKIN,
                                        conn=conn,
                                        logger=logger)
                    # mark the connection as available
                    data[_ConnStage.AVAILABLE] = True
                    result = True
                    if logger:
                        logger.debug(msg=f"Connection {id(conn)} "
                                         f"returned to the {self.db_engine} pool")
                    break

        return result

    def terminate(self,
                  logger: Logger = None) -> None:
        """
        Terminate the pool, releasing all held resources.

        Upon termination, all references to this instance should be disposed of,
        as invocation of any of its functions is guaranteed to fail.
        """
        with _instances_lock:
            _POOL_INSTANCES.pop(self.db_engine)

        with self.stage_lock:
            self.conn_data.clear()

        with self.event_lock:
            self.event_callbacks.clear()
            self.event_stmts.clear()

        if logger:
            logger.debug(msg=f"{self.db_engine} pool terminated")

    def __act_on_event(self,
                       event: DbPoolEvent,
                       conn: Any,
                       logger: Logger = None) -> None:
        """
        Act on *event*, by invoking the hooked callback function, and/or executing the registered SQL statements.

        The possible events are:
          - *PoolEvent.CREATE*: a connection is created and stored in the pool
          - *PoolEvent.CHECKOUT*: a connection is retrieved from the pool
          - *PoolEvent.CHECKIN*: a connection is returned to the pool
          - *PoolEven.CLOSE*: a connection is closed and removed from the pool

        If a callback function has been hooked to *event*, it is invoked with the following parameters:
          - the native connection obtained from the underlying database driver, its type being one of
              - *mysql.connector.aio.MySQLConnectionAbstract* (mysql-connector-python)
              - *oracle.Connection* (oracledb)
              - *psycopg2._psycopg.connection* (psycopg2)
              - *pyodbc.connection* (pyodbc)
          - the logger in use by the operation that raised the event, its type being *logging.Logger* (may be *None*)

        :param event: the reference event
        :param conn: the reference connection
        :param logger: optional logger
        """
        with self.event_lock:
            # invoke the callback
            callback: callable = self.event_callbacks[event]
            if callback:
                callback(conn,
                         logger)
                if logger:
                    logger.debug(msg=f"Event '{event}' on {self.db_engine}, "
                                     f"connection {id(conn)}: '{callback.__name__}' invoked")
            # execute the statements
            if self.event_stmts[event]:
                from . import db_pomes
                for stmt in self.event_stmts[event]:
                    errors: list[str] = []
                    db_pomes.db_execute(exc_stmt=stmt,
                                        engine=self.db_engine,
                                        connection=conn,
                                        errors=errors,
                                        logger=logger)
                    if logger:
                        msg: str = (f"Event '{event}' on {self.db_engine}, "
                                    f"connection {id(conn)}: '{stmt}' executed")
                        if errors:
                            logger.error(msg=f"{msg}, errors {'; '.join(errors)}")
                        else:
                            logger.debug(msg=msg)


def db_get_pool(engine: DbEngine = None) -> DbConnectionPool | None:
    """
    Retrieve the instance of the connection pool associated with *engine*.

    :param engine: the database engine to use (uses the default engine, if not provided)
    :return: the connection pool for *engine*, or *None* if no connection has been retrieved
    """
    # initialize the return variable
    result: DbConnectionPool | None = None

    # assert the database engine
    engine = _assert_engine(engine=engine)
    if engine:
        with _instances_lock:
            result = _POOL_INSTANCES.get(engine)

    return result


def db_pool_acquire(engine: DbEngine = None,
                    logger: Logger = None) -> Any | None:
    """
    Obtain a pooled connection for *engine*.

    :param engine: the database engine to use (uses the default engine, if not provided)
    :param logger: optional logger
    :return: a pooled connection to *engine*, or *None* if it unknown or has not been configured
    """
    # initialize the return variable
    result: Any = None

    # assert the database engine
    engine = _assert_engine(engine=engine)
    if engine:
        pool: DbConnectionPool = db_get_pool(engine=engine)
        # obtain the pooled connection
        if pool:
            result = pool.connect(logger=logger)
    return result


def db_pool_release(conn: Any,
                    engine: DbEngine = None,
                    logger: Logger = None) -> bool:
    """
    Return *conn* to the pool, to allow it to be reused.

    :param conn: the connection to return to the pool
    :param engine: the database engine to use (uses the default engine, if not provided)
    :param logger: optional logger
    :return: *True* if *conn* was successfully reclaimed by the pool, *False* otherwise
    """
    # initialize the return variable
    result: bool = False

    # assert the database engine
    engine = _assert_engine(engine=engine)
    if engine:
        # reclaim the connection
        pool: DbConnectionPool = db_get_pool(engine=engine)
        if pool:
            result = pool.reclaim(conn=conn,
                                  logger=logger)
    return result
