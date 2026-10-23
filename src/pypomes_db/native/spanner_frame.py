import json
import sys
import time
from contextlib import suppress
from google.cloud import spanner
from google.cloud.spanner import Client
from google.cloud.spanner import AbstractSessionPool, PingingPool
from google.cloud.spanner_v1.database import Database, StreamedResultSet
from google.cloud.spanner_v1.instance import Instance
from google.cloud.spanner_v1.param_types import Type
from google.cloud.spanner_v1.session import Session
from google.cloud.spanner_v1.transaction import Transaction
from google.oauth2.service_account import Credentials
from io import BytesIO
from logging import Logger
from pathlib import Path
from pypomes_core import file_get_data, exc_format
from threading import Thread
from typing import Any

from ..db_common import DB_CONN_DATA, DbParam, SpannerParam


class GoogleSpanner:
    """
    An encapsulation of the Google Cloud Spanner access lifecycle.

    *Spanner* uses *Identity and Access Management* (IAM), rather than simple username/password strings, and thus
    the encapsulation replaces traditional RDBMS URL/User patterns with *GCP Resource Hierarchies*, handling
    the *Google Cloud*'s hierarchical structure, namely, **Project/Instance/Database*. The *google-cloud-spanner*
    library manages a connection pool under the hood, and to encapsulate this logic, this class takes care of the
    *Client* object, while providing access to a *Database* object.

    Extensive documentation on the *google-cloud-spanner* package can be found at
    https://docs.cloud.google.com/python/docs/reference/spanner/latest.

    These are the instance variables:
        - *engine_id*: the identification for the GoogleSpanner database instance
        - *client*: *google.cloud.spanner.Client* - the Cloud Spanner Client
        - *credentials*: *dict[str, str]* - the access credentials
        - *database*: *google.cloud.spanner_v1.database.Database* - database to use
        - *instance*: *google.cloud.spanner_v1.instance.Instance* - instance to use
        - *pinger*: *threading.Thread* - thread for background pinging
    """

    def __init__(self,
                 *,
                 engine_id: str,
                 errors: list[str] = None) -> None:
        r"""
        Encapsulate the Google Cloud Spanner lifecycle.

        If *credentials* is provided, the project identification it contains supercedes *project_id*,
        otherwise the latter is required.

        The nature of access *credentials* depends on its data type:
            - type *dict*: *credentials* is a Python dictionary
            - type *BytesIO*: *credentials* is a byte stream
            - type *Path*: *credentials* is a path to a file holding the data
            - type *bytes*: *credentials* holds the data (used as is)
            - type *str*: *credentials* holds the data (used as utf8-encoded)

        The *google.oauth2.service_account* package requires the credentials sampled below for authentication:
            - "type": "service_account",
            - "project_id": "my-gcp-project-id",
            - "private_key_id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0",
            - "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBhEFAASC+...\\n-----END PRIVATE KEY-----\\n",
            - "client_email": "my-service-account-name@my-gcp-project-id.iam.gserviceaccount.com",
            - "client_id": "12345678901234567890",
            - "auth_uri": "accounts.google.com",
            - "token_uri": "oauth2.googleapis.com",
            - "auth_provider_x509_cert_url": "www.googleapis.com",
            - "client_x509_cert_url": "www.googleapis.com"

        A credentials file for Google Cloud Spanner is a standard *JSON* service account key file containing
        the data described above. The parameter *project_id* is required if *credentials* is not provided,
        otherwise it is ignored.

        :param engine_id: the reference database engine
        :param errors: incidental error messages (may be a non-empty list)
        """
        # instance objects
        self.engine_id: str | None = None
        self.client: Client | None = None
        self.instance: Instance | None = None
        self.database: Database | None = None
        self.credentials: dict[str, str] | None = None  # access credentials
        self.pinger: Thread | None = None               # thread for background pinging

        err_msg: str | None = None
        conn_data: dict[str, Any] = DB_CONN_DATA.get(engine_id)
        if not conn_data:
            err_msg = f"GoogleSpanner '{engine_id}' not yet configured"
        elif conn_data.get(SpannerParam.ENGINE):
            err_msg = f"Instance '{engine_id}' of GoogleSpanner already exists"
        else:
            credentials: dict[str, str] | BytesIO | Path | str | bytes = conn_data.get(SpannerParam.CREDENTIALS)
            if credentials:
                if not isinstance(credentials, dict):
                    credentials_bytes: bytes = file_get_data(credentials)
                    credentials = json.loads(s=credentials_bytes)
                self.credentials = credentials
                conn_data[SpannerParam.PROJECT_ID] = credentials["project_id"]
            elif not conn_data.get(SpannerParam.PROJECT_ID):
                err_msg = f"Either 'credentials' or 'project_id' mus be provided for GoogleSpanner '{engine_id}'"

        if err_msg:
            logger: Logger = conn_data.get(DbParam.LOGGER)
            if logger:
                logger.error(msg=err_msg)
            if isinstance(errors, list):
                errors.append(err_msg)
        else:
            self.engine_id = engine_id
            conn_data[SpannerParam.ENGINE] = self

    def startup(self,
                errors: list[str] = None) -> bool:
        """
        Initialize access to the Google Cloud Spanner database engine.

        This method should be invoked immediately after object instantiation time.

        :param errors: incidental error messages (might be a non-empty list)
        """
        # initialize the return variable
        result: bool = True

        # obtain the Cloud Spanner configuration data
        err_msg: str | None = None
        conn_data: dict[str, Any] = DB_CONN_DATA.get(self.engine_id)

        if not conn_data:
            err_msg = f"GoogleSpanner '{self.engine_id}' not properly initialized"
        elif not conn_data.get(DbParam.VERSION):
            if conn_data.get(SpannerParam.DATABASE_ID) and \
               conn_data.get(SpannerParam.INSTANCE_ID):
                try:
                    # obtain a Cloud Spanner Client
                    if self.credentials:
                        # use specific service account data
                        credentials: Any = Credentials.from_service_account_info(info=self.credentials)
                        self.client = spanner.Client(project=conn_data.get(SpannerParam.PROJECT_ID),
                                                     credentials=credentials)
                    else:
                        # fallback to Application Default Credentials (ADC)
                        self.client = spanner.Client(project=conn_data.get(SpannerParam.PROJECT_ID))

                    # establish the instance
                    self.instance = self.client.instance(instance_id=conn_data.get(SpannerParam.INSTANCE_ID))

                    if conn_data.get(SpannerParam.SESSION_POOL_SIZE) > 0:
                        # use PingingPool for mixed read/write workloads
                        session_pool: PingingPool = PingingPool(
                            size=conn_data.get(SpannerParam.SESSION_POOL_SIZE),
                            default_timeout=conn_data.get(SpannerParam.SESSION_DEFAULT_TIMEOUT),
                            ping_interval=conn_data.get(SpannerParam.SESSION_PING_INTERVAL),
                        )
                        self.database = self.instance.database(database_id=conn_data.get(SpannerParam.DATABASE_ID),
                                                               pool=session_pool)
                    else:
                        # no pools available (local emulation is being used)
                        self.database = self.instance.database(database_id=conn_data[SpannerParam.DATABASE_ID])

                    # obtain the server version and release date
                    stmt: str = ("SELECT OPTION_VALUE "
                                 "FROM INFORMATION_SCHEMA.DATABASE_OPTIONS "
                                 "WHERE OPTION_NAME = 'optimizer_version'")
                    with self.database.snapshot() as snapshot:
                        rows: StreamedResultSet = snapshot.execute_sql(sql=stmt)
                        if rows is not None and len(list(rows)) > 0:
                            version: str | None = None
                            if list(rows)[0][0] == 'LATEST':
                                stmt = ("SELECT MAX(VERSION) "
                                        "FROM FROM SPANNER_SYS.SUPPORTED_OPTIMIZER_VERSIONS")
                                rows = snapshot.execute_sql(sql=stmt)
                                if rows is not None and len(list(rows)) > 0:
                                    version = list(rows)[0][0]
                            else:
                                version = list(rows)[0][0]
                            if version:
                                # HAZARD: 'OPTION_VALUE' is string, whereas 'VERSION' is int
                                stmt = ("SELECT RELEASE_DATE "
                                        "FROM SPANNER_SYS.SUPPORTED_OPTIMIZER_VERSIONS "
                                        f"WHERE VERSION = {version}")
                                rows = snapshot.execute_sql(sql=stmt)
                                if rows is not None and len(list(rows)) > 0:
                                    conn_data[DbParam.VERSION] = f"Release {version}, {list(rows)[0][0]}"
                except Exception as e:
                    exc_err: str = exc_format(exc=e,
                                              exc_info=sys.exc_info())
                    err_msg = f"Unable to startup GoogleSpanner '{self.engine_id}': {exc_err}"
            else:
                err_msg = f"GoogleSpanner '{self.engine_id}' missing session configuration parameter"

        if err_msg:
            result = False
            logger: Logger = DB_CONN_DATA[self.engine_id].get(DbParam.LOGGER)
            if logger:
                logger.error(msg=err_msg)
            if isinstance(errors, list):
                errors.append(err_msg)

        return result

    def background_ping_and_prime(self) -> None:
        """
        Keep sessions/transactions ready with background pinging.
        """
        # make sure database pool is the proper type
        # noinspection PyProtectedMember
        if isinstance(self.database._pool, PingingPool):  # noqa: SLF001 - private member accessed
            # noinspection PyTypeChecker
            params: dict[SpannerParam, Any] = DB_CONN_DATA.get(self.engine_id)
            name: str = f"spanner-ping-{params[SpannerParam.INSTANCE_ID]}/{params[SpannerParam.DATABASE_ID]}"

            # build and launch the pinging daemon
            self.pinger = Thread(target=self._background_ping_and_prime,
                                 name=name,
                                 daemon=True)
            self.pinger.start()

    def _background_ping_and_prime(self) -> None:
        """
        Ping sessions to keep them ready.
        """
        # retrieve the pinging interval
        # noinspection PyTypeChecker
        params: dict[SpannerParam, Any] = DB_CONN_DATA.get(self.engine_id)
        interval: int = params[SpannerParam.SESSION_PING_INTERVAL]

        # retrieve the database pool
        # noinspection PyProtectedMember
        db_pool: PingingPool = self.database._pool

        # background-ping the sessions
        while True:
            with suppress(Exception):
                db_pool.ping()
            time.sleep(interval)


class SpannerConnection:
    """
    An implementation of the RDBMS *connection* paradigm, as applied to the Google Cloud Spanner database.

    These are the instance variables:
        - *_database*: *google.cloud.spanner_v1.database import Database* - the reference database
        - *_db_pool*: session pool associated with the database
        - *_pool_session*: *google.cloud.spanner_v1.session.Session* - session obtained from the pool
        - *_session_txn*: *google.cloud.spanner_v1.transaction.Transaction* - session transaction
        - *_autocommit*: whether the connection is in autocommit mode
    """

    def __init__(self,
                 engine_id: str,
                 autocommit: bool = None,
                 errors: list[str] = None) -> None:
        """
        Instantiate a *connection* with a Cloud Spanner database.

        The parameter *autocommit* indicates whether the connection should operate in autocommit mode.
        Regardless of its setting, the connection automatically falls back to autocommit mode if no
        session pools are available.

        :param engine_id: the reference database engine
        :param autocommit: whether the connection is to operate in autocommit mode (defaults to *False*)
        :param errors: incidental error messages (might be a non-empty list)
        """
        # instance variables
        self.engine_id: str | None = None
        self.google_spanner: GoogleSpanner | None = None
        self.db_pool: AbstractSessionPool | None = None
        self.pool_session: Session | None = None
        self.session_txn: Transaction | None = None
        self.autocommit: bool | None = None

        spanner_data: dict[str, Any] = DB_CONN_DATA.get(engine_id) or {}
        google_spanner: GoogleSpanner = spanner_data.get(SpannerParam.ENGINE)
        if google_spanner:
            self.engine_id = engine_id
            self.google_spanner = google_spanner

            self.autocommit = autocommit
            if not self.autocommit:
                try:
                    # retrieve the database pool
                    # noinspection PyProtectedMember
                    self.db_pool = self.google_spanner.database._pool
                    if self.db_pool is None:
                        # autocommit mode is required when pools are not available
                        self.autocommit = True
                    else:
                        # acquire a session from the pool
                        self.pool_session = self.db_pool.get()
                except Exception as e:
                    exc_err: str = exc_format(exc=e,
                                              exc_info=sys.exc_info())
                    logger: Logger = DB_CONN_DATA[engine_id].get(DbParam.LOGGER)
                    if logger:
                        logger.error(msg=exc_err)
                    if isinstance(errors, list):
                        errors.append(exc_err)

    def select(self,
               sel_stmt: str,
               *,
               param_values: dict[str, Any] = None,
               param_types: dict[str, Type] = None,
               committable: bool = None,
               errors: list[str] = None) -> list[tuple] | None:
        """
        Query the database and return all tuples that satisfy the *sel_stmt* command.

        The command can optionally contain selection criteria, with respective names and values
        given in *param_values*, and names and types given in *param_types*.

        If the connection is in *autocommit* mode, a commit or rollback operation is always performed following
        the query execution. Otherwise, this behaviour is defined by *commitable* (defaults to no commit/rollback).

        :param sel_stmt: SELECT command for the query operation
        :param param_values: the selection criteria, specified as key-value pairs
        :param param_types: the types involved in the selection criteria, specified as key-value pairs
        :param committable: whether to commit or rollback the operation, upon completion
        :param errors: incidental error messages (might be a non-empty list)
        :return: list of tuples containing the search result, *[]* on empty search, or *None* if error
        """
        # initialize the return variable
        result: list[tuple] | None = None

        if self.autocommit:
            try:
                with self.google_spanner.database.snapshot() as snapshot:
                    rows: StreamedResultSet = snapshot.execute_sql(sql=sel_stmt,
                                                                   params=param_values,
                                                                   param_types=param_types)
                    if rows is not None:
                        result = list(rows)
            except Exception as e:
                exc_err: str = exc_format(exc=e,
                                          exc_info=sys.exc_info())
                if isinstance(errors, list):
                    errors.append(exc_err)
        else:
            if self.session_txn is None:
                self.session_txn = Transaction(session=self.pool_session)
                self.session_txn.begin()
            try:
                rows: StreamedResultSet = self.session_txn.execute_sql(sql=sel_stmt,
                                                                       params=param_values,
                                                                       param_types=param_types)
                if rows is not None:
                    result = list(rows)
                if committable:
                    self.session_txn.commit()
                    self.session_txn = None
            except Exception as e:
                exc_err: str = exc_format(exc=e,
                                          exc_info=sys.exc_info())
                logger: Logger = DB_CONN_DATA[self.google_spanner.engine_id].get(DbParam.LOGGER)
                if logger:
                    logger.error(msg=exc_err)
                if isinstance(errors, list):
                    errors.append(exc_err)
                if committable:
                    self.session_txn.rollback()
                    self.session_txn = None

        return result

    def execute(self,
                exc_stmt: str,
                *,
                param_values: dict[str, Any] = None,
                param_types: dict[str, Type] = None,
                committable: bool = None,
                errors: list[str] = None) -> tuple | int | None:
        """
        Execute the command *exc_stmt* on the database.

        This command might be a DML ccommand modifying the database, such as inserting, updating or
        deleting tuples, or it might be a DDL statement, or it might even be an environment-related command.

        If the connection is in *autocommit* mode, a commit or rollback operation is always performed following
        the query execution. Otherwise, this behaviour is defined by *commitable* (defaults to no commit/rollback).

        :param exc_stmt: the command to execute
        :param param_values: the selection criteria, specified as key-value pairs
        :param param_types: the types involved in the selection criteria, specified as key-value pairs
        :param committable: whether to commit or rollback the operation, upon completion
        :param errors: incidental error messages (must be *[]* or *None*)
        :return: the value or values returned by the operation, or *None* if error
        """
        def execute_in_transaction(transaction: Transaction,
                                   curr_stmt: str,
                                   curr_params: dict[str, Any],
                                   curr_types: dict[str, Type]) -> tuple | int | None:

            # initialize the return variable
            result: tuple | int | None = None

            rs: StreamedResultSet = transaction.execute_sql(sql=curr_stmt,
                                                            params=curr_params,
                                                            param_types=curr_types)
            if rs is not None:
                rows: list = list(rs)
                if len(rows) > 0:
                    result = rows[0]

            return result

        # initialize the return variable
        result: tuple | int | None = None

        if self.autocommit:
            try:
                # noinspection PyTypeChecker
                result = self.google_spanner.database.run_in_transaction(func=execute_in_transaction,
                                                                         curr_stmt=exc_stmt,
                                                                         curr_params=param_values,
                                                                         curr_types=param_types)
            except Exception as e:
                exc_err: str = exc_format(exc=e,
                                          exc_info=sys.exc_info())
                logger: Logger = DB_CONN_DATA[self.google_spanner.engine_id].get(DbParam.LOGGER)
                if logger:
                    logger.error(msg=exc_err)
                if isinstance(errors, list):
                    errors.append(exc_err)
        else:
            if self.session_txn is None:
                self.session_txn = Transaction(session=self.pool_session)
                self.session_txn.begin()
            try:
                rs: StreamedResultSet = self.session_txn.execute_sql(sql=exc_stmt,
                                                                     params=param_values,
                                                                     param_types=param_types)
                if rs is not None:
                    rows: list = list(rs)
                    if len(rows) > 0:
                        result = rows[0]
                if committable:
                    self.session_txn.commit()
                    self.session_txn = None
            except Exception as e:
                exc_err: str = exc_format(exc=e,
                                          exc_info=sys.exc_info())
                logger: Logger = DB_CONN_DATA[self.google_spanner.engine_id].get(DbParam.LOGGER)
                if logger:
                    logger.error(msg=exc_err)
                if isinstance(errors, list):
                    errors.append(exc_err)
                if committable:
                    self.session_txn.rollback()
                    self.session_txn = None

        return result

    def commit(self) -> None:
        """
        Commit the ongoing transaction.
        """
        if self.session_txn:
            self.session_txn.commit()

    def rollback(self) -> None:
        """
        Rollback the ongoing transaction.
        """
        if self.session_txn:
            self.session_txn.rollback()

    def close(self) -> None:
        """
        Close the connection, releasing its resources.
        """
        # return session to the pool
        if self.pool_session and self.db_pool:
            self.db_pool.put(session=self.pool_session)

        # release all resources
        self.google_spanner = None
        self.db_pool = None
        self.pool_session = None
        self.session_txn = None

    def __del__(self) -> None:
        """
        Clean up object before garbage collection.

        Release all resources if 'close()' is not invoked prior to the loss of all references to the object.
        """
        self.google_spanner = None
        self.db_pool = None
        self.pool_session = None
        self.session_txn = None
