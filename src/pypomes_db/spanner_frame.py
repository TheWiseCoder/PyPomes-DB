import json
import sys
import time
from contextlib import suppress
from enum import StrEnum, auto
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
from pathlib import Path
from pypomes_core import file_get_data, exc_format
from threading import Thread
from typing import Any

from .db_common import (
    _DB_CONN_DATA, _DB_LOGGERS, DbEngine, DbParam
)


class SpannerParam(StrEnum):
    """
    Parameters for *GoogleSpanner*, an encapsulation of the Google Cloud Spanner access lifecycle.
    """
    ENGINE: auto()
    CLIENT_ID: auto()
    DATABASE_ID: auto()
    INSTANCE_ID: auto()
    PROJECT_ID: auto()
    SESSION_DEFAULT_TIMEOUT: auto()
    SESSION_PING_INTERVAL: auto()
    SESSION_POOL_SIZE = auto()


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
        - *_client*: *google.cloud.spanner.Client* - the Cloud Spanner Client
        - *_credentials*: *dict[str, str]* - the access credentials
        - *_database*: *google.cloud.spanner_v1.database.Database* - database to use
        - *_instance*: *google.cloud.spanner_v1.instance.Instance* - instance to use
        - *_pinger*: *threading.Thread* - thread for background pinging
    """

    def __init__(self,
                 *,
                 project_id: str = None,
                 credentials: dict[str, str] | BytesIO | Path | str | bytes = None,
                 errors: list[str] = None) -> None:
        r"""
        Encapsulate the Google Cloud Spanner lifecycle.

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

        :param errors: incidental error messages (might be a non-empty list)
        """
        # instance objects
        self._client: Client | None = None
        self._instance: Instance | None = None
        self._database: Database | None = None
        self._credentials: dict[str, str] | None = None  # access credentials
        self._pinger: Thread | None = None               # thread for background pinging

        err_msg: str | None = None
        client_id: str | None = None
        if DbEngine.SPANNER not in _DB_CONN_DATA:
            err_msg = "Google Cloud Spanner not yet configured"
        elif _DB_CONN_DATA[DbEngine.SPANNER].get(SpannerParam.ENGINE):
            err_msg = "Instance of Google Cloud Spanner already exists"
        elif project_id or credentials:
            # initilize the Cloud Spanner objects

            if credentials:
                if not isinstance(credentials, dict):
                    credentials_bytes: bytes = file_get_data(file_data=credentials)
                    self._credentials = json.loads(s=credentials_bytes)
                client_id = self._credentials.get("client_id")
                project_id = self._credentials["project_id"]
        else:
            err_msg = "Both 'credentials' and 'project_id' must be provided"

        if err_msg:
            if _DB_LOGGERS[DbEngine.SPANNER]:
                _DB_LOGGERS[DbEngine.SPANNER].error(msg=err_msg)
            if isinstance(errors, list):
                errors.append(err_msg)
        else:
            _DB_CONN_DATA[DbEngine.SPANNER].update({
                SpannerParam.ENGINE: self,
                SpannerParam.CLIENT_ID: client_id,
                SpannerParam.PROJECT_ID: project_id
            })

    def initialize(self,
                   errors: list[str] = None) -> bool:
        """
        Initialize access to the Google Cloud Spanner database engine.

        This method should be invoked once, immediately after object instantiation time.

        :param errors: incidental error messages (might be a non-empty list)
        """
        # initialize the return variable
        result: bool = True

        # obtain the Cloud Spanner configuration data
        conn_data: dict[str, Any] = _DB_CONN_DATA.get(DbEngine.SPANNER)

        err_msg: str | None = None
        if not conn_data or \
           not conn_data.get(SpannerParam.DATABASE_ID) or \
           not conn_data.get(SpannerParam.INSTANCE_ID):
            err_msg = "Unable to obtain session configuratiom parameters"
        elif not conn_data.get(DbParam.VERSION):
            try:
                # obtain a Cloud Spanner Client
                if self._credentials:
                    # use specific service account data
                    credentials: Any = Credentials.from_service_account_info(info=self._credentials)
                    self._client = spanner.Client(project=conn_data[SpannerParam.PROJECT_ID],
                                                  credentials=credentials)
                else:
                    # fallback to Application Default Credentials (ADC)
                    self._client = spanner.Client(project=conn_data[SpannerParam.PROJECT_ID])

                # establish the instance
                self._instance = self._client.instance(instance_id=conn_data[SpannerParam.INSTANCE_ID])

                if conn_data[SpannerParam.SESSION_POOL_SIZE] > 0:
                    # use PingingPool for mixed read/write workloads
                    session_pool: PingingPool = PingingPool(
                        size=conn_data[SpannerParam.SESSION_POOL_SIZE],
                        default_timeout=conn_data[SpannerParam.SESSION_DEFAULT_TIMEOUT],
                        ping_interval=conn_data[SpannerParam.SESSION_PING_INTERVAL],
                    )
                    self._database = self._instance.database(database_id=conn_data[SpannerParam.DATABASE_ID],
                                                             pool=session_pool)
                else:
                    # no pools available (local emulation is being used)
                    self._database = self._instance.database(database_id=conn_data[SpannerParam.DATABASE_ID])

                from google.cloud.spanner_v1 import __version__
                conn_data[DbParam.VERSION] = __version__
            except Exception as e:
                err_msg = exc_format(exc=e,
                                     exc_info=sys.exc_info())
        if err_msg:
            result = False
            if _DB_LOGGERS[DbEngine.SPANNER]:
                _DB_LOGGERS[DbEngine.SPANNER].error(msg=err_msg)
            if isinstance(errors, list):
                errors.append(err_msg)

        return result

    def background_ping_and_prime(self) -> None:
        """
        Keep sessions/transactions ready with background pinging.
        """
        # make sure database pool is the proper type
        # noinspection PyProtectedMember
        if isinstance(self._database._pool, PingingPool):  # noqa: SLF001 - private member accessed
            # noinspection PyTypeChecker
            params: dict[SpannerParam, Any] = _DB_CONN_DATA[DbEngine.SPANNER]
            name: str = f"spanner-ping-{params[SpannerParam.INSTANCE_ID]}/{params[SpannerParam.DATABASE_ID]}"

            # build and launch the pinging daemon
            self._pinger = Thread(target=self._background_ping_and_prime,
                                  name=name,
                                  daemon=True)
            self._pinger.start()

    def _background_ping_and_prime(self) -> None:
        """
        Ping sessions to keep them ready.
        """
        # retrieve the pinging interval
        # noinspection PyTypeChecker
        params: dict[SpannerParam, Any] = _DB_CONN_DATA[DbEngine.SPANNER]
        interval: int = params[SpannerParam.SESSION_PING_INTERVAL]

        # retrieve the database pool
        # noinspection PyProtectedMember
        db_pool: PingingPool = self._database._pool  # noqa: SLF001 - private member accessed

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
                 autocommit: bool = None,
                 errors: list[str] = None) -> None:
        """
        Instantiate a *connection* with a Cloud Spanner database.

        The parameter *autocommit* indicates whether the connection should operate in autocommit mode.
        Regardless of its setting, the connection automatically falls back to autocommit mode if no
        session pools are available.

        :param autocommit: whether the connection is to operate in autocommit mode (defaults to *False*)
        :param errors: incidental error messages (might be a non-empty list)
        """
        # instance variables
        self._database: Database | None = None
        self._db_pool: AbstractSessionPool | None = None
        self._pool_session: Session | None = None
        self._session_txn: Transaction | None = None
        self._autocommit: bool | None = None

        spanner_data: dict[str, Any] = _DB_CONN_DATA.get(DbEngine.SPANNER) or {}
        google_spanner: GoogleSpanner = spanner_data.get(SpannerParam.ENGINE)
        if google_spanner:
            # noinspection PyProtectedMember
            self._database = google_spanner._database  # noqa: SLF001 - private member accessed

            self._autocommit = autocommit
            if not self._autocommit:
                try:
                    # retrieve the database pool
                    # noinspection PyProtectedMember
                    self._db_pool = self._database._pool  # noqa: SLF001 - private member accessed
                    if self._db_pool is None:
                        # autocommit mode is required when pools are not available
                        self._autocommit = True
                    else:
                        # acquire a session from the pool
                        self._pool_session = self._db_pool.get()
                except Exception as e:
                    exc_err: str = exc_format(exc=e,
                                              exc_info=sys.exc_info())
                    if _DB_LOGGERS[DbEngine.SPANNER]:
                        _DB_LOGGERS[DbEngine.SPANNER].error(msg=exc_err)
                    if isinstance(errors, list):
                        errors.append(exc_err)

    def select(self,
               sel_stmt: str,
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

        if self._autocommit:
            try:
                with self._database.snapshot() as snapshot:
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
            if self._session_txn is None:
                self._session_txn = Transaction(session=self._pool_session)
                self._session_txn.begin()
            try:
                rows: StreamedResultSet = self._session_txn.execute_sql(sql=sel_stmt,
                                                                        params=param_values,
                                                                        param_types=param_types)
                if rows is not None:
                    result = list(rows)
                if committable:
                    self._session_txn.commit()
                    self._session_txn = None
            except Exception as e:
                exc_err: str = exc_format(exc=e,
                                          exc_info=sys.exc_info())
                if _DB_LOGGERS[DbEngine.SPANNER]:
                    _DB_LOGGERS[DbEngine.SPANNER].error(msg=exc_err)
                if isinstance(errors, list):
                    errors.append(exc_err)
                if committable:
                    self._session_txn.rollback()
                    self._session_txn = None

        return result

    def execute(self,
                exc_stmt: str,
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

        if self._autocommit:
            try:
                # noinspection PyTypeChecker
                result = self._database.run_in_transaction(func=execute_in_transaction,
                                                           curr_stmt=exc_stmt,
                                                           curr_params=param_values,
                                                           curr_types=param_types)
            except Exception as e:
                exc_err: str = exc_format(exc=e,
                                          exc_info=sys.exc_info())
                if _DB_LOGGERS[DbEngine.SPANNER]:
                    _DB_LOGGERS[DbEngine.SPANNER].error(msg=exc_err)
                if isinstance(errors, list):
                    errors.append(exc_err)
        else:
            if self._session_txn is None:
                self._session_txn = Transaction(session=self._pool_session)
                self._session_txn.begin()
            try:
                rs: StreamedResultSet = self._session_txn.execute_sql(sql=exc_stmt,
                                                                      params=param_values,
                                                                      param_types=param_types)
                if rs is not None:
                    rows: list = list(rs)
                    if len(rows) > 0:
                        result = rows[0]
                if committable:
                    self._session_txn.commit()
                    self._session_txn = None
            except Exception as e:
                exc_err: str = exc_format(exc=e,
                                          exc_info=sys.exc_info())
                if _DB_LOGGERS[DbEngine.SPANNER]:
                    _DB_LOGGERS[DbEngine.SPANNER].error(msg=exc_err)
                if isinstance(errors, list):
                    errors.append(exc_err)
                if committable:
                    self._session_txn.rollback()
                    self._session_txn = None

        return result

    def commit(self) -> None:
        """
        Commit the ongoing transaction.
        """
        if self._session_txn:
            self._session_txn.commit()

    def rollback(self) -> None:
        """
        Rollback the ongoing transaction.
        """
        if self._session_txn:
            self._session_txn.rollback()

    def close(self) -> None:
        """
        Close the connection, releasing its resources.
        """
        # return session to the pool
        if self._pool_session and self._db_pool:
            self._db_pool.put(session=self._pool_session)

        # release all resources
        self._database = None
        self._db_pool = None
        self._pool_session = None
        self._session_txn = None

    def __del__(self) -> None:
        """
        Clean up object before garbage collection.

        Release all resources if 'close()' is not invoked prior to the loss of all references to the object.
        """
        if self._database:
            self.close()
