
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Mapping, Optional, Sequence, Tuple
import threading

from google.cloud import spanner
from google.cloud.spanner_v1 import Client
from google.cloud.spanner_v1.database import Database
from google.cloud.spanner_v1.instance import Instance
from google.cloud.spanner_v1.session import Session
from google.cloud.spanner_v1.transaction import Transaction
from google.api_core.exceptions import Aborted
from google.auth.credentials import Credentials


# ----------------------------
# TransactionScope (per-thread)
# ----------------------------

@dataclass(slots=True)
class _ThreadTxnState:
    session: Session
    txn: Transaction


class TransactionScope:
    """Per-thread transaction manager bound to a Database."""

    def __init__(self, *, database: Database) -> None:
        self._db: Database = database
        self._local: threading.local = threading.local()

    def begin(self, *, labels: Optional[Mapping[str, str]] = None) -> None:
        if getattr(self._local, "state", None) is not None:
            raise RuntimeError("A transaction is already active in this thread.")
        # Explicit session + transaction begin (documented API). [1](https://cloud.google.com/python/docs/reference/spanner/latest/google.cloud.spanner_v1.session.Session)[2](https://cloud.google.com/python/docs/reference/spanner/2.1.1/transaction-api)
        session: Session = Session(database=self._db, labels=dict(labels) if labels else None)
        session.create()  # create the session resource on Spanner. [1](https://cloud.google.com/python/docs/reference/spanner/latest/google.cloud.spanner_v1.session.Session)
        txn: Transaction = Transaction(session)
        txn.begin()       # start RW transaction. [2](https://cloud.google.com/python/docs/reference/spanner/2.1.1/transaction-api)
        self._local.state = _ThreadTxnState(session=session, txn=txn)

    def is_active(self) -> bool:
        return getattr(self._local, "state", None) is not None

    def current(self) -> _ThreadTxnState:
        state = getattr(self._local, "state", None)
        if state is None:
            raise RuntimeError("No active transaction in this thread. Call begin() first.")
        return state

    def commit(self) -> None:
        state = self.current()
        try:
            state.txn.commit()  # returns a commit timestamp. [2](https://cloud.google.com/python/docs/reference/spanner/2.1.1/transaction-api)
        except Aborted:
            # Let caller decide how to rebuild / retry its unit of work. [4](https://docs.cloud.google.com/python/docs/reference/spanner/latest/transaction-usage)
            raise
        finally:
            try:
                state.session.delete()
            finally:
                self._local.state = None

    def rollback(self) -> None:
        state = self.current()
        try:
            state.txn.rollback()  # explicit rollback. [4](https://docs.cloud.google.com/python/docs/reference/spanner/latest/transaction-usage)
        finally:
            try:
                state.session.delete()
            finally:
                self._local.state = None


# -------------
# Cursor wrapper
# -------------
class SpannerCursor:
    """
    psycopg2-like cursor:
      - execute(sql, params, param_types)
      - fetchone() / fetchall()
      - rowcount for DML
    """

    def __init__(self, *, connection: SpannerConnection) -> None:  # type: ignore[name-defined]
        self._conn: SpannerConnection = connection
        self._rows: List[Tuple[Any, ...]] = []
        self.rowcount: int = -1

    def close(self) -> None:
        self._rows.clear()
        self.rowcount = -1

    def execute(
        self,
        *,
        sql: str,
        params: Optional[Mapping[str, Any]] = None,
        param_types: Optional[Mapping[str, spanner.param_types.ParamType]] = None,
    ) -> None:
        """Execute a query or DML. Results are buffered for fetchone/fetchall."""
        # Simple classification: SELECT vs DML.
        is_select: bool = sql.strip().lower().startswith("select")
        params = params or {}
        param_types = param_types or {}

        scope = self._conn._txn_scope
        db = self._conn.database

        self._rows = []
        self.rowcount = -1

        if scope.is_active():
            # Use the active RW transaction for both reads and DML. [4](https://docs.cloud.google.com/python/docs/reference/spanner/latest/transaction-usage)
            txn = scope.current().txn
            result = txn.execute_sql(sql=sql, params=params, param_types=param_types)
            if is_select:
                self._rows = [tuple(row) for row in result]  # buffer rows
                self.rowcount = len(self._rows)
            else:
                # DML returns affected-row count. [2](https://cloud.google.com/python/docs/reference/spanner/2.1.1/transaction-api)
                self.rowcount = int(result)
            return

        # No active transaction
        if is_select:
            # Read-only snapshot for autocommit-style SELECT. [3](https://googleapis.dev/python/spanner/1.15.0/database-usage.html)
            with db.snapshot() as snapshot:
                result_set = snapshot.execute_sql(sql=sql, params=params, param_types=param_types)
                self._rows = [tuple(row) for row in result_set]
                self.rowcount = len(self._rows)
            return

        # DML outside explicit transaction: only allowed if autocommit=True.
        if not self._conn.autocommit:
            raise RuntimeError("No active transaction. Call conn.begin() or set autocommit=True.")

        # One-off transactional callback for a single DML statement. [4](https://docs.cloud.google.com/python/docs/reference/spanner/latest/transaction-usage)
        def _one_dml(txn: Transaction) -> int:
            rc: int = txn.execute_sql(sql=sql, params=params, param_types=param_types)
            return rc

        self.rowcount = int(db.run_in_transaction(_one_dml))

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> List[Tuple[Any, ...]]:
        return list(self._rows)


# -------------------
# SpannerConnection
# -------------------
@dataclass(slots=True)
class SpannerConnection:
    """
    Encapsulates Client → Instance → Database and offers psycopg2-style API:
      - autocommit (default False)
      - begin()/commit()/rollback()
      - cursor().execute(...), fetchone(), fetchall(), rowcount

    Authentication is IAM-based (ADC / service account); no username/password. [7](https://docs.cloud.google.com/spanner/docs/getting-started/python)
    """

    project_id: str
    instance_id: str
    database_id: str

    credentials: Optional[Credentials] = field(default=None)
    endpoint: Optional[str] = field(default=None)
    emulator_host: Optional[str] = field(default=None)
    database_role: Optional[str] = field(default=None)

    min_sessions: int = field(default=10)
    max_sessions: int = field(default=400)
    write_sessions: float = field(default=0.2)

    autocommit: bool = field(default=False)  # psycopg2 default is False.

    _client: Optional[Client] = field(init=False, default=None)
    _instance: Optional[Instance] = field(init=False, default=None)
    _database: Optional[Database] = field(init=False, default=None)

    # Transaction scope bound to this connection (per-thread). Initialized in connect().
    _txn_scope: TransactionScope = field(init=False)

    def connect(self) -> None:
        if self._client and self._instance and self._database:
            return

        # Configure session pool (optional tuning).
        session_pool = spanner.SessionPool(
            min=self.min_sessions,
            max=self.max_sessions,
            write=self.write_sessions,
        )

        client_options: Optional[Mapping[str, Any]] = None
        if self.endpoint:
            client_options = {"api_endpoint": self.endpoint}

        if self.emulator_host:
            import os
            os.environ.setdefault("SPANNER_EMULATOR_HOST", self.emulator_host)

        self._client = spanner.Client(
            project=self.project_id,
            credentials=self.credentials,
            client_options=client_options,
        )
        self._instance = self._client.instance(self.instance_id)
        self._database = self._instance.database(
            self.database_id,
            pool=session_pool,
            database_role=self.database_role,
        )

        # Bind transaction scope to the database handle.
        self._txn_scope = TransactionScope(database=self._database)

    def is_connected(self) -> bool:
        return self._client is not None and self._instance is not None and self._database is not None

    @property
    def database(self) -> Database:
        if not self._database:
            raise RuntimeError("Spanner database is not initialized. Call connect() first.")
        return self._database

    # -------------
    # psycopg2-style
    # -------------
    def cursor(self) -> SpannerCursor:
        """Return a cursor bound to this connection."""
        if not self.is_connected():
            raise RuntimeError("Not connected. Call connect() first.")
        return SpannerCursor(connection=self)

    def begin(self, *, labels: Optional[Mapping[str, str]] = None) -> None:
        """Begin a transaction in the current thread (psycopg2: conn.begin())."""
        self._txn_scope.begin(labels=labels)

    def commit(self) -> None:
        """Commit current thread's transaction (psycopg2: conn.commit())."""
        self._txn_scope.commit()

    def rollback(self) -> None:
        """Rollback current thread's transaction (psycopg2: conn.rollback())."""
        self._txn_scope.rollback()

    # Optional convenience toggles
    def set_autocommit(self, *, autocommit: bool) -> None:
        self.autocommit = autocommit

    # Context manager
    def __enter__(self) -> SpannerConnection:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        # Explicitly close pooled sessions.
        if self._database is not None:
            try:
                self._database._pool.close()
            except Exception:
                pass
        self._client = None
        self._instance = None
        self._database = None