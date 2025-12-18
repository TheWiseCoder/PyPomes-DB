from __future__ import annotations

import threading
from collections.abc import Generator
from contextlib import contextmanager
from google.cloud import spanner
from google.cloud.spanner_v1 import transaction as spanner_tx
from google.cloud.spanner_v1.database import Snapshot


# Type aliases using modern syntax
SpannerValue = str | int | float | bytes | bool | None
SpannerRow = tuple[SpannerValue, ...]
SpannerResultSet = list[SpannerRow]
ParamsDict = dict[str, SpannerValue]


class SpannerConnection:
    """
    Encapsulates Google Cloud Spanner connection with explicit transaction control.

    Usage:
        conn = SpannerConnection(
            project_id="my-project",
            instance_id="my-instance",
            database_id="my-db"
        )

        with conn.transaction() as tx:
            tx.execute("SELECT * FROM Users WHERE id = @id", {"id": 123})
            tx.execute("UPDATE Users SET last_login = @now WHERE id = @id", {"id": 123, "now": "2025-12-16"})
            # Transaction automatically commits at exit, or rolls back on exception
    """

    def __init__(
        self,
        *,
        project_id: str,
        instance_id: str,
        database_id: str,
        credentials: object | None = None,  # google.auth.credentials.Credentials
        client_options: dict[str, object] | None = None,
    ) -> None:
        """
        Initialize Spanner connection parameters.

        Args:
            project_id: Google Cloud project ID
            instance_id: Spanner instance ID
            database_id: Spanner database ID
            credentials: Optional Google Auth credentials
            client_options: Optional client options (e.g., {"api_endpoint": "..."})
        """
        self._project_id = project_id
        self._instance_id = instance_id
        self._database_id = database_id
        self._credentials = credentials
        self._client_options = client_options or {}

        # Thread-local storage for active transactions
        self._local = threading.local()

        # Initialize Spanner client and database reference
        self._client = spanner.Client(
            project=project_id,
            credentials=credentials,
            **self._client_options
        )
        self._instance = self._client.instance(instance_id)
        self._database = self._instance.database(database_id)

    def _get_active_transaction(self) -> spanner_tx.Transaction | None:
        """Get the transaction bound to the current thread, if any."""
        return getattr(self._local, "active_transaction", None)

    def _set_active_transaction(self, tx: spanner_tx.Transaction | None) -> None:
        """Bind/unbind transaction to current thread."""
        if tx is None:
            if hasattr(self._local, "active_transaction"):
                delattr(self._local, "active_transaction")
        else:
            self._local.active_transaction = tx

    @contextmanager
    def transaction(self) -> Generator[SpannerTransaction, None, None]:
        """
        Context manager for explicit transaction control.

        All queries executed via the yielded SpannerTransaction will be part of the same transaction.
        Commits on successful exit, rolls back on exception.
        """
        if self._get_active_transaction() is not None:
            raise RuntimeError("Nested transactions are not supported")

        spanner_tx_obj = self._database.transaction()
        spanner_tx_obj.begin()

        self._set_active_transaction(spanner_tx_obj)
        tx_wrapper = SpannerTransaction(spanner_tx_obj)

        try:
            yield tx_wrapper
            spanner_tx_obj.commit()
        except Exception:
            spanner_tx_obj.rollback()
            raise
        finally:
            self._set_active_transaction(None)

    def snapshot(self) -> Snapshot:
        """
        Get a read-only snapshot for consistent reads without locking.
        Not bound to transaction context.
        """
        return self._database.snapshot()


class SpannerTransaction:
    """
    Wrapper for Spanner transaction operations.
    Should only be obtained via SpannerConnection.transaction().
    """

    def __init__(self, tx: spanner_tx.Transaction) -> None:
        self._tx = tx

    def execute(
        self,
        sql: str,
        params: ParamsDict | None = None,
        param_types: dict[str, object] | None = None,
    ) -> SpannerResultSet:
        """
        Execute a SQL statement (SELECT/UPDATE/DELETE/INSERT) in the current transaction.

        Args:
            sql: SQL statement with @param placeholders
            params: Dictionary of parameter values
            param_types: Optional explicit parameter types (see google.cloud.spanner.param_types)

        Returns:
            List of result rows (tuples) for SELECT; empty list for DML
        """
        params = params or {}
        param_types = param_types or {}

        result = self._tx.execute_sql(
            sql,
            params=params,
            param_types=param_types
        )

        # Materialize results immediately (Spanner streams by default)
        return [tuple(row) for row in result]

    def execute_update(
        self,
        sql: str,
        params: ParamsDict | None = None,
        param_types: dict[str, object] | None = None,
    ) -> int:
        """
        Execute a DML statement and return number of affected rows.
        More efficient than execute() for UPDATE/DELETE/INSERT.

        Args:
            sql: DML statement with @param placeholders
            params: Dictionary of parameter values
            param_types: Optional explicit parameter types

        Returns:
            Number of affected rows
        """
        params = params or {}
        param_types = param_types or {}

        return self._tx.execute_update(
            sql,
            params=params,
            param_types=param_types
        )


# Example usage
if __name__ == "__main__":
    # Initialize connection (thread-safe)
    conn = SpannerConnection(
        project_id="my-gcp-project",
        instance_id="my-spanner-instance",
        database_id="my-database"
    )

    # Example: Transaction with multiple operations
    def update_user_activity(user_id: int, activity_timestamp: str) -> None:
        with conn.transaction() as tx:
            rows = tx.execute(
                "SELECT user_id FROM Users WHERE user_id = @user_id",
                {"user_id": user_id}
            )
            if not rows:
                raise ValueError(f"User {user_id} not found")

            tx.execute_update(
                "UPDATE Users SET last_activity = @ts WHERE user_id = @user_id",
                {"user_id": user_id, "ts": activity_timestamp}
            )

            tx.execute_update(
                "INSERT INTO ActivityLog (user_id, timestamp) VALUES (@user_id, @ts)",
                {"user_id": user_id, "ts": activity_timestamp}
            )
        # Transaction automatically committed here

    # Concurrent usage in threads is safe
    import concurrent.futures

    def worker(user_id: int) -> None:
        try:
            update_user_activity(user_id, "2025-12-16T10:00:00Z")
            print(f"Worker {user_id}: Success")
        except Exception as e:
            print(f"Worker {user_id}: Failed - {e}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(worker, uid) for uid in [1, 2, 3]]
        concurrent.futures.wait(futures)

    # Read-only snapshot
    with conn.snapshot() as snapshot:
        rows = list(snapshot.execute_sql("SELECT COUNT(*) FROM Users"))
        print(f"Total users: {rows[0][0]}")