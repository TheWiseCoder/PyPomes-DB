from .db_pomes import (
    db_setup, db_get_engines, db_get_param, db_get_params,
    db_get_connection_string, db_assert_connection, db_connect,
    db_has_table, db_has_view, db_exists,
    db_select, db_insert, db_update, db_delete,
    db_bulk_insert, db_bulk_update, db_update_lob,
    db_execute, db_call_function, db_call_procedure,
)
from .migration_pomes import (
    db_migrate_data, db_migrate_lobs,
)

__all__ = [
    # db_pomes
    "db_setup", "db_get_engines", "db_get_param", "db_get_params",
    "db_get_connection_string", "db_assert_connection", "db_connect",
    "db_has_table", "db_has_view", "db_exists",
    "db_select", "db_insert", "db_update", "db_delete",
    "db_bulk_insert", "db_bulk_update", "db_update_lob",
    "db_execute", "db_call_function", "db_call_procedure",
    # migration_pomes
    "db_migrate_data", "db_migrate_lobs",
]

from importlib.metadata import version
__version__ = version("pypomes_db")
__version_info__ = tuple(int(i) for i in __version__.split(".") if i.isdigit())
