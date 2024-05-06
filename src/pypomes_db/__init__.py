from .db_pomes import (
    db_setup, db_get_engines, db_get_params,
    db_assert_connection, db_connect, db_exists,
    db_select_one, db_select_all, db_update,
    db_delete, db_insert, db_bulk_insert,
    db_execute, db_call_function, db_call_procedure,
)

__all__ = [
    # db_pomes
    "db_setup", "db_get_engines", "db_get_params",
    "db_assert_connection", "db_connect", "db_exists",
    "db_select_one", "db_select_all", "db_update",
    "db_delete", "db_insert", "db_bulk_insert",
    "db_execute", "db_call_function", "db_call_procedure",
]

from importlib.metadata import version
__version__ = version("pypomes_db")
__version_info__ = tuple(int(i) for i in __version__.split(".") if i.isdigit())
