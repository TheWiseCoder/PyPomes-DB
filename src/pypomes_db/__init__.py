from .db_pomes import (
    DB_HOST, DB_PWD, DB_NAME, DB_PORT, DB_USER, DB_DRIVER,
    db_connect, db_exists, db_select_one, db_select_all,
    db_update, db_delete, db_insert, db_bulk_insert, db_exec_stored_procedure,
)

__all__ = [
    # db_pomes
    "DB_HOST", "DB_PWD", "DB_NAME", "DB_PORT", "DB_USER", "DB_DRIVER",
    "db_connect", "db_exists", "db_select_one", "db_select_all",
    "db_update", "db_delete", "db_insert", "db_bulk_insert", "db_exec_stored_procedure",
]

from importlib.metadata import version
__version__ = version("pypomes_db")
__version_info__ = tuple(int(i) for i in __version__.split(".") if i.isdigit())
