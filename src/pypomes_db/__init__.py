from ._db_common import (
    DB_ENGINE, DB_HOST, DB_PWD, DB_NAME, DB_PORT, DB_USER,
)
from .db_pomes import (
    db_connect, db_exists, db_select_one, db_select_all,
    db_update, db_delete, db_insert, db_bulk_insert, db_exec_stored_procedure,
)

__all__ = [
    # _db_common
    "DB_ENGINE", "DB_HOST", "DB_PWD", "DB_NAME", "DB_PORT", "DB_USER",
    # db_pomes
    "db_connect", "db_exists", "db_select_one", "db_select_all",
    "db_update", "db_delete", "db_insert", "db_bulk_insert", "db_exec_stored_procedure",
]

from importlib.metadata import version
__version__ = version("pypomes_db")
__version_info__ = tuple(int(i) for i in __version__.split(".") if i.isdigit())
