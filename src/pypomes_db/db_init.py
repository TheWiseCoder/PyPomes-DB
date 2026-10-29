import sys
from pypomes_core import (
    APP_PREFIX, env_get_int, env_get_str, env_get_strs, env_get_enum, env_get_path
)

from .db_common import DB_CONN_DATA, DbEngine, DbParam, SpannerParam


def db_init_from_environment() -> None:
    """
    Establish the connection data for select database engines, from environment variables.

    The preferred way to specify database connection parameters is dynamically with *db_setup()*
    in module *db_pomes* or, for *GoogleSpanner* engines, *spanner_setup()* in module *spanner_pomes*.
    To make sure defined environment variables are processed, this function is invoked once,
    at module *db_pomes* load time. The environment variables can be setup in two ways:

    1. for a single database engine, specify the data set:
        - for a *GoogleSpanner* database engine
          (all non-defaulted parameters are required, except for *<APP_PREFIX>_DB_PROJECT_ID*):
            - *<APP_PREFIX>_DB_TYPE* (*spanner*)
            - *<APP_PREFIX>_DB_DATABASE_ID*
            - *<APP_PREFIX>_DB_INSTANCE_ID*
            - *<APP_PREFIX>_DB_PROJECT_ID*
            - *<APP_PREFIX>_DB_SESSION_DEFAULT_TIMEOUT* (defaults to 10 seconds)
            - *<APP_PREFIX>_DB_SESSION_PING_INTERVAL*  (defaults to 3000 seconds)
            - *<APP_PREFIX>_DB_SESSION_POOL_SIZE* (defaults to 10 for cloud operation)
        - for other engines
          (all parameters must be provided, except for *<APP_PREFIX>_DB_TYPE*):
            - *<APP_PREFIX>_DB_ENGINE*
            - *<APP_PREFIX>_DB_TYPE* (one of *mysql*, *oracle*, *postgres*, *sqlserver*)
            - *<APP_PREFIX>_DB_NAME*
            - *<APP_PREFIX>_DB_USER*
            - *<APP_PREFIX>_DB_PWD*
            - *<APP_PREFIX>_DB_HOST*
            - *<APP_PREFIX>_DB_PORT*
            - *<APP_PREFIX>_DB_CLIENT*  (for Oracle)
            - *<APP_PREFIX>_DB_DRIVER*  (for SQLServer)

    2. for multiple database engines, or if engine id named variables are preferred, specify a
       comma-separated list of engine identifications in *<APP_PREFIX>_DB_ENGINES*, and for each one,
       specify the data set above, respectively replacing *_DB_* with that unique identification.
    """
    prefixes: list[str] = []
    single_engine: str = env_get_str(key=f"{APP_PREFIX}_DB_ENGINE")
    if single_engine:
        prefixes.append("DB")
    else:
        multi_engines: list[str] = env_get_strs(key=f"{APP_PREFIX}_DB_ENGINES")
        if multi_engines:
            prefixes.extend(multi_engines)

    for prefix in prefixes:
        engine_id = single_engine or prefix
        # HAZARD: prevent re-establishing connection data for engine
        if engine_id not in DB_CONN_DATA:
            prefix = f"{APP_PREFIX}_{prefix}"
            db_type: DbEngine = env_get_enum(key=f"{prefix}_TYPE",
                                             enum_class=DbEngine)
            if not db_type:
                err_msg: str = f"Unable to establish type for database engine '{engine_id}'"
                sys.stderr.write(err_msg)
                raise ValueError(err_msg)

            DB_CONN_DATA[engine_id] = {
                    DbParam.TYPE: db_type,
                    DbParam.VERSION: "",
                    DbParam.LOGGER: None
            }
            if db_type == DbEngine.SPANNER:
                DB_CONN_DATA[engine_id].update({
                    SpannerParam.DATABASE_ID: env_get_str(key=f"{prefix}_DATABASE_ID"),
                    SpannerParam.INSTANCE_ID: env_get_str(key=f"{prefix}_INSTANCE_ID"),
                    SpannerParam.PROJECT_ID: env_get_str(key=f"{prefix}_PROJECT_ID"),
                    SpannerParam.CREDENTIALS:  env_get_path(key=f"{prefix}_CREDENTIALS_ID"),
                    SpannerParam.SESSION_DEFAULT_TIMEOUT: env_get_int(key=f"{prefix}_SESSION_DEFAULT_TIMEOUT",
                                                                      def_value=10),
                    SpannerParam.SESSION_PING_INTERVAL: env_get_int(key=f"{prefix}_SESSION_PING_INTERVAL",
                                                                    def_value=3000),
                    SpannerParam.SESSION_POOL_SIZE: env_get_int(key=f"{prefix}_SESSION_POOL_SIZE",
                                                                def_value=10)
                })
            else:
                DB_CONN_DATA[engine_id].update({
                    DbParam.NAME: env_get_str(key=f"{prefix}_NAME"),
                    DbParam.USER: env_get_str(key=f"{prefix}_USER"),
                    DbParam.PWD: env_get_str(key=f"{prefix}_PWD"),
                    DbParam.HOST: env_get_str(key=f"{prefix}_HOST"),
                    DbParam.PORT: env_get_int(key=f"{prefix}_PORT")
                })
                if db_type == DbEngine.ORACLE:
                    DB_CONN_DATA[engine_id][DbParam.CLIENT] = env_get_path(key=f"{prefix}_CLIENT")
                elif db_type == DbEngine.SQLSERVER:
                    DB_CONN_DATA[engine_id][DbParam.DRIVER] = env_get_str(key=f"{prefix}_DRIVER")
