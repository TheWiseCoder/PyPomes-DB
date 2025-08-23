from datetime import date, datetime
from logging import Logger
from mysql import connector
from mysql.connector.aio import MySQLConnectionAbstract
from pypomes_core import DateFormat, DatetimeFormat
from typing import Any, Final

from .db_common import (
    DbEngine, DbParam, _get_params, _except_msg
)

RESERVED_WORDS: Final[list[str]] = [
    "ACCESSIBLE", "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE", "BEFORE", "BETWEEN",
    "BIGINT", "BINARY", "BLOB", "BOTH", "BY", "CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER",
    "CHECK", "COLLATE", "COLUMN", "CONDITION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS",
    "CUBE", "CUME_DIST", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR",
    "DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL",
    "DECLARE", "DEFAULT", "DELAYED", "DELETE", "DENSE_RANK", "DESC", "DESCRIBE", "DETERMINISTIC", "DISTINCT",
    "DISTINCTROW", "DIV", "DOUBLE", "DROP", "DUAL", "EACH", "ELSE", "ELSEIF", "EMPTY", "ENCLOSED", "ESCAPED",
    "EXCEPT", "EXECUTE", "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH", "FIRST_VALUE", "FLOAT", "FLOAT4", "FLOAT8",
    "FOR", "FORCE", "FOREIGN", "FROM", "FULLTEXT", "FUNCTION", "GENERATED", "GET", "GRANT", "GROUP", "GROUPING",
    "GROUPS", "HAVING", "HIGH_PRIORITY", "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE", "IN",
    "INDEX", "INFILE", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT", "INT1", "INT2", "INT3", "INT4", "INT8",
    "INTEGER", "INTERVAL", "INTO", "IO_AFTER_GTIDS", "IO_BEFORE_GTIDS", "IS", "ITERATE", "JOIN", "JSON_TABLE",
    "KEY", "KEYS", "KILL", "LAG", "LAST_VALUE", "LATERAL", "LEAD", "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT",
    "LINEAR", "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT", "LOOP",
    "LOW_PRIORITY", "MASTER_BIND", "MASTER_SSL_VERIFY_SERVER_CERT", "MATCH", "MAXVALUE", "MEDIUMBLOB", "MEDIUMINT",
    "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "MODIFIES", "NATURAL", "NOT",
    "NO_WRITE_TO_BINLOG", "NTH_VALUE", "NTILE", "NULL", "NUMERIC", "OF", "ON", "OPTIMIZE", "OPTIMIZER_COSTS",
    "OPTION", "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER", "OUTFILE", "OVER", "PARTITION", "PERCENT_RANK",
    "PRECISION", "PRIMARY", "PROCEDURE", "PURGE", "RANGE", "RANK", "READ", "READS", "READ_WRITE", "REAL",
    "RECURSIVE", "REF_SYSTEM_ID", "REFERENCES", "REGEXP", "RELEASE", "RENAME", "REPEAT", "REPLACE", "REQUIRE",
    "RESIGNAL", "RESTRICT", "RETURN", "REVOKE", "RIGHT", "RLIKE", "ROW", "ROWS", "ROW_NUMBER", "SCHEMA",
    "SCHEMAS", "SECOND_MICROSECOND", "SELECT", "SENSITIVE", "SEPARATOR", "SET", "SHOW", "SIGNAL", "SMALLINT",
    "SPATIAL", "SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT",
    "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING", "STORED", "STRAIGHT_JOIN", "SYSTEM",
    "TABLE", "TERMINATED", "THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING", "TRIGGER", "TRUE",
    "UNDO", "UNION", "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE", "USING", "UTC_DATE", "UTC_TIME",
    "UTC_TIMESTAMP", "VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "VIRTUAL", "WHEN", "WHERE",
    "WHILE", "WINDOW", "WITH", "WRITE", "XOR", "YEAR_MONTH", "ZEROFILL"
]


def connect(autocommit: bool = None,
            errors: list[str] = None,
            logger: Logger = None) -> MySQLConnectionAbstract | None:
    """
    Obtain and return a connection to the database.

    Return *None* if the connection could not be obtained.

    :param autocommit: whether the connection is to be in autocommit mode (defaults to *False*)
    :param errors: incidental error messages
    :param logger: optional logger
    :return: the connection to the database, or *None* if error
    """
    # initialize the return variable
    result: MySQLConnectionAbstract | None = None

    # retrieve the connection parameters
    db_params: dict[DbParam, Any] = _get_params(DbEngine.MYSQL)

    # obtain a connection to the database
    err_msg: str | None = None
    try:
        result = connector.connect(database=db_params.get(DbParam.NAME),
                                   host=db_params.get(DbParam.HOST),
                                   port=db_params.get(DbParam.PORT),
                                   user=db_params.get(DbParam.USER),
                                   password=db_params.get(DbParam.PWD))
        # establish the connection's autocommit mode
        result.autocommit = isinstance(autocommit, bool) and autocommit
    except Exception as e:
        err_msg = _except_msg(exception=e,
                              engine=DbEngine.ORACLE)
    # log errors
    if err_msg:
        if isinstance(errors, list):
            errors.append(err_msg)
        if logger:
            logger.error(err_msg)
            logger.error(msg=f"Error connecting to '{db_params.get(DbParam.NAME)} '"
                             f"at '{db_params.get(DbParam.NAME)}'")

    return result


def bind_arguments(stmt: str,
                   bind_vals: list[Any]) -> str:
    """
    Replace the placeholders in *query_stmt* with the values in *bind_vals*, and return the modified query statement.

    Note that using a statement in a situation where values for types other than *bool*, *str*, *int*, *float*,
    *date*, or *datetime* were replaced, may bring about undesirable consequences, as the standard string
    representations for these other types would be used.

    :param stmt: the query statement
    :param bind_vals: the values to replace the placeholders with
    :return: the query statement with the placeholders replaced with their corresponding values
    """
    # initialize the return variable
    result: str = stmt

    # bind the arguments
    for bind_val in bind_vals:
        val: str
        if isinstance(bind_val, bool):
            val = "1" if bind_val else "0"
        elif isinstance(bind_val, int | float):
            val = f"{bind_val}"
        elif isinstance(bind_val, date):
            val = f"STR_TO_DATE('{bind_val.strftime(format=DateFormat.INV)}', '%Y-%m-%d')"
        elif isinstance(bind_val, datetime):
            val = f"STR_TO_DATE('{bind_val.strftime(format=DatetimeFormat.INV)}', '%Y-%m-%d %H:%i:%s')"
        else:
            val = f"'{bind_val}'"
        result = result.replace("?", val, 1)

    return result
