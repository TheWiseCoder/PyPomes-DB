# noinspection PyProtectedMember
from pyodbc import connect, Connection, Row
from typing import Final
from pypomes_core.env_pomes import APP_PREFIX, env_get_int, env_get_str

# dados para acesso ao BD
DB_DRIVER: Final[str] = env_get_str(f"{APP_PREFIX}_DB_DRIVER")
DB_NAME: Final[str] = env_get_str(f"{APP_PREFIX}_DB_NAME")
DB_HOST: Final[str] = env_get_str(f"{APP_PREFIX}_DB_HOST")
DB_PORT: Final[int] = env_get_int(f"{APP_PREFIX}_DB_PORT")
DB_PWD: Final[str] = env_get_str(f"{APP_PREFIX}_DB_PWD")
DB_USER: Final[str] = env_get_str(f"{APP_PREFIX}_DB_USER")

__CONNECTION_KWARGS: Final[str] = f"DRIVER={{{DB_DRIVER}}};SERVER={DB_HOST},{DB_PORT};" \
                                  f"DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PWD};TrustServerCertificate=yes;"


def db_connect(errors: list[str]) -> Connection:
    """
    Obtém e retorna uma conexão ao banco de dados, ou *None* se a conexão não pode ser obtida.

    :param errors: lista a ser apensada com mensagem apropriada, em caso de erro
    :return: a conexão ao banco de dados
    """
    # inicializa a variável de retorno
    result: Connection | None = None

    # Obtém a conexão com o BD
    try:
        result = connect(__CONNECTION_KWARGS)
    except Exception as e:
        errors.append(__db_except_msg(e))

    return result


def db_exists(errors: list[str], table: str, where_attrs: list[str], where_vals: tuple) -> bool:
    """
    Determina se a tabela *table* no banco de dados contem pelo menos uma tupla onde *attrs* são iguais a
    *values*, respectivamente. Se mais de um, os atributos são concatenados pelo conector lógico *AND*.
    Retorna *None* se houver erro na consulta ao banco de dados.

    :param errors: lista a ser apensada com mensagem apropriada, em caso de erro
    :param table: a tabela a ser pesquisada
    :param where_attrs: os atributos para a busca
    :param where_vals: lista de valores a serem atribuídos aos atributos
    :return: True se não houve erro, e se pelo menos uma tupla existir
    """
    sel_stmt: str = f"SELECT * FROM {table}"  # noqa
    if len(where_attrs) > 0:
        sel_stmt += " WHERE " + "".join(f"{attr} = ? AND " for attr in where_attrs)[0:-5]
    rec: tuple = db_select_one(errors, sel_stmt, where_vals)
    result: bool = None if len(errors) > 0 else rec is not None

    return result


def db_select_one(errors: list[str], sel_stmt: str,
                  where_vals: tuple, required: bool = False) -> tuple:
    """
    Busca no banco de dados e retorna a primeira tupla que satisfaça o comando de busca *sel_stmt*.
    O comando pode opcionalmente conter critérios de busca, com valores respectivos fornecidos
    em *where_vals*. A lista de valores para um atributo com a cláusula *IN* deve estar contida
    em tupla específica. Na hipótese de erro, ou se a busca resultar vazia, *None* é retornado.

    :param errors: lista a ser apensada com mensagem apropriada, em caso de erro
    :param sel_stmt: comando SELECT para a busca
    :param where_vals: lista de valores a serem associados aos critérios de busca
    :param required: define se busca vazia deve ser considerada erro
    :return: tupla contendo o resultado da busca, ou None se houve erro ou se a busca resultar vazia
    """
    # inicializa a variável de retorno
    result: tuple | None = None

    exc: bool = False
    try:
        with connect(__CONNECTION_KWARGS) as conn:
            # obtem o cursor e executa a operação
            with conn.cursor() as cursor:
                sel_stmt = sel_stmt.replace("SELECT", "SELECT TOP 1")
                cursor.execute(sel_stmt, where_vals)
                # obtem a primeira tupla retornada pelo SELECT (None se nenhuma foi retornada)
                result = cursor.fetchone()
    except Exception as e:
        exc = True
        errors.append(__db_except_msg(e))

    # o parâmetro 'required' foi definido e nenhum registro foi obtido?
    if required and not exc and result is None:
        # sim, reporte o erro
        errors.append(__db_required_msg(sel_stmt, where_vals))

    return result


def db_exec_stored_procedure(errors: list[str], nm_proced: str, param_vals: tuple) -> list[Row]:
    """
    Executa o stored procedure no banco de dados com os parâmetros informados e retorna os erros que ocorrerem

    :param errors: lista a ser apensada com mensagem apropriada, em caso de erro
    :param nm_proced: nome do stored procedure
    :param param_vals: lista de valores dos parâmetros
    :return: lista de tuplas contendo o resultado da busca, ou [] se a busca resultar vazia
    """
    # inicializa a variável de retorno
    result: list[Row] = []

    try:
        with connect(__CONNECTION_KWARGS) as conn:
            # obtém o cursor e executa a operação
            with conn.cursor() as cursor:
                sql = f"SET NOCOUNT ON; EXEC {nm_proced} {','.join(('?',) * len(param_vals))}"
                cursor.execute(sql, param_vals)
                # obtem as tuplas retornadas
                for record in cursor:
                    result.append(record)

    except Exception as e:
        errors.append(__db_except_msg(e))

    return result


def db_select_all(errors: list[str], sel_stmt: str,
                  where_vals: tuple, required: bool = False) -> list[Row]:
    """
    Busca no banco de dados e retorna todas as tuplas que satisfaçam o comando de busca *sele_stmt*.
    O comando pode opcionalmente conter critérios de busca, com valores respectivos fornecidos
    em *where_vals*. A lista de valores para um atributo com a cláusula *IN* deve estar contida
    em tupla específica. Se a busca resultar vazia, uma lista vazia é retornado.

    :param errors: lista a ser apensada com mensagem apropriada, em caso de erro
    :param sel_stmt: comando SELECT para a busca
    :param where_vals: lista de valores a serem associados aos critérios de busca
    :param required: define se busca vazia deve ser considerada erro
    :return: lista de tuplas contendo o resultado da busca, ou [] se a busca resultar vazia
    """
    # inicializa a variável de retorno
    result: list[Row] = []

    exc: bool = False
    try:
        with connect(__CONNECTION_KWARGS) as conn:
            # obtem o cursor e executa a operação
            with conn.cursor() as cursor:
                cursor.execute(sel_stmt, where_vals)
                # obtem as tuplas retornadas
                for record in cursor:
                    result.append(record)
    except Exception as e:
        exc = True
        errors.append(__db_except_msg(e))

    # o parâmetro 'required' foi definido, não houve erros, e nenhum registro foi obtido ?
    if required and not exc and len(result) == 0:
        # sim, reporte o erro
        errors.append(__db_required_msg(sel_stmt, where_vals))

    return result


def db_insert(errors: list[str], insert_stmt: str, insert_vals: tuple) -> int:
    """
    Insere no banco de dados uma tupla com valores definidos em *insert_vals*.
    Retorna o id da tupla inserida.

    :param errors: lista a ser apensada com mensagem apropriada, em caso de erro
    :param insert_stmt: comando INSERT
    :param insert_vals: lista de valores a serem inseridos
    :return: o id da tupla inserida, ou None em caso de erro
    """
    result: int | None = None
    try:
        with connect(__CONNECTION_KWARGS) as conn:
            with conn.cursor() as cursor:
                insert_stmt = insert_stmt.replace("VALUES", "OUTPUT INSERTED.id VALUES")
                cursor.execute(insert_stmt, insert_vals)
                result = cursor.fetchone()[0]
    except Exception as e:
        errors.append(str(e))

    return result


def db_update(errors: list[str], update_stmt: str,
              update_vals: tuple, where_vals: tuple) -> int:
    """
    Atualiza uma ou mais tuplas no banco de dados, segundo as definições do comando
    *update_stmt*. Os valores para essa atualização estão em *update_vals*.
    Os valores para a seleção das tuplas a serem atualizadas estão em *where_vals*.
    Retorna o número de tuplas modificadas.

    :param errors: lista a ser apensada com mensagem apropriada, em caso de erro
    :param update_stmt: comando UPDATE
    :param update_vals: lista de valores para a atualização
    :param where_vals: lista de valores para os critérios de seleção de tuplas
    :return: o número de tuplas atualizadas
    """
    values: tuple = update_vals + where_vals
    return __db_modify(errors, update_stmt, values)


def db_delete(errors: list[str], delete_stmt: str, where_vals: tuple) -> int:
    """
    Exclui uma ou mais tuplas no banco de dados, segundo as definições do comando *delete_stmt*.
    Os valores para a seleção das tuplas a serem excluídas estão em *where_vals*.

    :param errors: lista a ser apensada com mensagem apropriada, em caso de erro
    :param delete_stmt: comando DELETE
    :param where_vals: lista de valores para os critérios de seleção de tuplas
    :return: o número de tuplas excluídas
    """
    return __db_modify(errors, delete_stmt, where_vals)


def db_row_to_dict(row):
    """
    Converts a pyodbc.Row object to a dictionary.

    :param row: A pyodbc.Row object.
    :return: A dictionary where keys are column names and values are the corresponding values in the row.
    """
    return {description[0]: row[i] for i, description in enumerate(row.cursor_description)}


def __db_modify(errors: list[str], modify_stmt: str, bind_vals: tuple) -> int:
    """
    Modifica o banco de dados, inserindo, atualizando ou excluindo tuplas, segundo as
    definições do comando *modify_stmt*. Os valores para essa modificação, seguidos dos
    valores para a seleção das tuplas, estão em *bind_vals*.

    :param errors: lista a ser apensada com mensagem apropriada, em caso de erro
    :param modify_stmt: comando INSERT, UPDATE ou DELETE
    :param bind_vals: lista de valores para modificação e seleção de tuplas
    :return: o número de tuplas inseridas, atualizadas ou excluídas, ou None em caso de erro
    """
    result: int | None = None

    try:
        with connect(__CONNECTION_KWARGS) as conn:
            # obtem o cursor e executa a operação
            with conn.cursor() as cursor:
                cursor.execute(modify_stmt, bind_vals)
                result = cursor.rowcount
                conn.commit()
    except Exception as e:
        errors.append(__db_except_msg(e))

    return result


# TODO
def __db_except_msg(exception: Exception) -> str:
    """
    Formata e retorna a mensagem de erro correspondente à exceção levantada no acesso
    ao banco de dados.

    :param exception: A exceção levantada
    :return: A mensagem de erro formatada
    """
    exc_msg: str = f"{exception}"
    exc_msg = exc_msg.replace('"', "'") \
                     .replace('\n', " ") \
                     .replace('\t', " ") \
                     .replace("\\", "")
    result = f"Error accessing {DB_NAME} at {DB_HOST}: {exc_msg}"

    return result


def __db_required_msg(sel_stmt: str, where_vals: tuple) -> str:
    """
    Formata e retorna a mensagem indicativa de busca vazia.

    :param sel_stmt: O comando de busca utilizado
    :param where_vals: a lista de valores constituindo os critérios de busca
    :return: mensagem indicativa de busca vazia
    """
    stmt: str = sel_stmt.replace('"', "'") \
                        .replace('\n', " ") \
                        .replace('\t', " ") \
                        .replace("\\", "")
    result: str = f"No record found in {DB_NAME} at {DB_HOST}, for {stmt}"

    for val in where_vals:
        if isinstance(val, str):
            val = f"'{val}'"
        else:
            val = str(val)
        result = result.replace("%s", val, 1)

    return result
