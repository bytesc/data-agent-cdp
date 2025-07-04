import pandas as pd
import sqlalchemy

from agent.utils.get_config import config_data
from agent.utils.llm_access.LLM import get_llm
from .copilot.sql_code import get_sql_code, query_database_func, explain_sql_func

DATABASE_URL = config_data['pgsql']
engine = sqlalchemy.create_engine(DATABASE_URL)

STATIC_URL = config_data['static_path']

llm = get_llm()

from .copilot.python_code import draw_graph_func, draw_echart_block_func, draw_echart_file_func


def draw_graph(question: str, data: pd.DataFrame) -> str:
    """
    draw_graph(question: str, data: pd.DataFrame) -> str:
    Draw graph based on natural language graph type and data provided in a pandas DataFrame.
    Returns an url path string of the graph.

    Args:
    - question (str): Natural language graph type.
    - data (pd.DataFrame): A pandas DataFrame for providing drawing data.

    Returns:
    - str: url path string of the output graph.(e.g. "http://127.0.0.1:8003/tmp_imgs/mlkjcvep.png").

    Example:
    ```python
        data = pd.DataFrame({
            '月份': ['1月', '2月', '3月', '4月', '5月'],
            '销售额': [200, 220, 250, 210, 230]
        })
        graph_url = draw_graph("画折线图", data)
        # Output(str):
        # "http://127.0.0.1:8003/tmp_imgs/ekhidpcl.png"
    ```
    """
    result = draw_graph_func(question, data, llm)
    result = STATIC_URL + result[2:]
    return result


def draw_echart_block(question: str, data: pd.DataFrame) -> str:
    """

    """
    result = draw_echart_block_func(question, data, llm)
    return result


def draw_echart_file(question: str, data: pd.DataFrame) -> str:
    """

    """
    result = draw_echart_file_func(question, data, llm)
    result = STATIC_URL + result[2:]
    return result


def query_database(question: str, df_cols: str | list = None) -> pd.DataFrame:
    """
    query_database(question: str, df_cols: str | list = None) -> pd.DataFrame:
    Query the database using natural language question. Can not query anything not included in the database content!!!
    Returns the query results in pandas DataFrame.

    Args:
    - question (str): Natural language question, table names can be included in the question.
    - df_cols (str|list): The columns' names of the DataFrame(e.g. "uid, username, stu_num").

    Returns:
    - pd.DataFrame: A DataFrame containing the results of the database query.
        The DataFrame includes the columns provided in df_cols(the second args)

    Example:
    ```python
        ans_df = query_database('Select the grades of Jane Smith, use table stu_grade ,stu_info, class_info', df_cols='lesson_id, lesson_name, grade')
        # Output(pd.DataFrame):
        #        lesson_id lesson_name grade
        # 0        001  Mathematics     99.00
        # 1        002      English     88.50
        # 2        003     Physics    65.00
        # ... and so on(the structure of the output DataFrame id based on df_cols(the second input args))
    ```
    """

    df = query_database_func(question, df_cols, llm, engine)
    return df


def explain_sql(question: str, sql: str, ans: pd.DataFrame|None) -> str:
    ans = explain_sql_func(question, sql, ans, llm)
    return ans
