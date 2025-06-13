import pandas as pd
import sqlalchemy

from agent.utils.get_config import config_data
from agent.utils.llm_access.LLM import get_llm

DATABASE_URL = config_data['pgsql']
engine = sqlalchemy.create_engine(DATABASE_URL)

STATIC_URL = config_data['static_path']

llm = get_llm()

from .copilot.python_code import draw_graph_func


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




