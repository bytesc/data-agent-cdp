import json
from typing import List, Tuple, Optional, Dict, Union

import pandas as pd
from agent.utils.llm_access.LLM import get_llm
from .copilot.sql_code import get_sql_code
from .copilot.tp_sql_code import map_sql_code, get_raw_sql_func
from .copilot.utils.read_tp_db import execute_tp_sql

from .tools_def import engine

llm = get_llm()

def get_raw_sql(question: str, df_cols: str | list = None) -> str:
    sql = get_raw_sql_func(question, df_cols, llm, engine)
    return sql


def translate_tp_sql(sql: str) -> str:
    tp_sql = map_sql_code(sql, llm, engine)
    return tp_sql


def exe_tp_sql(tp_sql: str) -> pd.DataFrame:
    df = execute_tp_sql(tp_sql.replace(';', ''))
    return df.to_dict()  # df.to_json()
