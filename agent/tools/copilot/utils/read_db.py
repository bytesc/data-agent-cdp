import pandas as pd
from sqlalchemy import text, inspect
from sqlalchemy.exc import SQLAlchemyError

tables_data = None

import sqlalchemy
from sqlalchemy import text
from utils.get_config import config_data


tp_engine = sqlalchemy.create_engine(config_data["timeplus"])

def execute_sql(sql):
    with tp_engine.connect() as connection:
        # 使用 pandas 的 read_sql_query 直接返回 DataFrame
        df = pd.read_sql_query(text(sql), connection)
        return df

