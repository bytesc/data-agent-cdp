import pandas as pd
from sqlalchemy import text, inspect
from sqlalchemy.exc import SQLAlchemyError

tables_data = None

import sqlalchemy
from sqlalchemy import text
from utils.get_config import config_data


tp_engine = sqlalchemy.create_engine(config_data["timeplus"])


def execute_tp_sql(sql):
    try:
        with tp_engine.connect() as connection:
            # 使用 pandas 的 read_sql_query 直接返回 DataFrame
            df = pd.read_sql_query(text(sql), connection)
            return df
    except Exception as e:
        print(e)
        # 创建一个包含错误信息的DataFrame
        error_df = pd.DataFrame({
            'error': [str(e)],
            'sql': [sql]
        })
        return error_df


def get_tp_tables():
    with tp_engine.connect() as connection:
        # 查询 Timeplus 中的表名
        # 根据 Timeplus 的实际系统表调整此查询
        query = """
        SELECT name 
        FROM system.tables
        """
        result = connection.execute(text(query))
        tables = [row[0] for row in result.fetchall()]
        return tables




