import sqlalchemy
from agent.tools.tools_def import llm
from utils.get_config import config_data

pg_engine = sqlalchemy.create_engine(config_data["pgsql"])

tp_engine = sqlalchemy.create_engine(config_data["timeplus"])

from agent.tools.copilot.utils.pgsql_to_tp import get_tp_table_create, restore_column_names

ans = get_tp_table_create(pg_engine)
result = ''
for value in ans.values():
    result += value
    result += "\n"
print(result)

from agent.tools.copilot.sql_code import get_sql_code

sql = get_sql_code("查询昵称为邓丽君的用户的邮箱归属地", None, llm, pg_engine)

from agent.tools.copilot.utils.pgsql_to_tp import pgsql_to_tp

tp_sql = pgsql_to_tp(pg_engine, sql)
print(tp_sql)

from agent.tools.copilot.utils.read_db import execute_sql

df = execute_sql(tp_sql.replace(';', ''))

print(df)

ans_df = restore_column_names(pg_engine, df)


print(ans_df)
