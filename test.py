import sqlalchemy
from agent.tools.tools_def import llm
from utils.get_config import config_data

pg_engine = sqlalchemy.create_engine(config_data["pgsql"])

tp_engine = sqlalchemy.create_engine(config_data["timeplus"])

from agent.tools.copilot.utils.pgsql_to_tp import get_tp_table_create, pgsql_to_tp,\
    restore_column_names, get_table_name_dict

table_dict = get_table_name_dict(pg_engine)

ans = get_tp_table_create(pg_engine)
result = ''
for value in ans.values():
    result += value
    result += "\n"
print(result)

from agent.tools.copilot.sql_code import get_sql_code

sql = get_sql_code("查询昵称为邓丽君的用户的邮箱归属地", None, llm, pg_engine)
# sql="""
# SELECT m.email, a.city
# FROM member_master_4_1 m
# JOIN cdp_attribute_2_1 a ON m.userId = a.userId
# WHERE a.nickName = '邓丽君';
# """
# sql="""
# SELECT *
# FROM cdp_attribute_2_1
# WHERE cdp_attribute_2_1.nickName  = '邓丽君';
# """


tp_sql, r_map = pgsql_to_tp(pg_engine, sql)
print(tp_sql)

from agent.tools.copilot.utils.read_db import execute_sql

df = execute_sql(sql.replace(';', ''))

print(df)

# ans_df = restore_column_names(pg_engine, df)
ans_df = restore_column_names(df, r_map)

print(ans_df)
