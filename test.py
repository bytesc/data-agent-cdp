import sqlalchemy
from sqlalchemy import text
from utils.get_config import config_data

pg_engine = sqlalchemy.create_engine(config_data["pgsql"])

tp_engine = sqlalchemy.create_engine(config_data["timeplus"])

from agent.tools.copilot.utils.pgsql_to_tp import get_tp_table_create

ans = get_tp_table_create(pg_engine)
result = ''
for value in ans.values():
    result += value
    result += "\n"
print(result)
