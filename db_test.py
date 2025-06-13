import sqlalchemy
from sqlalchemy import text
from utils.get_config import config_data

# timeplus                                 1.4.1
# timeplus-connect                         0.8.17


pg_engine = sqlalchemy.create_engine(config_data["pgsql"])

tp_engine = sqlalchemy.create_engine(config_data["timeplus"])
#
# with tp_engine.connect() as connection:
#     # Example 1: Basic query
#     result = connection.execute(text("select * from table(cdp_attribute_2_1)"))
#     print("Basic query test:", result.fetchone())


with tp_engine.connect() as connection:
    # Example 1: Basic query
    result = connection.execute(text("""
SELECT t2.email, t1.strcol_1
FROM cdp_attribute_2_1 t1
JOIN member_master_4_1 t2 ON t1.userId = t2.userId
WHERE t1.nickName = '邓丽君'
"""))
    print("Basic query test:", result.fetchone())


with pg_engine.connect() as connection:
    # Example 1: Basic query
    result = connection.execute(text("select * from cdp_table"))
    print("Basic query test:", result.fetchone())
