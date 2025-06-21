import logging

from .utils.call_llm_test import call_llm
from .utils.parse_output import parse_generated_sql_code
from .utils.pgsql_to_tp import get_tp_table_create
from .utils.read_db import execute_tp_sql

# tables = ['class', 'lesson_info',
#           'semester', 'stu_detail', 'stu_grade',
#           'teacher_detail', 'user_info']
tables = None


def get_db_info_prompt(engine, simple=False, example=False):
    data_prompt = """
Here is the structure of the database:
"""
    data_prompt += "\n```sql\n"+str(get_tp_table_create(engine))+"\n```\n"
#     if example:
#         data_prompt += """
# Here is data samples(just samples, do not mock any data):
# """
#         data_prompt += str(get_rows_from_all_tables(engine, tables, 3))
    return data_prompt


def get_sql_code(question, df_cols, llm, engine, retries=3):
    retries_times = 0
    error_msg = ""
    while retries_times <= retries:
        retries_times += 1
        pre_prompt = """
Please write SQL code to select the data needed according to the following requirements:
"""
        data_prompt = get_db_info_prompt(engine, example=True)
        if df_cols:
            data_prompt += "With output columns names: \n"
            data_prompt += str(df_cols) + "\n"

        end_prompt = """
Remind:
1. All code should be completed in a single markdown code block without any comments, explanations or cmds.
"""
        final_prompt = question + pre_prompt + "\n" + data_prompt + end_prompt

        ans = call_llm(final_prompt + error_msg, llm)
        print("sql################################")
        print(ans.content)
        result_sql = parse_generated_sql_code(ans.content)
        if result_sql is None:
            error_msg = """
code should only be in a md code block: 
```sql
# some sql code
```
without any additional comments, explanations or cmds !!!
"""
            print(ans + "No code was generated.")
            continue
        else:
            return result_sql


from .utils.pgsql_to_tp import get_table_name_dict,filter_identical_mappings
def map_sql_code(sql, llm, engine, retries=3):
    retries_times = 0
    error_msg = ""
    while retries_times <= retries:
        retries_times += 1
        pre_prompt = """
    Please replace some column names in the sql code. 
    From given original_table_column to target_table_column and keep the SQL semantics unchanged.
    column names map format:
    {table_name: {{original_table_column1: target_table_column1},{original_table_column2: target_table_column2}, ...}, ...}
    column names map:
    """
        map_prompt = filter_identical_mappings(get_table_name_dict(engine))
        print(map_prompt)

        end_prompt = """
    Remind:
    1. All code should be completed in a single markdown code block without any comments, explanations or cmds.
    2. The SQL given can be complicated and have certain functions, please replace all original_table_column with target_table_column and keep the SQL semantics unchanged.
    3. Output column names should be readable, you can add `AS` on final select.
    4. Some columns may not appear in the original_table_columns, you do not need to replace them, do not change the columns not in map.
    5. Return the original sql if no need to transfer.
    """
        final_prompt = pre_prompt + str(map_prompt) + "\n```sql\n"+sql+"\n```\n" + end_prompt

        ans = call_llm(final_prompt + error_msg, llm)
        print("sql################################3")
        print(ans.content)
        result_sql = parse_generated_sql_code(ans.content)
        if result_sql is None:
            error_msg = """
    code should only be in a md code block: 
    ```sql
    # some sql code
    ```
    without any additional comments, explanations or cmds !!!
    """
            print(ans + "No code was generated.")
            continue
        else:
            return result_sql


def query_tp_database_func(question, df_cols, llm, engine):
    sql = get_sql_code(question, df_cols, llm, engine)
    tp_sql = map_sql_code(sql, llm, engine)
    df = execute_tp_sql(tp_sql.replace(';', ''))
    return df
