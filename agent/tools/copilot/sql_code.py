import logging

from .utils.call_llm_test import call_llm
from .utils.parse_output import parse_generated_sql_code
from .utils.read_db import get_rows_from_all_tables, get_table_creation_statements, get_table_and_column_comments
from .utils.read_tp_db import execute_tp_sql, execute_sql

# tables = ['class', 'lesson_info',
#           'semester', 'stu_detail', 'stu_grade',
#           'teacher_detail', 'user_info']
tables = None


def get_db_info_prompt(engine, simple=False, example=False):
    data_prompt = """
Here is the structure of the database:
"""
    data_prompt += "\n```sql\n"+str(get_table_creation_statements(engine, tables, simple))+"\n```\n"
    data_prompt += """
Here is the table and column comments:
"""
    data_prompt += str(get_table_and_column_comments(engine, tables))
    if example:
        data_prompt += """
Here is data samples(just samples, do not mock any data):
"""
        data_prompt += str(get_rows_from_all_tables(engine, tables, 3))
    return data_prompt


def get_sql_code(question, df_cols, llm, engine, retries=3, get_db_info_prompt=get_db_info_prompt):
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

def query_database_func(question, df_cols, llm, engine):
    sql = get_sql_code(question, df_cols, llm, engine)
    df = execute_sql(sql, engine)
    return df


