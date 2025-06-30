import logging

from .utils.call_llm_test import call_llm
from .utils.parse_output import parse_generated_sql_code
from .utils.read_db import get_rows_from_all_tables, get_table_creation_statements, get_table_and_column_comments, \
    execute_select
from .utils.read_db import execute_sql
from ...utils.df_process import sample_df_if_large

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
            print(ans.content + "No code was generated.")
            continue
        else:
            return result_sql


def query_database_func(question, df_cols, llm, engine, retries=2):
    exp = None
    for i in range(retries):
        err_msg = ""
        for j in range(retries):
            sql = get_sql_code(question + err_msg, df_cols, llm, engine)
            # print(sql)
            if sql is None:
                continue
            try:
                result = execute_select(engine, sql)
                logging.info(f"query_database_SQL: {sql}\nQuestion: {question}\nResult: {result}\n")
                return result
            except Exception as e:
                err_msg = str(e)
                exp = e
                print(e)
                continue
    return None



def explain_sql_func(question, sql, ans, llm) -> str:
    pre_prompt = """
    Please explain the sql provided code and query result according to the question. 
    Here is the background question:
    """

    sql_prompt = """
    Here is the sql code: 
    """

    data_prompt = """
    Here is the query result:
    """
    if ans is not None and not ans.empty:
        data_prompt += str(sample_df_if_large(ans))
    else:
        data_prompt += """
        The query return None
        """

    end_prompt = """
    
    Remind:
    1. The explanation should be short and clear. 
    2. The answer should only have to parts: `Query Process:` and `Query Result`.
    3. You need to tell me what tables and columns are used in `Query Process`.
    4. You need to summarize and explain the `Query Result` with only a few sentences.
    5. The question may contain other information and steps, please just focus on the data query part and ignore other parts such as draw graph. 
    6. You show give the answer in the same language with the background question.
    
    For Example:
    
    Query Process:
    - Join `tableA` with `tableB` on `W`
    - get `X` and `Y` as `hello` from `tableA` , get `Z` ans `zz` from `tableB`
    
    Query Result:
    The result shows ...
    
    """
    final_prompt = pre_prompt + question + sql_prompt + "```sql\n"+sql+"```\n" + data_prompt + end_prompt

    ans = call_llm(final_prompt, llm)
    return ans.content


