import sqlalchemy


def get_tp_table_create(engine, tail="1"):
    conn = engine.connect()
    try:
        # 获取所有表定义
        tables_result = conn.execute(sqlalchemy.text("""
            SELECT table_id, table_name, table_alias, table_business_type
            FROM cdp_table
        """))

        table_definitions = {}

        for table_row in tables_result:
            table_id, table_name, table_alias, table_business_type = table_row

            # 获取表的列定义
            columns_result = conn.execute(sqlalchemy.text("""
                SELECT 
                    table_column_name, 
                    data_type,
                    short_data_type,
                    data_type_name,
                    is_pk,
                    "comment",
                    table_column_alias,
                    table_column_tm,
                    col_type
                FROM cdp_table_column
                WHERE table_id = :table_id
                ORDER BY table_column_id
            """), {"table_id": table_id})

            columns = []
            primary_keys = []

            for col_row in columns_result:
                col_name, data_type, short_data_type, data_type_name, \
                is_pk, comment, col_alias, table_column_tm, col_type = col_row

                if data_type == 1 and short_data_type == 1:
                    data_type = "STRING"
                elif data_type == 2 and short_data_type == 2:
                    data_type = "INT64"
                elif data_type == 3 and short_data_type == 2:
                    data_type = "INT64"
                elif data_type == 4 and short_data_type == 3:
                    data_type = "DECIMAL(64,2)"
                elif data_type == 5 and short_data_type == 4:
                    data_type = "DATE"
                elif data_type == 6 and short_data_type == 5:
                    data_type = "DATETIME"
                elif data_type == 7 and short_data_type == 5:
                    data_type = "DATETIME"
                else:
                    data_type = "STRING"  # default fallback

                # 构建列定义
                col_def = f"{col_name} {data_type}"

                # 添加中文数据类型作为注释
                col_def += f" /* {data_type_name} */"

                # 添加列别名（跳过空字符串、null和纯空格）
                if col_alias and str(col_alias).strip():
                    col_def += f" /* {col_alias.strip()} */"

                # 添加注释（跳过空字符串、null和纯空格）
                if comment and str(comment).strip():
                    col_def += f" /* {comment.strip()} */"

                # 如果是主键，记录下来
                if is_pk == 1:
                    primary_keys.append(col_name)

                columns.append(col_def)

            # 构建完整的CREATE TABLE语句
            tp_table_name = f"{table_name}_{table_id}_{tail}"
            create_sql = f"CREATE TABLE {tp_table_name} (\n"
            create_sql += ",\n".join([f"    {col}" for col in columns])

            # 添加主键约束
            if primary_keys:
                create_sql += f",\n    CONSTRAINT {tp_table_name}_pkey PRIMARY KEY ({', '.join(primary_keys)})"

            create_sql += "\n);"

            # 添加表注释
            create_sql += f"\nCOMMENT ON TABLE {tp_table_name} IS '{table_alias} - {table_business_type}';\n"

            table_definitions[tp_table_name] = create_sql

        return table_definitions

    except Exception as e:
        print(f"Error generating table definitions: {e}")
        raise e
    finally:
        conn.close()


def get_table_name_dict(engine, tail="1"):
    conn = engine.connect()
    try:
        # 获取表名映射 {table_name: tp_table_name} 和 {tp_table_name: table_id}
        tables_result = conn.execute(sqlalchemy.text("""
                SELECT table_id, table_name
                FROM cdp_table
            """))

        # 保留原有的表名拼接方式
        table_name_map = {}  # {原始表名: 拼接后表名}
        tp_table_to_id = {}  # {拼接后表名: table_id}
        table_id_to_name = {}  # {table_id: 原始表名} 用于后续映射

        for table_row in tables_result:
            table_id, table_name = table_row
            tp_table_name = f"{table_name}_{table_id}_{tail}"
            table_name_map[table_name] = tp_table_name
            tp_table_to_id[tp_table_name] = table_id
            table_id_to_name[table_id] = table_name

        # 获取列名映射 {table_id: {table_column_name: table_column_tm}}
        columns_result = conn.execute(sqlalchemy.text("""
                SELECT table_id, table_column_name, table_column_tm
                FROM cdp_table_column
            """))

        # 先按table_id组织列数据
        column_map_by_id = {}
        for col_row in columns_result:
            table_id, col_name, col_tm = col_row
            if table_id not in column_map_by_id:
                column_map_by_id[table_id] = {}
            column_map_by_id[table_id][col_name] = col_tm

        # 转换为最终格式 {拼接后表名: {列名: 列名tm}}
        result = {}
        for tp_table_name, table_id in tp_table_to_id.items():
            if table_id in column_map_by_id:
                result[tp_table_name] = column_map_by_id[table_id]

        return result

    except Exception as e:
        print(f"Error in SQL transformation: {e}")
        raise
    finally:
        conn.close()


def filter_identical_mappings(table_dict):
    filtered_dict = {}

    for table_name, column_map in table_dict.items():
        filtered_columns = {}

        for col_name, col_tm in column_map.items():
            if col_name != col_tm:  # Only keep if key and value are different
                filtered_columns[col_name] = col_tm

        # Only add the table if it has any filtered columns
        if filtered_columns:
            filtered_dict[table_name] = filtered_columns

    return filtered_dict


import re
from sqlparse import parse, format
from sqlparse.tokens import Name, Wildcard
from sqlparse.sql import Identifier, IdentifierList, Token


def pgsql_to_tp(engine, sql):
   pass
    # return transformed_sql




