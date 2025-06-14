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


import re
from sqlparse import parse, format
from sqlparse.tokens import Name, Wildcard
from sqlparse.sql import Identifier, IdentifierList, Token


def pgsql_to_tp(engine, sql, tail="1"):
    # Get the column name mapping dictionary
    col_name_dict = get_table_name_dict(engine, tail)

    # This will store all the mappings we make
    reverse_mappings = {}

    # Parse the SQL statement
    parsed = parse(sql)[0]

    # Process each token in the parsed SQL
    for token in parsed.flatten():
        # Skip if not an identifier or wildcard
        if token.ttype not in (Name, Wildcard) or token.is_keyword:
            continue

        # Skip wildcard (*) tokens
        if token.value == '*':
            continue

        # Handle qualified identifiers (like table.column)
        if '.' in token.value:
            parts = token.value.split('.')
            if len(parts) != 2:
                continue

            table_ref, column_name = parts

            # Find the actual table name in our mapping
            # First check if the table_ref is an alias
            actual_table = None
            for table_pattern in col_name_dict:
                # Check if table_ref matches the full table name or is an alias
                # This requires tracking aliases which we'll do later
                if table_pattern == table_ref or table_pattern.endswith(f"_{tail}"):
                    # Check if this is the right table by seeing if column exists
                    if column_name in col_name_dict[table_pattern]:
                        actual_table = table_pattern
                        break

            # If we found a matching table and column, replace
            if actual_table and column_name in col_name_dict[actual_table]:
                new_column = col_name_dict[actual_table][column_name]
                token.value = f"{table_ref}.{new_column}"

                # Store the reverse mapping
                if table_ref not in reverse_mappings:
                    reverse_mappings[table_ref] = {}
                reverse_mappings[table_ref][new_column] = column_name

    # Convert back to string
    transformed_sql = str(parsed)

    # Now handle the alias tracking more precisely with regex
    # First find all table aliases in the FROM and JOIN clauses
    alias_map = {}
    from_join_matches = re.finditer(
        r'(?:FROM|JOIN)\s+([\w_]+(?:_\d+_1)?)(?:\s+([a-zA-Z_]\w*))?',
        sql,
        re.IGNORECASE
    )

    for match in from_join_matches:
        table_name = match.group(1)
        alias = match.group(2)
        if alias:
            alias_map[alias] = table_name

    # Now process column references that might use aliases
    def replace_column_ref(match):
        prefix = match.group(1) or ''
        column = match.group(2)

        # If no prefix, we can't determine the table, so leave as is
        if not prefix:
            return match.group(0)

        # Look up the actual table name from the alias map
        actual_table = alias_map.get(prefix)
        if not actual_table:
            return match.group(0)

        # Check if we have a mapping for this column
        if actual_table in col_name_dict and column in col_name_dict[actual_table]:
            new_column = col_name_dict[actual_table][column]

            # Store the reverse mapping
            if prefix not in reverse_mappings:
                reverse_mappings[prefix] = {}
            reverse_mappings[prefix][new_column] = column

            return f"{prefix}.{new_column}"
        else:
            return match.group(0)

    # Apply the replacement for column references
    transformed_sql = re.sub(
        r'([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)',
        replace_column_ref,
        transformed_sql
    )

    return transformed_sql, reverse_mappings


def restore_column_names(results, column_mappings):
    if isinstance(results, list) and all(isinstance(row, dict) for row in results):
        # Handle list of dictionaries (common for raw DB results)
        restored = []
        for row in results:
            new_row = {}
            for col, value in row.items():
                # Check if column is qualified (table.column)
                if '.' in col:
                    table_part, col_part = col.split('.')
                    if table_part in column_mappings and col_part in column_mappings[table_part]:
                        orig_col = column_mappings[table_part][col_part]
                        new_row[f"{table_part}.{orig_col}"] = value
                    else:
                        new_row[col] = value
                else:
                    # Unqualified column - try to find in any table
                    found = False
                    for table in column_mappings:
                        if col in column_mappings[table]:
                            orig_col = column_mappings[table][col]
                            new_row[orig_col] = value
                            found = True
                            break
                    if not found:
                        new_row[col] = value
            restored.append(new_row)
        return restored

    elif hasattr(results, 'columns') and hasattr(results, 'rename'):  # Pandas DataFrame
        # Handle pandas DataFrame
        new_columns = []
        for col in results.columns:
            if '.' in col:
                table_part, col_part = col.split('.')
                if table_part in column_mappings and col_part in column_mappings[table_part]:
                    new_columns.append(f"{table_part}.{column_mappings[table_part][col_part]}")
                else:
                    new_columns.append(col)
            else:
                # Unqualified column - try to find in any table
                found = False
                for table in column_mappings:
                    if col in column_mappings[table]:
                        new_columns.append(column_mappings[table][col])
                        found = True
                        break
                if not found:
                    new_columns.append(col)
        return results.rename(columns=dict(zip(results.columns, new_columns)))

    else:
        # Unsupported type, return as-is
        return results

# def pgsql_to_tp(engine, sql, tail="1"):
#     conn = engine.connect()
#     try:
#         # 获取表名映射 {table_name: tp_table_name} 和 {tp_table_name: table_id}
#         tables_result = conn.execute(sqlalchemy.text("""
#             SELECT table_id, table_name
#             FROM cdp_table
#         """))
#         table_name_map = {}
#         tp_table_to_id = {}
#         for table_row in tables_result:
#             table_id, table_name = table_row
#             tp_table_name = f"{table_name}_{table_id}_{tail}"
#             table_name_map[table_name] = tp_table_name
#             tp_table_to_id[tp_table_name] = table_id
#
#         # 获取列名映射 {table_id: {table_column_name: table_column_tm}}
#         columns_result = conn.execute(sqlalchemy.text("""
#             SELECT table_id, table_column_name, table_column_tm
#             FROM cdp_table_column
#         """))
#         column_map = {}
#         for col_row in columns_result:
#             table_id, col_name, col_tm = col_row
#             if table_id not in column_map:
#                 column_map[table_id] = {}
#             column_map[table_id][col_name] = col_tm
#
#         # 第一步：替换表名
#         # 匹配格式：表名 或 "表名"
#         table_pattern = re.compile(r'(\b\w+\b|"\w+")')
#
#         def replace_table(match):
#             table = match.group(1)
#             clean_table = table.strip('"')
#             return table_name_map.get(clean_table, table)
#
#         new_sql = table_pattern.sub(replace_table, sql)
#
#         # 第二步：构建表别名映射（包含原始表名和别名）
#         alias_map = {}
#         # 匹配格式：表名 [AS] 别名 或 "表名" [AS] "别名"
#         alias_pattern = re.compile(
#             r'\b(?:FROM|JOIN)\s+(\w+|"\w+")(?:\s+(?:AS\s+)?(\w+|"\w+"))?',
#             re.IGNORECASE
#         )
#
#         # 找出所有表别名定义
#         for match in alias_pattern.finditer(new_sql):
#             table_ref = match.group(1)
#             alias = match.group(2)
#
#             # 清理引号获取物理表名
#             clean_table_ref = table_ref.strip('"')
#
#             # 只处理转换后的表名（包含_{table_id}_格式）
#             if re.search(r'_\d+_' + tail + r'\b', clean_table_ref):
#                 # 物理表名自身作为引用
#                 alias_map[clean_table_ref] = clean_table_ref
#
#                 # 处理表别名
#                 if alias:
#                     clean_alias = alias.strip('"')
#                     alias_map[clean_alias] = clean_table_ref
#
#         # 第三步：替换列名（处理三种格式）
#         # 1. 别名.列名  2. 表名.列名  3. "表名"."列名"
#         col_pattern = re.compile(
#             r'(\b\w+\b|"\w+")\.(\b\w+\b|"\w+")',
#             re.IGNORECASE
#         )
#
#         def replace_col(match):
#             table_ref = match.group(1)  # 表名或别名（可能带引号）
#             col_ref = match.group(2)  # 列名（可能带引号）
#
#             # 清理引号获取实体名
#             clean_table_ref = table_ref.strip('"')
#             clean_col_ref = col_ref.strip('"')
#
#             # 通过别名映射找到物理表名
#             physical_table = alias_map.get(clean_table_ref)
#             if not physical_table:
#                 return match.group(0)  # 找不到物理表，不替换
#
#             # 从物理表名提取table_id
#             table_id = tp_table_to_id.get(physical_table)
#             if not table_id:
#                 return match.group(0)  # 找不到table_id，不替换
#
#             # 获取列映射
#             col_mapping = column_map.get(table_id, {})
#             new_col = col_mapping.get(clean_col_ref, clean_col_ref)
#
#             # 保留原始格式的引号
#             quote_table = '"' if table_ref.startswith('"') else ''
#             quote_col = '"' if col_ref.startswith('"') else ''
#
#             return f"{quote_table}{clean_table_ref}{quote_table}.{quote_col}{new_col}{quote_col}"
#
#         # 替换所有带表引用的列
#         return col_pattern.sub(replace_col, new_sql)
#
#     except Exception as e:
#         print(f"Error in SQL transformation: {e}")
#         raise
#     finally:
#         conn.close()


# def restore_column_names(engine, df, tail="1"):
#     """
#     Restores original column names in the DataFrame by reversing the transformations done by pgsql_to_tp,
#     focusing only on replacing the column reference portion after the dot.
#
#     Args:
#         engine: SQLAlchemy engine to connect to the database
#         df: DataFrame with transformed column names
#         tail: The tail suffix used in the original transformation
#
#     Returns:
#         DataFrame with column names restored to original names (only column references after dots are replaced)
#     """
#     conn = engine.connect()
#     try:
#         # Get column mappings {table_id: {table_column_tm: table_column_name}}
#         columns_result = conn.execute(sqlalchemy.text("""
#             SELECT table_id, table_column_name, table_column_tm
#             FROM cdp_table_column
#         """))
#         column_map = {}
#         for col_row in columns_result:
#             table_id, col_name, col_tm = col_row
#             column_map[col_tm] = col_name
#
#         # Process DataFrame column names
#         new_columns = []
#         for col in df.columns:
#             # Check if column is in format table.column
#             if '.' in col:
#                 table_ref, col_ref = col.split('.')
#                 # Only replace the column reference if it exists in our mapping
#                 original_col = column_map.get(col_ref, col_ref)
#                 new_columns.append(f"{table_ref}.{original_col}")
#             else:
#                 # For columns without table reference, try to find in column mapping
#                 new_columns.append(column_map.get(col, col))
#
#         # Create new DataFrame with renamed columns
#         df = df.copy()
#         df.columns = new_columns
#         return df
#
#     except Exception as e:
#         print(f"Error restoring column names: {e}")
#         raise
#     finally:
#         conn.close()
