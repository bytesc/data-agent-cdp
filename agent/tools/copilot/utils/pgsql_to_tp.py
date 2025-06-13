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



import re


def pgsql_to_tp(engine, sql, tail="1"):
    conn = engine.connect()
    try:
        # 获取表名映射 {table_name: tp_table_name} 和 {tp_table_name: table_id}
        tables_result = conn.execute(sqlalchemy.text("""
            SELECT table_id, table_name
            FROM cdp_table
        """))
        table_name_map = {}
        tp_table_to_id = {}
        for table_row in tables_result:
            table_id, table_name = table_row
            tp_table_name = f"{table_name}_{table_id}_{tail}"
            table_name_map[table_name] = tp_table_name
            tp_table_to_id[tp_table_name] = table_id

        # 获取列名映射 {table_id: {table_column_name: table_column_tm}}
        columns_result = conn.execute(sqlalchemy.text("""
            SELECT table_id, table_column_name, table_column_tm
            FROM cdp_table_column
        """))
        column_map = {}
        for col_row in columns_result:
            table_id, col_name, col_tm = col_row
            if table_id not in column_map:
                column_map[table_id] = {}
            column_map[table_id][col_name] = col_tm

        # 第一步：替换表名
        # 匹配格式：表名 或 "表名"
        table_pattern = re.compile(r'(\b\w+\b|"\w+")')

        def replace_table(match):
            table = match.group(1)
            clean_table = table.strip('"')
            return table_name_map.get(clean_table, table)

        new_sql = table_pattern.sub(replace_table, sql)

        # 第二步：构建表别名映射
        alias_map = {}
        # 匹配格式：表名 [AS] 别名
        alias_pattern = re.compile(
            r'\b(?:FROM|JOIN)\s+(\w+)(?:\s+(?:AS\s+)?\b(\w+)\b)?',
            re.IGNORECASE
        )

        # 找出所有表别名定义
        for match in alias_pattern.finditer(new_sql):
            table_ref = match.group(1)
            alias = match.group(2)

            # 如果表名是转换后的表名（包含_{table_id}_格式）
            if re.search(r'_\d+_' + tail + r'\b', table_ref):
                # 表别名映射到物理表名
                if alias:
                    alias_map[alias] = table_ref
                # 表名自身也作为引用
                alias_map[table_ref] = table_ref

        # 第三步：替换列名
        # 匹配格式：别名.列名 或 表名.列名
        col_pattern = re.compile(
            r'(\b\w+\b)\s*\.\s*(\b\w+\b)',
            re.IGNORECASE
        )

        def replace_col(match):
            table_ref = match.group(1)
            col_ref = match.group(2)

            # 通过别名找到物理表名
            physical_table = alias_map.get(table_ref)
            if not physical_table:
                return match.group(0)  # 找不到物理表，不替换

            # 从物理表名提取table_id
            table_id = tp_table_to_id.get(physical_table)
            if not table_id:
                return match.group(0)  # 找不到table_id，不替换

            # 获取列映射
            col_mapping = column_map.get(table_id, {})
            new_col = col_mapping.get(col_ref, col_ref)

            return f"{table_ref}.{new_col}"

        # 替换所有带表引用的列
        return col_pattern.sub(replace_col, new_sql)

    except Exception as e:
        print(f"Error in SQL transformation: {e}")
        raise
    finally:
        conn.close()


def restore_column_names(engine, df, tail="1"):
    """
    Restores original column names in the DataFrame by reversing the transformations done by pgsql_to_tp.

    Args:
        engine: SQLAlchemy engine to connect to the database
        df: DataFrame with transformed column names
        tail: The tail suffix used in the original transformation

    Returns:
        DataFrame with column names restored to original names
    """
    conn = engine.connect()
    try:
        # Get the original table and column mappings
        # 1. Get table mappings {tp_table_name: (table_id, original_table_name)}
        tables_result = conn.execute(sqlalchemy.text("""
            SELECT table_id, table_name
            FROM cdp_table
        """))
        tp_table_map = {}
        for table_row in tables_result:
            table_id, table_name = table_row
            tp_table_name = f"{table_name}_{table_id}_{tail}"
            tp_table_map[tp_table_name] = (table_id, table_name)

        # 2. Get column mappings {table_id: {table_column_tm: table_column_name}}
        columns_result = conn.execute(sqlalchemy.text("""
            SELECT table_id, table_column_name, table_column_tm
            FROM cdp_table_column
        """))
        reverse_column_map = {}
        for col_row in columns_result:
            table_id, col_name, col_tm = col_row
            if table_id not in reverse_column_map:
                reverse_column_map[table_id] = {}
            reverse_column_map[table_id][col_tm] = col_name

        # Process DataFrame column names
        new_columns = []
        for col in df.columns:
            # Check if column is in format table.column
            if '.' in col:
                table_ref, col_ref = col.split('.')

                # Get table info
                table_info = tp_table_map.get(table_ref)
                if not table_info:
                    new_columns.append(col)
                    continue

                table_id, original_table_name = table_info

                # Get original column name
                col_mapping = reverse_column_map.get(table_id, {})
                original_col = col_mapping.get(col_ref, col_ref)

                new_columns.append(f"{original_table_name}.{original_col}")
            else:
                # For columns without table reference, try to find in any table
                original_col = col
                for table_id, col_mapping in reverse_column_map.items():
                    if col in col_mapping:
                        original_col = col_mapping[col]
                        break
                new_columns.append(original_col)

        # Create new DataFrame with renamed columns
        df = df.copy()
        df.columns = new_columns
        return df

    except Exception as e:
        print(f"Error restoring column names: {e}")
        raise
    finally:
        conn.close()


