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
                    col_type
                FROM cdp_table_column
                WHERE table_id = :table_id
                ORDER BY table_column_id
            """), {"table_id": table_id})

            columns = []
            primary_keys = []

            for col_row in columns_result:
                col_name, data_type, short_data_type, data_type_name, is_pk, comment, col_alias, col_type = col_row

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
