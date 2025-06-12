import sqlalchemy

def get_tp_table_create(engine, tail="_1"):
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
                col_name, data_type, is_pk, comment, col_alias, col_type = col_row

                # 构建列定义
                col_def = f"{col_name} {data_type}"

                # 添加注释（跳过空字符串、null和纯空格）
                if comment and str(comment).strip():
                    col_def += f" /* {comment.strip()} */"

                # 如果是主键，记录下来
                if is_pk == 1:
                    primary_keys.append(col_name)

                columns.append(col_def)

            # 构建完整的CREATE TABLE语句
            create_sql = f"CREATE TABLE {table_name} (\n"
            create_sql += ",\n".join([f"    {col}" for col in columns])

            # 添加主键约束
            if primary_keys:
                create_sql += f",\n    CONSTRAINT {table_name}_pkey PRIMARY KEY ({', '.join(primary_keys)})"

            create_sql += "\n);"

            # 添加表注释
            create_sql += f"\nCOMMENT ON TABLE {table_name} IS '{table_alias} - {table_business_type}';\n"

            # 添加列注释（跳过空字符串、null和纯空格）
            columns_result = conn.execute(sqlalchemy.text("""
                SELECT 
                    table_column_name, 
                    "comment"
                FROM cdp_table_column
                WHERE table_id = :table_id AND "comment" IS NOT NULL AND "comment" != '' AND TRIM("comment") != ''
                ORDER BY table_column_id
            """), {"table_id": table_id})

            for col_row in columns_result:
                col_name, comment = col_row
                create_sql += f"COMMENT ON COLUMN {table_name}.{col_name} IS '{comment}';\n"

            table_definitions[table_name] = create_sql

        return table_definitions

    except Exception as e:
        print(f"Error generating table definitions: {e}")
        raise e
    finally:
        conn.close()

