def sample_df_if_large(df, max_length=8000):

    # 将DataFrame转换为字符串并计算长度
    df_str = df.to_string()

    if len(df_str) > max_length:
        print("DataFrame字符串长度超过限制，进行采样...")
        # 计算采样间隔
        n = len(df)
        sample_interval = max(1, n // 50)  # 确保至少为1

        # 使用iloc进行间隔采样
        sampled_df = df.iloc[::sample_interval].head(50)
        return sampled_df

    return df
