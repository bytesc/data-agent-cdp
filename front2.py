import httpx
from utils.get_config import config_data

from pywebio.input import input, TEXT, textarea, actions, select
from pywebio.output import put_text, put_html, put_markdown, clear, put_loading, put_table, put_error, put_info, \
    put_warning
from pywebio import start_server


def ai_agent_api(request_data: dict, path: str = "/api/", url="http://127.0.0.1:" + str(config_data["server_port"])):
    # Use httpx to send request to the agent API
    with httpx.Client(timeout=180.0) as client:
        try:
            response = client.post(url + path, json=request_data)
            # Check response status code
            if response.status_code == 200:
                print(response.json())
                return response.json()
            else:
                return None
        except httpx.RequestError as e:
            print(e)
            # Handle request error
            return None


def main():
    while True:
        clear()
        put_markdown("## 数据分析流程")

        # 1. Get user question
        question = textarea("请输入您的问题:", rows=3)
        if not question:
            continue
        put_markdown(question)

        # 2. Get raw SQL
        put_markdown("正在获取原始SQL...")
        with put_loading():
            raw_sql_res = ai_agent_api({"question": question}, "/api/get-raw-sql/")

        raw_sql = raw_sql_res["ans"]
        put_markdown("## 1. 原始SQL")
        put_markdown("```sql\n" + raw_sql + "\n```")

        # 3. Translate SQL
        put_markdown("正在翻译SQL...")
        with put_loading():
            translate_res = ai_agent_api({"question": raw_sql}, "/api/translate-sql/")

        translated_sql = translate_res["ans"]
        put_markdown("## 2. 翻译后的SQL")
        put_markdown("```sql\n" + translated_sql + "\n```")

        # 4. Execute SQL
        put_markdown("正在执行SQL...")
        with put_loading():
            exec_res = ai_agent_api({"question": translated_sql}, "/api/exe-tp-sql/")

        put_markdown("## 3. SQL执行结果")
        put_markdown(str(exec_res["ans"]))

        # 5. Explain SQL
        put_markdown("正在解释SQL...")
        with put_loading():
            explain_res = ai_agent_api({
                "question": question,
                "ans": str(exec_res),
                "code": raw_sql # translated_sql
            }, "/api/explain-sql/")

        if explain_res:
            put_markdown("## 4. SQL解释")
            put_markdown(explain_res["ans"])
        else:
            put_warning("SQL解释失败")

        # 6. Ask if user wants to generate chart
        action = actions("", buttons=[
            {'label': '新的查询', 'value': 'new'},
            {'label': '生成图表', 'value': 'chart'},
            {'label': '退出', 'value': 'exit'}
        ])

        if action == 'chart':
            # Get chart
            put_markdown("正在生成图表...")
            with put_loading():
                chart_res = ai_agent_api({
                    "question": question,
                    "data": exec_res["ans"]
                }, "/api/get-echart/")

            if chart_res:
                put_markdown("## 5. 生成图表")
                put_markdown(chart_res["ans"])
            else:
                put_error("图表生成失败，请尝试其他问题或检查数据")

            if actions("", buttons=[{'label': '新的查询', 'value': 'new'},
                                    {'label': '退出', 'value': 'exit'}]) != 'new':
                break

        elif action == 'exit':
            break


# 启动 PyWebIO 应用
if __name__ == '__main__':
    start_server(main, port=8018, debug=True)