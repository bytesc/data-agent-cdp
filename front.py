import httpx
from utils.get_config import config_data

from pywebio.input import input, TEXT, textarea, actions
from pywebio.output import put_text, put_html, put_markdown, clear, put_loading, put_table, put_error, put_info, \
    put_warning
from pywebio import start_server


def ai_agent_api(question: str, path: str = "/api/", url="http://127.0.0.1:" + str(config_data["server_port"])):
    # 使用 httpx 发送请求到另一个服务器的 /ask-agent/ 接口
    with httpx.Client(timeout=180.0) as client:
        try:
            response = client.post(url+path, json={"question": question})
            # 检查响应状态码
            if response.status_code == 200:
                print(response.json()["ans"])
                return response.json()["ans"]
            else:
                return None
        except httpx.RequestError as e:
            print(e)
            # 处理请求错误
            return None


def display_editable_step(title, default_content=""):
    put_markdown(f"## {title}")
    return textarea("可直接编辑内容:", value=default_content, rows=10)


def main():
    history = []
    while True:
        clear()
        put_markdown("## 数据分析流程")

        # 1. 获取用户问题
        question = textarea("请输入您的问题:", rows=3)
        if not question:
            continue
        put_markdown(question)

        # 2. 获取原始SQL
        put_markdown("正在获取原始SQL...")
        with put_loading():
            raw_sql_res = ai_agent_api(question, "/api/get-raw-sql/")

        raw_sql = raw_sql_res
        raw_sql = display_editable_step("1. 原始SQL", raw_sql)

        put_markdown("```sql\n"+raw_sql+"\n```")

        # 3. 翻译SQL
        put_markdown("正在翻译SQL...")
        with put_loading():
            translate_res = ai_agent_api(raw_sql, "/api/translate-sql/")

        translated_sql = translate_res
        translated_sql = display_editable_step("2. 翻译后的SQL", translated_sql)

        put_markdown("```sql\n" + translated_sql + "\n```")

        # 4. 执行SQL
        put_markdown("正在执行SQL...")
        with put_loading():
            exec_res = ai_agent_api(translated_sql, "/api/exe-sql/")

        exec_result = exec_res
        put_markdown("3. SQL执行结果")
        put_markdown(str(exec_res))

        # 记录历史
        history.append({
            "question": question,
            "raw_sql": raw_sql,
            "translated_sql": translated_sql,
            "exec_result": exec_result
        })

        if actions("", buttons=[{'label': '新的查询', 'value': 'new'}, {'label': '查看历史', 'value': 'history'},
                                {'label': '退出', 'value': 'exit'}]) != 'new':
            break

    # 显示历史记录
    if history:
        clear()
        put_markdown("## 历史查询记录")
        put_table([
            ['时间', '问题', '原始SQL', '执行结果'],
            *[[h['time'], h['question'], h['raw_sql'][:50] + "...", h['exec_result']] for h in history]
        ])


# 启动 PyWebIO 应用
if __name__ == '__main__':
    start_server(main, port=8016, debug=True)
