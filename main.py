import mimetypes

import pandas as pd
import sqlalchemy
import uvicorn
import os
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.responses import JSONResponse

from agent.cot_chat import get_cot_chat
from agent.tools.custom_tools_def import exe_tp_sql, translate_tp_sql, get_raw_sql
from agent.tools.tools_def import query_database, draw_graph, draw_echart_file
from agent.utils.pd_to_walker import pd_to_walker
from utils.get_config import config_data

from agent.agent import exe_cot_code, get_cot_code, cot_agent
from agent.summary import get_ans_summary
from agent.ans_review import get_ans_review


app = FastAPI()

STATIC_FOLDER = "tmp_imgs"
STATIC_PATH = f"/{STATIC_FOLDER}"
# http://127.0.0.1:8003/tmp_imgs/mlkjcvep.png
@app.get(f"/{STATIC_FOLDER}/{{filename}}")
async def read_static_file(request: Request, filename: str):
    filepath = os.path.join(STATIC_FOLDER, filename)
    if os.path.isfile(filepath):
        # 猜测文件的MIME类型
        content_type, _ = mimetypes.guess_type(filepath)
        if content_type is None:
            content_type = "application/octet-stream"  # 默认为二进制流，如果无法确定类型
        # 读取文件内容
        with open(filepath, "rb") as file:
            file_content = file.read()
        # 返回Response对象，文件内容作为字节流发送
        return Response(content=file_content, media_type=content_type)
    else:
        return {"error": "File not found"}


class AgentInput(BaseModel):
    question: str

class AgentInputDict(BaseModel):
    question: str
    data: dict

class ReviewInput(BaseModel):
    question: str
    ans: str
    code: str


@app.post("/api/ask-agent/")
async def ask_agent(request: Request, user_input: AgentInput):
    ans, map = cot_agent(user_input.question)
    print(ans)
    if ans:
        processed_data = {
            "question": user_input.question,
            "ans": ans,
            "map": map,
            "type": "success",
            "msg": "处理成功"
        }
    else:
        processed_data = {
            "question": user_input.question,
            "ans": "",
            "map": "",
            "type": "error",
            "msg": "处理失败，请换个问法吧"
        }
    return JSONResponse(content=processed_data)


@app.post("/api/exe-code/")
async def exe_code(request: Request, user_input: AgentInput):
    ans = exe_cot_code(user_input.question)
    print(ans)
    if ans:
        processed_data = {
            "question": user_input.question,
            "ans": ans,
            "type": "success",
            "msg": "处理成功"
        }
    else:
        processed_data = {
            "question": user_input.question,
            "ans": "",
            "type": "error",
            "msg": "处理失败，请换个问法吧"
        }
    return JSONResponse(content=processed_data)


@app.post("/api/get-code/")
async def get_code(request: Request, user_input: AgentInput):
    code = get_cot_code(user_input.question)
    print(code)
    if code:
        processed_data = {
            "question": user_input.question,
            "code": code,
            "type": "success",
            "msg": "处理成功"
        }
    else:
        processed_data = {
            "question": user_input.question,
            "code": "",
            "type": "error",
            "msg": "处理失败，请换个问法吧"
        }
    return JSONResponse(content=processed_data)


@app.post("/api/review/")
async def get_review(request: Request, user_input: ReviewInput):
    ans = get_ans_review(user_input.question, user_input.ans, user_input.code)
    print(ans)
    if ans:
        processed_data = {
            "question": user_input.question,
            "ans": ans,
            "type": "success",
            "msg": "处理成功"
        }
    else:
        processed_data = {
            "question": user_input.question,
            "ans": "",
            "type": "error",
            "msg": "处理失败，请换个问法吧"
        }
    return JSONResponse(content=processed_data)


@app.post("/api/agent-summary/")
async def agent_summary(request: Request, user_input: AgentInput):
    ans = get_ans_summary(user_input.question)
    print(ans)
    if ans:
        processed_data = {
            "question": user_input.question,
            "ans": ans,
            "type": "success",
            "msg": "处理成功"
        }
    else:
        processed_data = {
            "question": user_input.question,
            "ans": "",
            "type": "error",
            "msg": "处理失败，请换个问法吧"
        }
    return JSONResponse(content=processed_data)


@app.post("/api/cot-chat/")
async def cot_chat(request: Request, user_input: AgentInput):
    ans = get_cot_chat(user_input.question)
    print(ans)
    if ans:
        processed_data = {
            "question": user_input.question,
            "ans": ans,
            "type": "success",
            "msg": "处理成功"
        }
    else:
        processed_data = {
            "question": user_input.question,
            "ans": "",
            "type": "error",
            "msg": "处理失败，请换个问法吧"
        }
    return JSONResponse(content=processed_data)



@app.post("/api/query-db/")
async def query_db(request: Request, user_input: AgentInput):
    ans = query_database(user_input.question)
    print(ans)
    if ans:
        processed_data = {
            "question": user_input.question,
            "ans": ans,
            "type": "success",
            "msg": "处理成功"
        }
    else:
        processed_data = {
            "question": user_input.question,
            "ans": "",
            "type": "error",
            "msg": "处理失败，请换个问法吧"
        }
    return JSONResponse(content=processed_data)

@app.post("/api/get-raw-sql/")
async def get_sql(request: Request, user_input: AgentInput):
    ans = get_raw_sql(user_input.question)
    if ans:
        processed_data = {
            "question": user_input.question,
            "ans": ans,
            "type": "success",
            "msg": "处理成功"
        }
    else:
        processed_data = {
            "question": user_input.question,
            "ans": "",
            "type": "error",
            "msg": "处理失败，请换个问法吧"
        }
    return JSONResponse(content=processed_data)

@app.post("/api/translate-sql/")
async def translate_sql(request: Request, user_input: AgentInput):
    ans = translate_tp_sql(user_input.question)
    if ans:
        processed_data = {
            "question": user_input.question,
            "ans": ans,
            "type": "success",
            "msg": "处理成功"
        }
    else:
        processed_data = {
            "question": user_input.question,
            "ans": "",
            "type": "error",
            "msg": "处理失败，请换个问法吧"
        }
    return JSONResponse(content=processed_data)

@app.post("/api/exe-sql/")
async def exe_sql(request: Request, user_input: AgentInput):
    ans = exe_tp_sql(user_input.question)
    print(ans)
    if ans:
        processed_data = {
            "question": user_input.question,
            "ans": ans,
            "type": "success",
            "msg": "处理成功"
        }
    else:
        processed_data = {
            "question": user_input.question,
            "ans": "",
            "type": "error",
            "msg": "处理失败，请换个问法吧"
        }
    return JSONResponse(content=processed_data)


@app.post("/api/get-graph/")
async def get_graph(request: Request, user_input: AgentInputDict):
    df = pd.DataFrame.from_dict(user_input.data)
    ans = draw_graph(user_input.question, df)
    if ans:
        processed_data = {
            "question": user_input.question,
            "ans": ans,
            "type": "success",
            "msg": "处理成功"
        }
    else:
        processed_data = {
            "question": user_input.question,
            "ans": "",
            "type": "error",
            "msg": "处理失败，请换个问法吧"
        }
    return JSONResponse(content=processed_data)


@app.post("/api/get-echart/")
async def get_echart(request: Request, user_input: AgentInputDict):
    df = pd.DataFrame.from_dict(user_input.data)
    ans = draw_echart_file(user_input.question, df)
    if ans:
        processed_data = {
            "question": user_input.question,
            "ans": ans,
            "type": "success",
            "msg": "处理成功"
        }
    else:
        processed_data = {
            "question": user_input.question,
            "ans": "",
            "type": "error",
            "msg": "处理失败，请换个问法吧"
        }
    return JSONResponse(content=processed_data)


@app.post("/api/get-pygwalker/")
async def get_pygwalker(request: Request, user_input: AgentInputDict):
    df = pd.DataFrame.from_dict(user_input.data)
    ans = pd_to_walker(df)
    if ans:
        processed_data = {
            "question": user_input.question,
            "ans": ans,
            "type": "success",
            "msg": "处理成功"
        }
    else:
        processed_data = {
            "question": user_input.question,
            "ans": "",
            "type": "error",
            "msg": "处理失败，请换个问法吧"
        }
    return JSONResponse(content=processed_data)


if __name__ == "__main__":
    uvicorn.run(app, host=config_data['server_host'], port=config_data['server_port'])
