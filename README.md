# data-copilot


✨ **基于代码生成和函数调用(function call)的大语言模型(LLM)智能体**

通过自然语言提问，使用大语言模型智能解析数据库结构，对数据进行智能多表结构化查询和统计计算，根据查询结果智能绘制多种图表。
支持自定义函数(function call)和Agent调用，多智能体协同。
基于代码生成的思维链(COT)。
实现智能体对用户的反问，解决用户提问模糊、不完整的情况。

🚩[English Readme](./README.en.md)

- [基于大语言模型 (LLM) 的<u>**可解释型**</u>自然语言数据库查询系统 (RAG) https://github.com/bytesc/data-copilot-steps](https://github.com/bytesc/data-copilot-steps)
- [基于大语言模型 (LLM)和并发预测模型的自然语言数据库查询系统 (RAG) (https://github.com/bytesc/data-copilot-v2](https://github.com/bytesc/data-copilot-v2)

🔔 如有项目相关问题，欢迎在本项目提出`issue`，我一般会在 24 小时内回复。

## 功能简介

- 1, 基于代码生成的大语言模型智能体(AI Agent)。
- 2, 实现智能体对用户的反问，解决用户提问模糊、不完整的情况。
- 3, 智能体支持灵活的自定义函数调用(function call)和思维链(COT)
- 4, 实现多智能体的合作调用
- 5, 智能体实现智能绘制多种统计图表
- 6, 能够处理大语言模型表现不稳定等异常情况
- 7, 支持 `openai` 格式(如 `glm` ,`deepseek`, `qwen`)的 api 接口




## 创新点
- 基于代码生成的智能体(Agent)支持灵活的自定义函数调用(function call)和思维链(COT)
- 引入函数依赖图(Function Graph)的概念，实现自定义函数调用
- 引入智能体函数(Agent as Function)的概念，实现多智能体(Agent)的合作调用
- 实现智能体(Agent)对用户的反问，解决用户提问模糊、不完整的情况
- 包含输出断言和异常处理，能够处理大语言模型表现不稳定等异常情况

## 如何使用

### 安装依赖

python 版本 3.10

```bash
pip install -r requirement.txt
```

### 配置文件

`config.yaml`


### 大语言模型配置


新建文件：`agent\utils\llm_access\api_key_openai.txt` 在其中填写`api-key`

`api-key`获取链接：
- 阿里云:[https://bailian.console.aliyun.com/](https://bailian.console.aliyun.com/)
- deepseek:[https://api-docs.deepseek.com/](https://api-docs.deepseek.com/)
- glm:[https://open.bigmodel.cn/](https://open.bigmodel.cn/)



### 运行

#### 服务端

```bash
# 服务端
python ./main.py
```



### 自定义 function call

在 `agent/tools/custom_tools_def.py` 中定义自定义函数

需要写规范、详细的注释，前三行是函数基本功能信息，之后是详细信息


然后在 `agent/tools/get_function_info.py` 中注册函数

import 函数之后，在 `FUNCTION_DICT` 中添加函数，在 `FUNCTION_IMPORT ` 中添加 import 语句

例如：

```python
from .custom_tools_def import get_minimap  # 导入


FUNCTION_DICT = {
    "query_database": query_database,
    "draw_graph": draw_graph,
}

FUNCTION_IMPORT = {
    query_database: "from agent.tools.tools_def import query_database",
    draw_graph: "from agent.tools.tools_def import draw_graph",
    }
```

`ASSIST_FUNCTION_DICT` 定义了函数的依赖关系图。

例如：

```python
ASSIST_FUNCTION_DICT = {
    # predict_grade_for_stu: [from_username_to_uid, from_lesson_name_to_lesson_num],
}
```

```txt
data agent问题

查询单一用户问题
请问88523679256的用户
是哪个城市的呢？他的名字叫什么？
请问他的状态是否正常呢？
请问他的账户何时过期？
请问他的email是多少
请问她具备哪些特征
请问他是否使用系统？
请问匿名id是X的用户，
最近一次访问的页面名字叫什么
最近一次登录的时间是什么？
第一次登录的时间是什么？
他的最后一次访问的IP是多少
他最后从哪个城市访问的？
他有过购买记录么？

查询某个表问题
系统中有匿名id 的是多少
系统中有会员id的是多少？
系统中会员的城市分布是怎样的，请画一个饼图。
系统中的用有多少已经试用过，多少没有试用过，帮我画一个柱状图。
系统中的用户被冻结的有几个？帮我画一个饼图看下冻结比例。
系统中的用户最近三天天的浏览次数一共是多少，帮我画一个三天的线图以日为标准，并预测下后续的趋势。


```

# 开源许可证

此翻译版本仅供参考，以 LICENSE 文件中的英文版本为准

MIT 开源许可证：

版权所有 (c) 2025 bytesc

特此授权，免费向任何获得本软件及相关文档文件（以下简称“软件”）副本的人提供使用、复制、修改、合并、出版、发行、再许可和/或销售软件的权利，但须遵守以下条件：

上述版权声明和本许可声明应包含在所有副本或实质性部分中。

本软件按“原样”提供，不作任何明示或暗示的保证，包括但不限于适销性、特定用途适用性和非侵权性。在任何情况下，作者或版权持有人均不对因使用本软件而产生的任何索赔、损害或其他责任负责，无论是在合同、侵权或其他方面。
