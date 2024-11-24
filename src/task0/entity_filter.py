import os,sys
import traceback
sys.path.append(os.getcwd())
import re
import json
from src.settings import Settings
from src.util import (
    build_logger,
)

logger = build_logger()

hadoop_streaming_path = Settings.basic_settings.hadoop_streaming_path
ckg4j_root = Settings.CKG4J_ROOT

def extract_json(content):
    """
    从字符串中提取 JSON 部分。
    :param content: 包含可能的 JSON 的字符串
    :return: 解析后的 JSON 对象或 {}
    """
    try:
        # 使用正则表达式提取 JSON 对象
        json_pattern = r"\{.*\}"  # 匹配以 { 开始并以 } 结束的内容
        match = re.search(json_pattern, content, re.DOTALL)
        if match:
            json_text = match.group()
            return json.loads(json_text)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
    
    # 如果提取失败，返回空字典
    logger.warning("No valid JSON found in the response content.")
    return {}


def read_statistics(file_or_dir):
    """
    读取 reducer 输出结果，格式为：名称\t频次。
    """
    data = []
    if os.path.isdir(file_or_dir):
        files = [os.path.join(file_or_dir, f) for f in os.listdir(file_or_dir)]
    else:
        files = [file_or_dir]

    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                name, count = line.split("\t")
                data.append((name, int(count)))
    return data


def filter_by_frequency(data, top_n=None, min_frequency=None):
    """
    根据频次筛选人名：选出排名前 top_n 或出现频次大于等于 min_frequency 的人名。
    """
    if top_n:
        filtered_data = sorted(data, key=lambda x: x[1], reverse=True)[:top_n]
    elif min_frequency:
        filtered_data = [item for item in data if item[1] >= min_frequency]
    else:
        filtered_data = data

    return {name: name for (name, _) in filtered_data}


def filter_with_llm(data):
    """
    使用 LLM 筛选并格式化疑似人名。
    """
    from openai import OpenAI
    client = OpenAI(
        api_key=Settings.basic_settings.openai_api_key,
        base_url=Settings.basic_settings.openai_api_base
    )
    system_prompt = """
    你只返回json串。给定输入列表[("词组1","出现频次1"),...]，你的任务是结合常识和自身知识，选择出其中疑似人名的词组，返回一个人名json字典。
    字典的格式是：
    ```json
    {{
        "词组1": "人名1",
        "词组2": "人名1",
        "词组3": "人名2",
        "词组4": "人名2",
        ...
    }}
    ```
    你需要特别注意以下几点：
    1. 直接过滤掉不可能是人名的低频词组，**只需要保留显然是人名的词组**。
    2. 每个人名都有可能有其它的昵称或者外号代表，你需要结合知识合理判定并适当补充其外号或昵称。
    3. 你可以适当删除一些不重要（出现频次较少）的人名。
    4. 你输出的json串的语言应该与输入数据保持一致。
    """
    user_prompt = f"输入列表：\n{str(data)}\n请返回json人名字典：\n"
    try:
        response = client.chat.completions.create(
            model=Settings.basic_settings.openai_api_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7,
            max_tokens=2048,
        )
        print(response)
        content =  response.choices[0].message.content
        return extract_json(content)
    except Exception as e:
        print(traceback.format_exc())
        logger.error(f"Error using LLM: {e}")
        return {}


def filter_entities(data, top_n=None, min_frequency=None, use_llm=False):
    """
    通用实体筛选函数。
    :param data: 输入数据，格式为 [(名称1, 频次1), (名称2, 频次2), ...]
    :param top_n: 选出频次排名前 top_n 的人名
    :param min_frequency: 选出频次大于等于 min_frequency 的人名
    :param use_llm: 是否使用 LLM 筛选
    :return: 筛选后的字典，格式为 {名称: 人物编号}
    """
    logger.info(f"Filtering entity data: {data}")
    if use_llm:
        logger.info("Using LLM for filtering...")
        return filter_with_llm(data)
    else:
        logger.info(f"Filtering data by frequency: top_n={top_n}, min_frequency={min_frequency}")
        return filter_by_frequency(data, top_n=top_n, min_frequency=min_frequency)

if __name__ == "__main__":
    file_path = "data/tb/output/task0"
    data = read_statistics(file_path)[0:100]
    print(str(data))
    # d = filter_entities(
    #     data,
    #     use_llm=True
    # )
    # print(json.dumps(d,indent=4,ensure_ascii=False))

