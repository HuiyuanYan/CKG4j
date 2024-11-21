import sys
import re
from collections import defaultdict
import json  # 导入json模块

# 读取JSON文件中的人名字典
def read_name_dict(cache_files):
    name_dict = {}
    for cache_file in cache_files:
        with open(cache_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            name_dict.update(data)
    return name_dict

# 从环境变量中获取缓存文件路径
cache_files = sys.argv[1:]
# 读取人名字典
name_dict = read_name_dict(cache_files)
name_pattern = re.compile('|'.join(re.escape(name) for name in name_dict.keys()))

# 初始化一个字典来存储每个名字的出现次数
name_count = defaultdict(int)

for line in sys.stdin:
    names = ""
    line = line.strip()
    # 移除标点符号
    line = re.sub(r"[^\w\s'-]", "", line)
    # 使用正则表达式匹配名字
    matches = name_pattern.findall(line)
    for match in matches:
        # 如果匹配到的名字在字典中，则增加对应的计数
        if match in name_dict:
            names += f",{name_dict[match]}"
    if names.startswith(","):
        names = names[1:]
    print(names)