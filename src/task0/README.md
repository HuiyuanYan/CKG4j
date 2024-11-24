# Task0阶段说明

本阶段的任务是给定输入文件夹，利用 `NLP`方法，提取其中指定的实体（人名），并进行过滤，将其输出为一个人名字典，以供后续阶段使用。

若用户不指定该阶段，后续的任务需自己提供人名列表。

注意：**由于实体提取器及LLM筛选的不稳定性，我们仍推荐您自己手动指定人名字典！**

## 1. 实现原理

对于给定的输入，`mapper`使用实体抽取器（在 `extractor`模块中定义，目前支持使用 `spacy`、`jieba`、`nltk`）先抽取出可能的人名词组，然后对这些可能的词组进行过滤，过滤方法有三种（实现在 `entity_filter.py`）：

+ 基于 `词频`过滤：指定前 `m`频繁出现的词组，及最小出现次数 `n`，最终的人名字典将返回前 `m`个词组中且至少出现 `n`次的字典。
+ 基于 `LLM`过滤：对出现频率前100高的人名词组，使用大语言模型（默认使用 `LLM`）进行筛选，最终的人名字典将返回通过筛选后的人名词组。

## 2. 运行示例

```bash
python src/task0/run.py --input_dir data/test/test_files --output_dir data/test/output/task0 --extractor_method jieba --entity_type nr --json_output_path data/test/output/task0/character.json --use_llm
```
