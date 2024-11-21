# Task4阶段说明

## 1. 阶段目标
本阶段（`task4`）的目标是使用`pagerank`算法计算人物重要性。输入文件夹中的文本内容是`task3`的输出，其格式示例是`<主人物>   [人物1@出现频次1|人物2@出现频次2...]`。其输出是人物的重要性分数及排名：格式示例（重要性降序排序）是：
```
人物1|重要性分数1
人物2|重要性分数2

...

```

由于`hadoop streaming`不支持/弱支持mapper之间的共享计数器，所以使用python程序无法进行分布式的计算`pagerank`，本仓库为了保持整体`pipeline`的一致性，就先用python版本实现该算法。在作者以往的仓库有用`java`实现的`map-reduce`版本，分布式实现可以参考。

## 2.运行示例
```bash
python src/task4/run.py --input_dir data/test/output/task3 --output_dir data/test/output/task4
```
