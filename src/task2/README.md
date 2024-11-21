# Task2阶段说明

## 1. 阶段目标
该阶段（`task2`）的目标是统计人物同现统计，输入为`task1`的输出文件夹，输入文本的每一行的格式如下：

```
name1,name2,name3...
```

表示原文本中，一段内容中出现的人名统计（name可能重复出现）。

而`task2`的任务是，根据人名同现统计，计算出所有可能的同现人物对，并统计其共现次数，输出为如下格式：
```
<name1,name2,count>
```

实现原理较简单可参见相关map和reduce代码。

## 2. 运行示例
```bash
python src/task2/run.py --input_dir data/test/output/task1 --output_dir data/test/output/task2
```