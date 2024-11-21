import sys

freq_dict = {}
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    if line.startswith("<") and line.endswith(">"):
        name1, name2, count = line[1:-1].replace('\t',',').split(",")
        if name1 not in freq_dict:
            freq_dict[name1] = {}
        if name2 not in freq_dict[name1]:
            freq_dict[name1][name2] = 0
        freq_dict[name1][name2] += int(count)

for name1 in freq_dict:
    total_count = 0
    for name2 in freq_dict[name1]:
        total_count += freq_dict[name1][name2]
    
    freq_list = sorted(freq_dict[name1].items(), key=lambda x: x[1], reverse=True)
    # 计算每一频率，保留五位小数
    freq_list = [(name2, round(count / total_count, 5)) for name2, count in freq_list]
    freq_str = name1 + "\t" + "[" + "|".join([f"{name2}@{freq}" for name2, freq in freq_list]) + "]"
    print(freq_str)