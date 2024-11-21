import sys
frep_dict = {}
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    line = line.replace("\t",",")
    name1,name2,count = line.split(",")
    if name1 not in frep_dict:
        frep_dict[name1] = {}
    if name2 not in frep_dict[name1]:
        frep_dict[name1][name2] = 0
    frep_dict[name1][name2] += int(count)

for name1 in frep_dict:
    for name2 in frep_dict[name1]:
        print(f"<{name1},{name2},{frep_dict[name1][name2]}>")