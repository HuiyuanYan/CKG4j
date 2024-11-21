import sys
for line in sys.stdin:
    line = line.strip()
    names = line.split(",")
    for name1 in names:
        for name2 in names:
            if name1 != name2:
                if name1 and name2:
                    print(f"{name1},{name2},1")
                pass