import sys
for line in sys.stdin:
    line = line.strip()
    if line:
        sys.stderr.write("task3 reporter:counter:<group>,<counter>,<amount>")
        print(line)