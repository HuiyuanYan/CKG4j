import sys

entity_dict = {}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    if line not in entity_dict:
        entity_dict[line] = 0
    entity_dict[line] += 1

for entity in sorted(entity_dict, key=lambda x:entity_dict[x], reverse=True):
    print(f"{entity}\t{entity_dict[entity]}")