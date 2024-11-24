import os,sys
sys.path.append(os.getcwd())
from src.task0.extractor import get_entity_extractor

if len(sys.argv) < 3:
    print("Usage: python mapper.py <extractor_type> <entity_type> <spacy_model_name>(optional)", file=sys.stderr)
    sys.exit(1)

extractor_type = sys.argv[1]  # 抽取器类型 (如 spacy, nltk, jieba)
entity_type = sys.argv[2]    # 实体类型 (如 PERSON, nr 等)




try:
    if extractor_type == "spacy":
        if len(sys.argv) != 4:
            print("Usage: python mapper.py spacy <entity_type> <spacy_model_name>", file=sys.stderr)
            sys.exit(1)
        spacy_model_name = sys.argv[3]
        extractor = get_entity_extractor(extractor_type, entity_type=entity_type, model_name=spacy_model_name)
    else:
        extractor = get_entity_extractor(extractor_type, entity_type=entity_type)
except ValueError as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    entities = extractor.extract_entities(line)
    for entity in entities:
        print(entity)