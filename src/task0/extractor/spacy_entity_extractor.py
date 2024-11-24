import spacy
from .base import EntityExtractor

class SpacyEntityExtractor(EntityExtractor):
    """
    使用 spaCy 实现的实体抽取
    """
    def __init__(self, model_name="zh_core_web_sm", entity_type="PERSON"):
        super().__init__(entity_type)
        self.nlp = spacy.load(model_name)

    def extract_entities(self, text):
        """
        使用 spaCy 提取实体
        """
        entities = []
        doc = self.nlp(text)
        for ent in doc.ents:
            if ent.label_ == self.entity_type:
                entities.append(ent.text)
        return entities