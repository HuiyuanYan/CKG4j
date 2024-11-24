import jieba.posseg as pseg
from .base import EntityExtractor

class JiebaEntityExtractor(EntityExtractor):
    """
    使用 jieba 实现的实体抽取
    """
    def __init__(self, entity_type="nr"):
        super().__init__(entity_type)

    def extract_entities(self, text):
        """
        使用 jieba 提取实体
        """
        entities = []
        words = pseg.cut(text)
        for word, flag in words:
            if flag == self.entity_type:
                entities.append(word)
        return entities